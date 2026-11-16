"""Univariate changepoint segmentation over a fixed ArrayBench snapshot.

This module is an independent read-only analysis built on the normalized rows
returned by :meth:`arraybench.Dataset.window`.  It never reinterprets units,
fills missing observations, resamples or rewrites any chunk.

Pipeline
--------
1. ``window`` rows are grouped by their *full* coordinate tuple and sorted by
   UTC time.  Rows may arrive in any traversal order (bucket/chunk order); the
   router below makes the result order-independent.
2. Missing-block planning: for the selected variable, an actual ``null``
   observation or an adjacent time gap strictly greater than the configured
   limit opens a *forced* block boundary.  Invalid values of *other* variables
   do not cut this variable's blocks, missing data is never treated as zero,
   and segments never cross a forced boundary.
3. Interval costs are squared errors relative to the segment mean, computed
   with a shifted Welford recurrence -- never the ``sum(x**2) - sum(x)**2/n``
   form, which cancels on large baselines.  Costs are never clamped.
4. A global dynamic program minimizes ``SSE + penalty * autonomous
   changepoints`` over *all* segmentations satisfying the per-segment minimum
   point count and minimum time span.  Forced boundaries carry no penalty.
   Ties are resolved by fewer segments, then the lexicographically smallest
   tuple of cut times.  No local segmentation is finalized before the whole
   block has been read: the DP back trace only runs after the block closes.

Memory strategy: external per-group staging versus streaming cost cache
------------------------------------------------------------------------
Two ways preserve the alternative cuts required by the global solver:

* **External per-group staging** keeps every raw (time, value) record.  Groups
  are held in resident buffers up to ``max_resident_records``; overflow is
  spilled to per-group JSONL staging files bounded by ``max_staging_bytes`` and
  re-read at finalization.  All alternatives survive because the full block is
  reconstructed verbatim, independently of input traversal order.
* A **streaming interval cost cache** (see :class:`IntervalCostCache`) extends
  every open interval's Welford state as observations arrive, retaining the
  same alternative cost information in ``n(n+1)/2`` cached states while raw
  values can be discarded.  The block DP still cannot be settled until the
  block closes, and (time, coordinates) identities must still be retained for
  the output, so this module combines both: staging for cross-chunk grouping
  under explicit byte/record caps, and the incremental cost cache inside each
  block.  A block longer than ``max_group_observations`` is rejected outright;
  there is no silent fallback to a greedy/threshold segmentation.
"""
import json
import math
import os
import tempfile
from .units import epoch, iso

# Explicit numerical tolerances.  Adding an exactly representable constant
# (e.g. 10**12) to every observation must leave cuts and squared errors within
# these tolerances and shift every segment mean by that constant.
COST_REL_TOL = 1e-12
COST_ABS_TOL = 1e-9
MEAN_REL_TOL = 1e-12

DEFAULT_LIMITS = {
    "max_resident_records": 10000,
    "max_staging_bytes": 8 * 1024 * 1024,
    # A block of n observations caches n(n+1)/2 interval states plus O(n) DP.
    "max_group_observations": 1000,
}


class IntervalCostCache:
    """Incremental squared-error/mean cache for every interval of a block.

    All values are shifted by the block's first observation.  Translation does
    not change squared errors and keeps the Welford recurrence working on small
    differences even when every raw value sits on a 10**12 baseline.  The
    cache grows by one interval state per open start on every ``append`` (the
    streaming cost-cache option described in the module docstring); a block is
    only solved once it has been fully read.
    """

    def __init__(self):
        self.shift = None
        # Parallel triangular rows: cell (i, j) lives at self._m2[i][j].
        self._m2 = []
        self._means = []
        # Open Welford runs per start index i: (count, shifted_mean, m2).
        # The whole recurrence operates on shifted values x - shift, which are
        # small even for 10**12 baselines; means are translated back on read.
        self._open = []

    def append(self, value):
        if self.shift is None:
            self.shift = float(value)
        y = float(value) - self.shift
        j = len(self._open)
        m2_row, mean_row = [], []
        for i in range(j):
            count, mean, m2 = self._open[i]
            count += 1
            delta = y - mean
            mean += delta / count
            m2 += delta * (y - mean)
            self._open[i] = (count, mean, m2)
            m2_row.append(m2)
            mean_row.append(mean)
        # Interval consisting of the new observation alone.
        self._open.append((1, y, 0.0))
        m2_row.append(0.0)
        mean_row.append(y)
        self._m2.append(m2_row)
        self._means.append(mean_row)

    def sse(self, first, last):
        """Squared error of observations [first, last] around their mean."""
        return self._m2[last][first]

    def mean(self, first, last):
        """Arithmetic mean on the original (unshifted) scale."""
        return self.shift + self._means[last][first]

    @property
    def size(self):
        return len(self._open)


def plan_blocks(records, max_gap_micros):
    """Split sorted (time, value) records into estimable candidate blocks.

    A ``null`` value for the selected variable forces a boundary and is not
    part of any block; a gap strictly greater than ``max_gap_micros`` forces a
    boundary between two valid observations.  Everything else stays in the
    same block -- invalid *other* variables are invisible here.
    """
    blocks = []
    start = None
    start_reason = None
    previous_time = None
    previous_missing = False
    for index, (time, value) in enumerate(records):
        if value is None:
            if start is not None:
                blocks.append({"start": start, "end": index - 1,
                               "start_reason": start_reason,
                               "end_reason": "forced:missing",
                               "boundary_time": time})
                start = None
            previous_time, previous_missing = time, True
            continue
        if start is None:
            start_reason = "edge" if index == 0 else "forced:missing"
            start = index
        elif time - previous_time > max_gap_micros:
            blocks.append({"start": start, "end": index - 1,
                           "start_reason": start_reason,
                           "end_reason": "forced:gap",
                           "boundary_time": time})
            start, start_reason = index, "forced:gap"
        previous_time, previous_missing = time, False
    if start is not None:
        blocks.append({"start": start, "end": len(records) - 1,
                       "start_reason": start_reason, "end_reason": "edge",
                       "boundary_time": None})
    return blocks


def _identity(time, coordinates):
    return {"time": iso(time), "coordinates": coordinates}


def solve_block(records, block, *, penalty, min_points, min_span_micros,
                max_group_observations, coordinates):
    """Global DP for one candidate block; never finalizes cuts incrementally."""
    first, last = block["start"], block["end"]
    times = [records[index][0] for index in range(first, last + 1)]
    n = len(times)
    if n > max_group_observations:
        raise ValueError(
            f"coordinate group {json.dumps(coordinates, sort_keys=True)} has a block "
            f"of {n} observations exceeding the per-group solver limit "
            f"{max_group_observations} (cached interval states n(n+1)/2)")
    identity = [_identity(records[first + index][0], coordinates) for index in range(n)]
    if n < min_points:
        return {"status": "not_estimable", "reason": "min_points", "count": n,
                "span_seconds": (times[-1] - times[0]) / 1e6,
                "start_reason": block["start_reason"], "end_reason": block["end_reason"],
                "boundary_time": iso(block["boundary_time"]) if block["boundary_time"] is not None else None,
                "first_observation": identity[0], "last_observation": identity[-1]}
    if times[-1] - times[0] < min_span_micros:
        return {"status": "not_estimable", "reason": "min_span", "count": n,
                "span_seconds": (times[-1] - times[0]) / 1e6,
                "start_reason": block["start_reason"], "end_reason": block["end_reason"],
                "boundary_time": iso(block["boundary_time"]) if block["boundary_time"] is not None else None,
                "first_observation": identity[0], "last_observation": identity[-1]}

    cache = IntervalCostCache()
    for index in range(first, last + 1):
        cache.append(records[index][1])

    # Every state is (objective_so_far, segment_count, cut_times), where
    # objective_so_far already includes the penalty for every autonomous cut
    # charged so far.  Keeping all non-dominated prefixes per endpoint is
    # essential: two prefixes with the same objective but different segment
    # counts diverge once another segment (and penalty) is appended, so a
    # single "best prefix" would destroy the global optimum.  A prefix
    # dominates another when it is no worse on objective and segment count
    # and strictly better on at least one; lexicographic cut order only breaks
    # genuinely identical-objective/count ties.
    dp = {-1: [(0.0, 0, ())]}
    for j in range(n):
        candidates = []
        for i in range(j + 1):
            if j - i + 1 < min_points:
                continue
            if times[j] - times[i] < min_span_micros:
                continue
            for previous in dp.get(i - 1, ()):
                # The first segment starts at the forced block edge: no penalty.
                charged_penalty = penalty if i > 0 else 0.0
                cut_times = previous[2] + ((times[i],) if i > 0 else ())
                objective = previous[0] + cache.sse(i, j) + charged_penalty
                candidates.append((objective, previous[1] + 1, cut_times))
        # Linear-after-sort Pareto pruning.  Sorting by (objective, segment
        # count, cut times) means a state is dominated exactly when an earlier
        # state already used no more segments; the running minimum tracks that.
        candidates.sort()
        pruned = []
        minimum_segments = None
        for candidate in candidates:
            if minimum_segments is not None and candidate[1] >= minimum_segments:
                continue
            pruned.append(candidate)
            minimum_segments = candidate[1]
        if pruned:
            dp[j] = pruned
    if n - 1 not in dp:
        return {"status": "not_estimable", "reason": "constraints", "count": n,
                "span_seconds": (times[-1] - times[0]) / 1e6,
                "start_reason": block["start_reason"], "end_reason": block["end_reason"],
                "boundary_time": iso(block["boundary_time"]) if block["boundary_time"] is not None else None,
                "first_observation": identity[0], "last_observation": identity[-1]}

    # Final arbitration over every complete, non-dominated segmentation:
    # smallest objective, then fewest segments, then earliest cut sequence.
    objective, segment_count, cut_times = min(
        dp[n - 1], key=lambda state: (state[0], state[1], state[2]))
    cut_indices = [times.index(cut) for cut in cut_times]
    bounds = [0, *cut_indices, n]
    segments = []
    for segment_index in range(len(bounds) - 1):
        a, b = bounds[segment_index], bounds[segment_index + 1] - 1
        segments.append({
            "first_observation": identity[a],
            "last_observation": identity[b],
            "count": b - a + 1,
            "mean": cache.mean(a, b),
            "error": cache.sse(a, b),
            "start_reason": block["start_reason"] if a == 0 else "autonomous",
            "end_reason": block["end_reason"] if b == n - 1 else "autonomous",
        })
    autonomous = segment_count - 1
    return {"status": "estimated", "count": n,
            "span_seconds": (times[-1] - times[0]) / 1e6,
            "start_reason": block["start_reason"], "end_reason": block["end_reason"],
            "boundary_time": iso(block["boundary_time"]) if block["boundary_time"] is not None else None,
            "segments": segments, "objective": objective,
            "squared_error": objective - autonomous * penalty,
            "autonomous_changepoints": autonomous}


def _validate_number(name, value, *, allow_zero=True):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number")
    if value < 0 or (not allow_zero and value == 0):
        raise ValueError(f"{name} must be non-negative" if allow_zero else f"{name} must be positive")


def segment_rows(rows, *, schema, variable, snapshot_id, start, end,
                 penalty, min_points, min_span_seconds, max_gap_seconds,
                 limits=None):
    """Segment normalized ``window`` rows for one variable.

    ``rows`` is any iterable of the read-only dict shape produced by
    ``Dataset.window``; it is consumed once and never mutated.
    """
    if variable not in schema["variables"]:
        raise ValueError(f"unknown variable: {variable}")
    _validate_number("penalty", penalty)
    _validate_number("min_span_seconds", min_span_seconds)
    _validate_number("max_gap_seconds", max_gap_seconds)
    penalty = float(penalty)
    if type(min_points) is not int or min_points < 1:
        raise ValueError("min_points must be a positive integer")
    effective_limits = dict(DEFAULT_LIMITS)
    if limits is not None:
        if set(limits) - set(DEFAULT_LIMITS):
            raise ValueError("unknown segmentation memory limit")
        effective_limits.update(limits)
    for name, value in effective_limits.items():
        if type(value) is not int or value < 1:
            raise ValueError(f"{name} must be a positive integer")
    max_resident = effective_limits["max_resident_records"]
    max_staging_bytes = effective_limits["max_staging_bytes"]
    max_group_observations = effective_limits["max_group_observations"]
    min_span_micros = int(round(min_span_seconds * 1_000_000))
    max_gap_micros = int(round(max_gap_seconds * 1_000_000))

    buffers, staged_paths, staged_bytes = {}, {}, 0
    resident = 0
    resident_peak = 0
    staging_dir = tempfile.mkdtemp(prefix="arraybench-segment-")
    try:
        for row in rows:
            key = json.dumps(row["coordinates"], sort_keys=True, separators=(",", ":"))
            if key not in buffers:
                # Spilled groups keep an empty resident slot, so the number of
                # tracked groups is exactly len(buffers); each owns >=1 record.
                if len(buffers) >= max_resident:
                    raise ValueError("segmentation group count exceeds resident record limit")
                buffers[key] = []
            record = [epoch(row["time"]), row["values"][variable]]
            if resident >= max_resident:
                # Spill the largest resident buffer; ties take the smallest key.
                victim = max((key for key in buffers if buffers[key]),
                             key=lambda candidate: (len(buffers[candidate]), candidate))
                path = staged_paths.get(victim)
                if path is None:
                    path = tempfile.NamedTemporaryFile(prefix="group-", suffix=".jsonl",
                                                       dir=staging_dir, delete=False).name
                    staged_paths[victim] = path
                payload = "".join(
                    json.dumps(item, separators=(",", ":")) + "\n" for item in buffers[victim])
                if staged_bytes + len(payload.encode()) > max_staging_bytes:
                    raise ValueError("segmentation staging byte limit exceeded")
                with open(path, "a", encoding="utf-8") as stream:
                    stream.write(payload)
                staged_bytes += len(payload.encode())
                resident -= len(buffers[victim])
                buffers[victim] = []
            buffers[key].append(record)
            resident += 1
            resident_peak = max(resident_peak, resident)

        groups = []
        for key in sorted(set(buffers) | set(staged_paths)):
            records = []
            path = staged_paths.get(key)
            if path is not None:
                with open(path, encoding="utf-8") as stream:
                    records.extend(json.loads(line) for line in stream if line.strip())
            records.extend(buffers[key])
            records.sort(key=lambda record: record[0])
            coordinates = json.loads(key)
            block_results = []
            for block in plan_blocks(records, max_gap_micros):
                block_results.append(solve_block(
                    records, block, penalty=penalty, min_points=min_points,
                    min_span_micros=min_span_micros,
                    max_group_observations=max_group_observations,
                    coordinates=coordinates))
            groups.append({"coordinates": coordinates, "blocks": block_results})
    finally:
        for path in staged_paths.values():
            try:
                os.unlink(path)
            except OSError:
                pass
        try:
            os.rmdir(staging_dir)
        except OSError:
            pass

    return {
        "snapshot": snapshot_id,
        "variable": variable,
        "window": {"start": start, "end": end},
        "parameters": {"penalty": penalty, "min_points": min_points,
                       "min_span_seconds": min_span_seconds,
                       "max_gap_seconds": max_gap_seconds},
        "groups": groups,
        "execution": {"resident_peak": resident_peak,
                      "staged_bytes": staged_bytes,
                      "spilled_groups": len(staged_paths),
                      "group_count": len(groups),
                      "limits": dict(effective_limits)},
    }
