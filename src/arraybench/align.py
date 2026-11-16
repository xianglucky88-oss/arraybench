"""Exact observation alignment across two pinned dataset snapshots.

Two devices sample the same spatial coordinates at slightly different times.
This module aligns their *normalized* rows one-to-one inside each complete
coordinate group, strictly on top of the existing snapshot read interface
(``Dataset.snapshot`` for schemas, ``Dataset.window`` for rows). It never
appends, never interpolates, never reinterprets units, never rewrites
coordinate identity, never densifies into a time x coordinate grid, and
never touches HEAD: both sides are pinned to explicit snapshot versions in
the plan, so later commits cannot leak into a run and no cross-dataset
simultaneous transaction is implied.

Contract between the three layers:

1. Plan validation (``validate_plan``) fixes both dataset roots and snapshot
   versions, the half-open window ``[start, end)``, a bijective left->right
   dimension mapping, a non-negative integer microsecond tolerance, the
   scheduling strategy and three resource limits. Both schemas are checked
   up front: every dimension must be mapped exactly once and mapped
   dimensions must share a type; unmapped dimensions are rejected, never
   silently merged. Unknown plan fields are rejected as well.

2. Grouped scheduling (``align``) streams rows from the pinned windows,
   derives each row's group key by renaming right-side coordinates through
   the mapping (values are copied verbatim), and feeds one complete
   coordinate group at a time to the solver. Strategy ``memory``
   materializes all rows (resident O(N), no temporaries); strategy
   ``staging`` spills per-group JSONL partitions to temporary files
   (resident O(groups + group rows), temporaries O(N)). An external sort
   was considered and rejected: it adds O(N log N) comparisons and O(N)
   temporaries while the exact solver already requires bounded per-group
   state, so per-group staging strictly dominates it for sparse data. Both
   strategies call the same solver and produce identical alignments.

3. Exact solver (``solve_group``) aligns two strictly increasing time lists
   with a one-to-one, non-crossing matching: first maximize the number of
   pairs, then minimize the total absolute time difference, then take the
   lexicographically smallest pair sequence (left/right times, then stable
   row identities; times are unique within a group, so identities never
   reorder a group). Any crossed feasible matching can be uncrossed without
   breaking tolerance or increasing cost, so the LCS-style dynamic program
   is exact, never greedy. Rows at exactly the tolerance distance match;
   rows at ``end`` never enter the window; null measurements are ordinary
   pairable observations.

Resource contract: the plan declares ``max_resident_rows``,
``max_temp_bytes`` and ``max_group_cells``. Exceeding any of them raises
``AlignmentLimitError`` and the run fails; it never degrades to a greedy
approximation that still returns success. Temporary files live outside the
dataset roots and are removed after the run.

Row identity: every pair and unpaired row carries ``(dataset, snapshot,
coordinates, time)``, which can be looked up again through
``Dataset.window`` with a coordinate filter; physical chunk layout is never
part of the identity. Unpaired rows carry a reason:
``peer_group_missing`` (the other side has no such coordinate group),
``no_candidate_within_tolerance`` (no peer row inside the tolerance), or
``candidate_claimed_by_better_match`` (feasible peers exist but the optimal
matching paired them otherwise).
"""
import bisect
import hashlib
import json
import os
import shutil
import tempfile
from dataclasses import dataclass

from .dataset import Dataset
from .units import epoch, iso

__all__ = ["AlignmentLimitError", "Plan", "align", "solve_group", "validate_plan"]


class AlignmentLimitError(Exception):
    """A declared plan limit was exceeded; the run failed explicitly."""


REASON_PEER_GROUP_MISSING = "peer_group_missing"
REASON_NO_CANDIDATE = "no_candidate_within_tolerance"
REASON_CANDIDATE_CLAIMED = "candidate_claimed_by_better_match"

_PLAN_FIELDS = {"left", "right", "window", "dimensions", "tolerance_micros", "strategy", "limits", "temp_dir"}
_LIMIT_FIELDS = {"max_resident_rows", "max_temp_bytes", "max_group_cells"}
_STRATEGIES = ("memory", "staging")


@dataclass(frozen=True)
class Plan:
    """A fully validated, immutable alignment plan with pinned snapshots."""
    left_root: str
    left_version: str
    right_root: str
    right_version: str
    start: str
    end: str
    start_micros: int
    end_micros: int
    dimensions: dict
    tolerance_micros: int
    strategy: str
    max_resident_rows: int
    max_temp_bytes: int
    max_group_cells: int
    temp_dir: str | None


def _positive_int(value, name):
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def validate_plan(plan):
    """Validate an alignment plan and pin both snapshots.

    Reads only the two pinned snapshot summaries; HEAD is never consulted,
    so commits made after validation cannot leak into the run.
    """
    if not isinstance(plan, dict):
        raise ValueError("alignment plan must be a JSON object")
    unknown = set(plan) - _PLAN_FIELDS
    if unknown:
        raise ValueError(f"unknown alignment plan fields: {sorted(unknown)}")
    roots, versions = {}, {}
    for side in ("left", "right"):
        spec = plan.get(side)
        if not isinstance(spec, dict) or set(spec) - {"root", "version"}:
            raise ValueError(f"plan {side} spec must contain only root and version")
        root, version = spec.get("root"), spec.get("version")
        if not isinstance(root, str) or not root:
            raise ValueError(f"plan {side} root must be a dataset path")
        if not isinstance(version, str) or not version:
            raise ValueError(f"plan {side} requires an explicit pinned snapshot version")
        roots[side], versions[side] = root, version
    window = plan.get("window")
    if not isinstance(window, dict) or set(window) - {"start", "end"}:
        raise ValueError("plan window must contain only start and end")
    start_micros, end_micros = epoch(window.get("start")), epoch(window.get("end"))
    if end_micros <= start_micros:
        raise ValueError("alignment window must be a nonempty half-open interval")
    tolerance = plan.get("tolerance_micros")
    if isinstance(tolerance, bool) or not isinstance(tolerance, int) or tolerance < 0:
        raise ValueError("tolerance_micros must be a non-negative integer")
    strategy = plan.get("strategy", "memory")
    if strategy not in _STRATEGIES:
        raise ValueError(f"strategy must be one of {_STRATEGIES}")
    limits = plan.get("limits")
    if not isinstance(limits, dict) or set(limits) != _LIMIT_FIELDS:
        raise ValueError("plan limits must declare max_resident_rows, max_temp_bytes and max_group_cells")
    max_resident, max_temp, max_cells = (_positive_int(limits[name], name) for name in
                                         ("max_resident_rows", "max_temp_bytes", "max_group_cells"))
    temp_dir = plan.get("temp_dir")
    if temp_dir is not None and (not isinstance(temp_dir, str) or not os.path.isdir(temp_dir)):
        raise ValueError("temp_dir must be an existing directory")
    dimensions = plan.get("dimensions")
    if not isinstance(dimensions, dict) or not dimensions:
        raise ValueError("plan dimensions must map every left dimension to a right dimension")
    if any(not isinstance(left, str) or not isinstance(right, str) for left, right in dimensions.items()):
        raise ValueError("dimension mapping keys and values must be strings")
    schemas = {side: Dataset(roots[side]).snapshot(versions[side])["schema"] for side in ("left", "right")}
    left_dims, right_dims = schemas["left"]["dimensions"], schemas["right"]["dimensions"]
    unmapped = sorted(set(left_dims) - set(dimensions))
    unknown_left = sorted(set(dimensions) - set(left_dims))
    if unmapped or unknown_left:
        raise ValueError(f"dimension mapping must cover every left dimension: unmapped {unmapped}, unknown {unknown_left}")
    targets = list(dimensions.values())
    uncovered = sorted(set(right_dims) - set(targets))
    if len(set(targets)) != len(targets) or uncovered:
        raise ValueError(f"dimension mapping must be a bijection onto the right dimensions: uncovered {uncovered}")
    for left, right in dimensions.items():
        if left_dims[left] != right_dims[right]:
            raise ValueError(f"dimension type mismatch for {left} -> {right}: {left_dims[left]} vs {right_dims[right]}")
    return Plan(left_root=roots["left"], left_version=versions["left"],
                right_root=roots["right"], right_version=versions["right"],
                start=window["start"], end=window["end"],
                start_micros=start_micros, end_micros=end_micros,
                dimensions=dict(dimensions), tolerance_micros=tolerance, strategy=strategy,
                max_resident_rows=max_resident, max_temp_bytes=max_temp, max_group_cells=max_cells,
                temp_dir=temp_dir)


def _prefer(first, second):
    """Better of two (pairs, cost, sequence) candidates for one DP cell."""
    if second[0] != first[0]:
        return second if second[0] > first[0] else first
    if second[1] != first[1]:
        return second if second[1] < first[1] else first
    return second if second[2] < first[2] else first


def solve_group(left_times, right_times, tolerance_micros, max_cells):
    """Exact one-to-one non-crossing alignment of two strictly increasing time lists.

    Objectives, in priority order: maximize the number of pairs, minimize the
    total absolute time difference, then take the lexicographically smallest
    pair sequence. Pair ``(i, j)`` compares as ``(left_times[i],
    right_times[j])`` because both lists are strictly increasing; row
    identities are carried by the caller and never change the order inside a
    group, where times are unique. Returns ``(pairs, unpaired_left,
    unpaired_right)`` as index lists. Raises ``AlignmentLimitError`` when the
    DP state would exceed ``max_cells``.
    """
    if isinstance(tolerance_micros, bool) or not isinstance(tolerance_micros, int) or tolerance_micros < 0:
        raise ValueError("tolerance_micros must be a non-negative integer")
    if isinstance(max_cells, bool) or not isinstance(max_cells, int) or max_cells < 1:
        raise ValueError("max_cells must be a positive integer")
    for times in (left_times, right_times):
        if any(isinstance(t, bool) or not isinstance(t, int) for t in times):
            raise ValueError("times must be integer microseconds")
        if any(b <= a for a, b in zip(times, times[1:])):
            raise ValueError("times must be strictly increasing")
    cells = (len(left_times) + 1) * (len(right_times) + 1)
    if cells > max_cells:
        raise AlignmentLimitError(f"group solve state needs {cells} cells, plan allows {max_cells}")
    rows, cols = len(left_times), len(right_times)
    dp = [[None] * (cols + 1) for _ in range(rows + 1)]
    for i in range(rows + 1):
        dp[i][cols] = (0, 0, ())
    for j in range(cols + 1):
        dp[rows][j] = (0, 0, ())
    for i in range(rows - 1, -1, -1):
        for j in range(cols - 1, -1, -1):
            best = _prefer(dp[i + 1][j], dp[i][j + 1])
            diff = left_times[i] - right_times[j]
            if -tolerance_micros <= diff <= tolerance_micros:
                rest = dp[i + 1][j + 1]
                best = _prefer(best, (rest[0] + 1, rest[1] + abs(diff), ((i, j),) + rest[2]))
            dp[i][j] = best
    pairs = dp[0][0][2]
    matched_left = {i for i, _ in pairs}
    matched_right = {j for _, j in pairs}
    return (list(pairs),
            [i for i in range(rows) if i not in matched_left],
            [j for j in range(cols) if j not in matched_right])


class _Ledger:
    """Accounts resident records and temporary bytes against plan limits."""

    def __init__(self, max_resident_rows, max_temp_bytes):
        self.max_resident = max_resident_rows
        self.max_temp = max_temp_bytes
        self.resident = 0
        self.peak_resident = 0
        self.temp = 0

    def hold(self, count, what):
        self.resident += count
        self.peak_resident = max(self.peak_resident, self.resident)
        if self.resident > self.max_resident:
            raise AlignmentLimitError(f"resident record limit exceeded while {what}: {self.resident} > {self.max_resident}")

    def release(self, count):
        self.resident -= count

    def spill(self, nbytes):
        self.temp += nbytes
        if self.temp > self.max_temp:
            raise AlignmentLimitError(f"temporary byte limit exceeded: {self.temp} > {self.max_temp}")


def _window_rows(plan, side):
    root = plan.left_root if side == "left" else plan.right_root
    version = plan.left_version if side == "left" else plan.right_version
    yield from Dataset(root).window(plan.start, plan.end, version=version)


def _record(plan, side, row):
    coordinates = row["coordinates"]
    if side == "right":
        mapped = {left: coordinates[right] for left, right in plan.dimensions.items()}
    else:
        mapped = coordinates
    key = json.dumps(mapped, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return key, {"us": epoch(row["time"]), "coordinates": coordinates}


def _record_order(record):
    return record["us"], json.dumps(record["coordinates"], sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _rowref(root, version, coordinates, micros):
    return {"dataset": root, "snapshot": version, "coordinates": coordinates, "time": iso(micros)}


def _reason(micros, peer_times, tolerance):
    if peer_times is None:
        return REASON_PEER_GROUP_MISSING
    at = bisect.bisect_left(peer_times, micros)
    for index in (at - 1, at):
        if 0 <= index < len(peer_times) and abs(peer_times[index] - micros) <= tolerance:
            return REASON_CANDIDATE_CLAIMED
    return REASON_NO_CANDIDATE


class _MemorySource:
    """Groups fully materialized in memory; rows already counted as resident."""
    track = False

    def __init__(self, groups):
        self._groups = groups

    def keys(self):
        return sorted(set(self._groups["left"]) | set(self._groups["right"]))

    def load(self, key):
        return self._groups["left"].get(key, []), self._groups["right"].get(key, [])


class _StagingSource:
    """Groups spilled to per-group JSONL partitions; loads are accounted."""
    track = True

    def __init__(self, files):
        self._files = files

    def keys(self):
        return sorted(set(self._files["left"]) | set(self._files["right"]))

    def load(self, key):
        sides = []
        for side in ("left", "right"):
            rows = []
            path = self._files[side].get(key)
            if path is not None:
                with open(path, encoding="utf-8") as stream:
                    for line in stream:
                        rows.append(json.loads(line))
            sides.append(rows)
        return sides[0], sides[1]


def _finish(plan, source, ledger):
    pairs, unpaired_left, unpaired_right = [], [], []
    peak_cells, group_count = 0, 0
    for key in source.keys():
        left_rows, right_rows = source.load(key)
        held = len(left_rows) + len(right_rows)
        if source.track:
            ledger.hold(held, "solving coordinate group")
        try:
            group_count += 1
            left_rows.sort(key=_record_order)
            right_rows.sort(key=_record_order)
            left_times = [row["us"] for row in left_rows]
            right_times = [row["us"] for row in right_rows]
            cells = (len(left_times) + 1) * (len(right_times) + 1)
            peak_cells = max(peak_cells, cells)
            idx_pairs, idx_left, idx_right = solve_group(left_times, right_times,
                                                         plan.tolerance_micros, plan.max_group_cells)
            mapped = json.loads(key)
            for i, j in idx_pairs:
                pairs.append({
                    "left": _rowref(plan.left_root, plan.left_version, left_rows[i]["coordinates"], left_rows[i]["us"]),
                    "right": _rowref(plan.right_root, plan.right_version, right_rows[j]["coordinates"], right_rows[j]["us"]),
                    "dt_micros": right_rows[j]["us"] - left_rows[i]["us"],
                    "group": mapped,
                })
            for i in idx_left:
                unpaired_left.append({
                    "row": _rowref(plan.left_root, plan.left_version, left_rows[i]["coordinates"], left_rows[i]["us"]),
                    "reason": _reason(left_rows[i]["us"], right_times or None, plan.tolerance_micros),
                    "group": mapped,
                })
            for j in idx_right:
                unpaired_right.append({
                    "row": _rowref(plan.right_root, plan.right_version, right_rows[j]["coordinates"], right_rows[j]["us"]),
                    "reason": _reason(right_rows[j]["us"], left_times or None, plan.tolerance_micros),
                    "group": mapped,
                })
        finally:
            if source.track:
                ledger.release(held)
    return pairs, unpaired_left, unpaired_right, {"groups": group_count, "peak_cells": peak_cells}


def _align_memory(plan, ledger):
    groups = {"left": {}, "right": {}}
    for side in ("left", "right"):
        for row in _window_rows(plan, side):
            ledger.hold(1, f"materializing {side} rows")
            key, record = _record(plan, side, row)
            groups[side].setdefault(key, []).append(record)
    return _finish(plan, _MemorySource(groups), ledger)


def _align_staging(plan, ledger):
    directory = tempfile.mkdtemp(prefix="arraybench-align-", dir=plan.temp_dir)
    try:
        files = {"left": {}, "right": {}}
        for side in ("left", "right"):
            for row in _window_rows(plan, side):
                ledger.hold(1, f"streaming {side} row")
                try:
                    key, record = _record(plan, side, row)
                    path = files[side].get(key)
                    if path is None:
                        ledger.hold(1, "tracking staged group")
                        path = os.path.join(directory, hashlib.sha256(f"{side}:{key}".encode()).hexdigest() + ".jsonl")
                        files[side][key] = path
                    line = (json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
                    ledger.spill(len(line))
                    with open(path, "ab") as stream:
                        stream.write(line)
                finally:
                    ledger.release(1)
        return _finish(plan, _StagingSource(files), ledger)
    finally:
        shutil.rmtree(directory, ignore_errors=True)


def align(plan):
    """Validate the plan, then align the two pinned snapshots exactly.

    Returns a JSON-ready dict with pair identities and signed time
    differences, both sides' unpaired rows with reasons, and a resource
    summary. Raises ``AlignmentLimitError`` on any limit overflow instead of
    degrading to an approximation.
    """
    resolved = validate_plan(plan)
    ledger = _Ledger(resolved.max_resident_rows, resolved.max_temp_bytes)
    if resolved.strategy == "staging":
        pairs, unpaired_left, unpaired_right, extra = _align_staging(resolved, ledger)
    else:
        pairs, unpaired_left, unpaired_right, extra = _align_memory(resolved, ledger)
    return {
        "left": {"root": resolved.left_root, "version": resolved.left_version},
        "right": {"root": resolved.right_root, "version": resolved.right_version},
        "window": {"start": resolved.start, "end": resolved.end},
        "dimensions": dict(resolved.dimensions),
        "tolerance_micros": resolved.tolerance_micros,
        "strategy": resolved.strategy,
        "pairs": pairs,
        "unpaired": {"left": unpaired_left, "right": unpaired_right},
        "summary": {
            "pairs": len(pairs),
            "unpaired_left": len(unpaired_left),
            "unpaired_right": len(unpaired_right),
            "groups": extra["groups"],
            "peak_resident_rows": ledger.peak_resident,
            "temp_bytes": ledger.temp,
            "peak_group_cells": extra["peak_cells"],
        },
    }
