import json
import math
import os
import random
import subprocess
import sys
import tempfile
import unittest
from itertools import combinations
from pathlib import Path

from arraybench import Dataset
from arraybench.segmentation import (
    COST_ABS_TOL, COST_REL_TOL, IntervalCostCache, plan_blocks, segment_rows,
)
from arraybench.units import epoch, iso

START = "2026-11-01T00:00:00Z"
T0 = epoch(START)
HOUR = 3600 * 1000000
MINUTE = 60 * 1000000

SCHEMA = {"dimensions": {"station": "str"},
          "variables": {"temperature": {"unit": "degC", "min": -100, "max": 100000000000000.0},
                        "pressure": {"unit": "kPa"}}}


def row(offset_seconds, value, station="east", variable="temperature", pressure=100.0):
    values = {"temperature": None, "pressure": pressure}
    values[variable] = value
    return {"time": iso(T0 + offset_seconds * 1000000),
            "coordinates": {"station": station}, "values": values, "invalid": []}


def series_rows(values, *, gap_seconds=60, station="east", start_offset=0):
    return [row(start_offset + index * gap_seconds, value, station=station)
            for index, value in enumerate(values)]


def run(rows, **overrides):
    params = dict(penalty=10.0, min_points=2, min_span_seconds=0,
                  max_gap_seconds=90, limits=None)
    params.update(overrides)
    return segment_rows(rows, schema=SCHEMA, variable="temperature",
                        snapshot_id="snap-1", start=START,
                        end="2026-11-02T00:00:00Z", **params)


def brute_force(values, times, penalty, min_points, min_span_micros):
    """Enumerate every legal segmentation and score it with fsum."""
    n = len(values)
    best = None
    for cuts in range(n):
        for cut_indices in combinations(range(1, n), cuts):
            bounds = (0, *cut_indices, n)
            if any(b - a < min_points for a, b in zip(bounds, bounds[1:])):
                continue
            if any(times[b - 1] - times[a] < min_span_micros
                   for a, b in zip(bounds, bounds[1:])):
                continue
            total = 0.0
            for a, b in zip(bounds, bounds[1:]):
                part = values[a:b]
                mean = math.fsum(part) / len(part)
                total += math.fsum((x - mean) ** 2 for x in part)
            objective = total + penalty * cuts
            # Arbitration: objective first, then fewer segments, then cuts.
            candidate = (objective, cuts + 1, tuple(times[index] for index in cut_indices))
            if best is None or candidate < best:
                best = candidate
    return best


class WorkedExampleTests(unittest.TestCase):
    def test_specification_example_global_optimum(self):
        # [0,8,9,12,12,12], min two points, zero span, penalty 10:
        # the optimum cut is after the SECOND point, objective 48.75.
        result = run(series_rows([0, 8, 9, 12, 12, 12]))
        block = result["groups"][0]["blocks"][0]
        self.assertEqual(block["status"], "estimated")
        self.assertEqual(block["autonomous_changepoints"], 1)
        self.assertAlmostEqual(block["objective"], 48.75)
        segments = block["segments"]
        self.assertEqual(len(segments), 2)
        self.assertEqual([segment["count"] for segment in segments], [2, 4])
        self.assertEqual(segments[0]["first_observation"]["time"], iso(T0))
        self.assertEqual(segments[0]["last_observation"]["time"], iso(T0 + MINUTE))
        self.assertAlmostEqual(segments[0]["mean"], 4.0)
        self.assertAlmostEqual(segments[0]["error"], 32.0)
        self.assertAlmostEqual(segments[1]["mean"], 11.25)
        self.assertAlmostEqual(segments[1]["error"], 6.75)
        # The larger later jump (after point three) is the worse segmentation.
        self.assertGreater(32.0 + 2.0 / 3.0 + 6.75 + 10.0, 48.75)

    def test_penalty_zero_allows_more_segments(self):
        # With min_points=2 the equal tail values [12,12,12] stay in one
        # zero-error segment instead of illegal singleton cuts.
        result = run(series_rows([0, 8, 9, 12, 12, 12]), penalty=0.0)
        block = result["groups"][0]["blocks"][0]
        # min_points=2 forbids singleton segments; best is [0,8]|[9,12]|[12,12].
        self.assertAlmostEqual(block["objective"], 36.5)
        self.assertEqual(block["autonomous_changepoints"], 2)
        self.assertEqual([segment["count"] for segment in block["segments"]], [2, 2, 2])
        singleton = run(series_rows([0, 8, 9, 12, 12, 12]), penalty=0.0, min_points=1)
        block = singleton["groups"][0]["blocks"][0]
        self.assertAlmostEqual(block["objective"], 0.0)
        # [0],[8],[9],[12,12,12]: splitting the equal tail costs nothing, but
        # the tie rule prefers the fewest segments, so all three 12s merge.
        self.assertEqual(block["autonomous_changepoints"], 3)
        self.assertEqual([segment["count"] for segment in block["segments"]],
                         [1, 1, 1, 3])
        self.assertTrue(all(segment["error"] == 0.0 for segment in block["segments"]))


class BruteForceAgreementTests(unittest.TestCase):
    def _check_sequence(self, values, penalty, min_points, min_span_seconds=0):
        rows = series_rows(values)
        result = run(rows, penalty=penalty, min_points=min_points,
                     min_span_seconds=min_span_seconds)
        block = result["groups"][0]["blocks"][0]
        times = [T0 + index * MINUTE for index in range(len(values))]
        expected = brute_force(values, times, penalty, min_points,
                               min_span_seconds * 1000000)
        if expected is None:
            self.assertEqual(block["status"], "not_estimable")
            return
        self.assertEqual(block["status"], "estimated")
        self.assertTrue(math.isclose(block["objective"], expected[0],
                                     rel_tol=COST_REL_TOL, abs_tol=COST_ABS_TOL))
        cut_times = tuple(epoch(segment["first_observation"]["time"])
                          for segment in block["segments"][1:])
        # Exact float ties must follow the declared arbitration exactly.
        # Near-ties between different shapes may pick either shape; both
        # objectives must still agree within the declared tolerance.
        if block["objective"] == expected[0]:
            self.assertEqual(block["autonomous_changepoints"] + 1, expected[1])
            self.assertEqual(cut_times, expected[2])
        # Independently recompute the DP-selected segmentation with fsum.
        bounds = [0]
        for cut in cut_times:
            bounds.append(times.index(cut))
        bounds.append(len(values))
        independent = math.fsum(
            (lambda part: math.fsum((x - math.fsum(part) / len(part)) ** 2 for x in part))
            (values[a:b]) for a, b in zip(bounds, bounds[1:]))
        independent += penalty * len(cut_times)
        self.assertTrue(math.isclose(block["objective"], independent,
                                     rel_tol=COST_REL_TOL, abs_tol=COST_ABS_TOL))
        for segment in block["segments"]:
            self.assertGreaterEqual(segment["count"], min_points)

    def test_enumerate_all_legal_segmentations(self):
        rng = random.Random(20261116)
        for length in range(2, 9):
            for _ in range(40):
                values = [rng.choice([0, 1, 2, 4, 8, 9, 12]) for _ in range(length)]
                for penalty in (0.0, 1.0, 10.0, 100.0):
                    for min_points in (1, 2, 3):
                        self._check_sequence(values, penalty, min_points)

    def test_minimum_span_enumerated(self):
        rng = random.Random(7)
        for _ in range(20):
            values = [rng.randint(0, 20) for _ in range(6)]
            self._check_sequence(values, 5.0, 1, min_span_seconds=90)
            self._check_sequence(values, 5.0, 2, min_span_seconds=120)

    def test_tie_fewer_segments_then_lexicographic_cut_times(self):
        # Constant series: every partition has zero error -> fewest wins.
        result = run(series_rows([5, 5, 5, 5]), penalty=0.0, min_points=1)
        block = result["groups"][0]["blocks"][0]
        self.assertEqual(len(block["segments"]), 1)
        # [0,0,1,2,2] with penalty 1.0: the two one-cut plans both score
        # 5/3 and strictly beat the single segment (3.0) and finer cuts; the
        # lexicographically smallest (earliest) cut time must win.
        result = run(series_rows([0, 0, 1, 2, 2]), penalty=1.0, min_points=1)
        block = result["groups"][0]["blocks"][0]
        self.assertEqual(block["autonomous_changepoints"], 1)
        self.assertAlmostEqual(block["objective"], 5.0 / 3.0)
        self.assertEqual([segment["count"] for segment in block["segments"]], [2, 3])
        self.assertEqual(block["segments"][0]["first_observation"]["time"], iso(T0))


class ForcedBoundaryTests(unittest.TestCase):
    def test_null_observations_force_blocks_and_are_not_zero(self):
        rows = series_rows([1, 1, None, 3, 3])
        result = run(rows, penalty=0.0, min_points=2)
        blocks = result["groups"][0]["blocks"]
        self.assertEqual(len(blocks), 2)
        self.assertEqual([block["count"] for block in blocks], [2, 2])
        self.assertEqual(blocks[0]["end_reason"], "forced:missing")
        self.assertEqual(blocks[1]["start_reason"], "forced:missing")
        self.assertAlmostEqual(blocks[0]["segments"][0]["mean"], 1.0)
        self.assertAlmostEqual(blocks[1]["segments"][0]["mean"], 3.0)
        # Forced boundaries never pay an autonomous changepoint penalty.
        penalized = run(rows, penalty=1e6, min_points=2)
        for block in penalized["groups"][0]["blocks"]:
            self.assertEqual(block["autonomous_changepoints"], 0)
            self.assertAlmostEqual(block["objective"], 0.0)

    def test_null_at_edges_and_all_null_group(self):
        result = run(series_rows([None, 4, 4, None]), min_points=2)
        blocks = result["groups"][0]["blocks"]
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0]["start_reason"], "forced:missing")
        self.assertEqual(blocks[0]["end_reason"], "forced:missing")
        self.assertAlmostEqual(blocks[0]["segments"][0]["mean"], 4.0)
        empty = run(series_rows([None, None]), min_points=2)
        self.assertEqual(empty["groups"][0]["blocks"], [])

    def test_long_gap_forces_boundary_equality_does_not(self):
        # Gaps: 30s (within), 170s (strictly over 90s), 1s (within).
        rows = [row(0, 1), row(30, 1), row(200, 5), row(201, 5)]
        result = run(rows, max_gap_seconds=90, min_points=2, penalty=1e6)
        blocks = result["groups"][0]["blocks"]
        self.assertEqual(len(blocks), 2)
        self.assertEqual(blocks[0]["end_reason"], "forced:gap")
        self.assertEqual(blocks[1]["start_reason"], "forced:gap")
        self.assertEqual(blocks[0]["boundary_time"], iso(T0 + 200 * 1000000))
        # Every gap exactly equal to the limit must NOT split.
        joined = run([row(0, 1), row(90, 1), row(180, 5), row(270, 5)],
                     max_gap_seconds=90, min_points=2)
        self.assertEqual(len(joined["groups"][0]["blocks"]), 1)

    def test_other_variable_invalid_does_not_cut_target_blocks(self):
        rows = [row(0, 1, pressure=None), row(60, 1, pressure="bad"),
                row(120, 3, pressure=None)]
        result = run(rows, min_points=2, penalty=0.0)
        blocks = result["groups"][0]["blocks"]
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0]["count"], 3)


class NotEstimableTests(unittest.TestCase):
    def test_short_block_returns_not_estimable_without_fabricated_mean(self):
        result = run(series_rows([7]), min_points=2)
        block = result["groups"][0]["blocks"][0]
        self.assertEqual(block["status"], "not_estimable")
        self.assertEqual(block["reason"], "min_points")
        self.assertNotIn("segments", block)
        self.assertNotIn("mean", block)
        self.assertEqual(block["first_observation"]["time"], iso(T0))

    def test_span_too_short_returns_not_estimable(self):
        result = run(series_rows([1, 2, 3]), min_points=2, min_span_seconds=600)
        block = result["groups"][0]["blocks"][0]
        self.assertEqual(block["status"], "not_estimable")
        self.assertEqual(block["reason"], "min_span")
        self.assertAlmostEqual(block["span_seconds"], 120.0)

    def test_trivial_single_point_estimable_when_allowed(self):
        result = run(series_rows([7.5]), min_points=1)
        block = result["groups"][0]["blocks"][0]
        self.assertEqual(block["status"], "estimated")
        self.assertEqual(block["segments"][0]["mean"], 7.5)
        self.assertEqual(block["segments"][0]["error"], 0.0)

    def test_empty_window(self):
        result = run([], min_points=2)
        self.assertEqual(result["groups"], [])


class NumericalStabilityTests(unittest.TestCase):
    BASE = 10 ** 12

    def test_exact_translation_preserves_cuts_and_errors(self):
        small = [0, 8, 9, 12, 12, 12]
        shifted = [self.BASE + value for value in small]
        plain = run(series_rows(small))
        high = run(series_rows(shifted))
        plain_block, high_block = plain["groups"][0]["blocks"][0], high["groups"][0]["blocks"][0]
        self.assertEqual([s["count"] for s in plain_block["segments"]],
                         [s["count"] for s in high_block["segments"]])
        self.assertEqual(plain_block["autonomous_changepoints"],
                         high_block["autonomous_changepoints"])
        self.assertTrue(math.isclose(plain_block["objective"], high_block["objective"],
                                     rel_tol=COST_REL_TOL, abs_tol=COST_ABS_TOL))
        for plain_segment, high_segment in zip(plain_block["segments"], high_block["segments"]):
            self.assertTrue(math.isclose(plain_segment["error"], high_segment["error"],
                                         rel_tol=COST_REL_TOL, abs_tol=COST_ABS_TOL))
            self.assertTrue(math.isclose(high_segment["mean"],
                                         plain_segment["mean"] + self.BASE,
                                         rel_tol=COST_REL_TOL, abs_tol=COST_ABS_TOL))
            self.assertGreaterEqual(high_segment["error"], 0.0)

    def test_naive_sum_of_squares_loses_precision_but_cache_does_not(self):
        # Demonstrates why clamping negative costs to zero would hide an error:
        # the textbook one-pass formula collapses on a large constant baseline.
        values = [self.BASE + value for value in (0, 8, 9, 12, 12, 12)]
        naive = sum(v * v for v in values) - sum(values) ** 2 / len(values)
        true_sse = 108.0 + 5.0 / 6.0
        self.assertLess(naive, true_sse * 0.5)  # wildly wrong, often <= 0
        cache = IntervalCostCache()
        for value in values:
            cache.append(value)
        self.assertTrue(math.isclose(cache.sse(0, len(values) - 1), true_sse,
                                     rel_tol=COST_REL_TOL, abs_tol=COST_ABS_TOL))
        self.assertGreater(cache.sse(0, len(values) - 1), 0.0)

    def test_cost_cache_means_and_sse_match_reference(self):
        rng = random.Random(11)
        values = [rng.uniform(-50, 50) for _ in range(12)]
        cache = IntervalCostCache()
        for value in values:
            cache.append(value)
        for first in range(len(values)):
            for last in range(first, len(values)):
                part = values[first:last + 1]
                mean = math.fsum(part) / len(part)
                sse = math.fsum((x - mean) ** 2 for x in part)
                self.assertTrue(math.isclose(cache.sse(first, last), sse,
                                             rel_tol=COST_REL_TOL, abs_tol=COST_ABS_TOL))
                self.assertTrue(math.isclose(cache.mean(first, last), mean,
                                             rel_tol=COST_REL_TOL, abs_tol=COST_ABS_TOL))


class PlanBlocksTests(unittest.TestCase):
    def test_plan_blocks_boundary_reasons(self):
        records = [(0, 1.0), (60, 2.0), (120, None), (300, 3.0), (360, 4.0)]
        blocks = plan_blocks(records, 90 * 1000000)
        self.assertEqual([(b["start"], b["end"]) for b in blocks], [(0, 1), (3, 4)])
        self.assertEqual(blocks[0]["start_reason"], "edge")
        self.assertEqual(blocks[0]["end_reason"], "forced:missing")
        self.assertEqual(blocks[1]["start_reason"], "forced:missing")
        self.assertEqual(blocks[1]["end_reason"], "edge")
        gap_records = [(0, 1.0), (10, 2.0), (500, 3.0)]
        blocks = plan_blocks(gap_records, 100)
        self.assertEqual([(b["start"], b["end"]) for b in blocks], [(0, 1), (2, 2)])
        self.assertEqual(blocks[0]["end_reason"], "forced:gap")
        self.assertEqual(blocks[1]["start_reason"], "forced:gap")


class DatasetIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "data"
        self.dataset = Dataset(self.root)
        self.dataset.create({
            "dimensions": {"station": "str"},
            "variables": {"temperature": {"unit": "degC", "min": -100, "max": 200},
                          "pressure": {"unit": "kPa"}},
            "bucket_seconds": 3600, "chunk_rows": 2,
            "max_bucket_rows": 100, "max_batch_rows": 100})

    def _append_series(self, station, values, start_offset=0):
        rows = [{"time": iso(T0 + (start_offset + index) * MINUTE),
                 "coordinates": {"station": station},
                 "values": {"temperature": value, "pressure": 100.0}}
                for index, value in enumerate(values)]
        return self.dataset.append(rows)

    def test_cross_chunk_and_half_open_window(self):
        head_before = self.dataset.store.head()
        version = self._append_series("east", [0, 8, 9, 12, 12, 12])
        result = self.dataset.segment(
            START, "2026-11-01T02:00:00Z", "temperature",
            penalty=10, min_points=2, min_span_seconds=0, max_gap_seconds=90)
        self.assertEqual(result["snapshot"], version)
        block = result["groups"][0]["blocks"][0]
        self.assertAlmostEqual(block["objective"], 48.75)
        # Snapshot is fixed and untouched by the read-only analysis.
        self.assertEqual(self.dataset.store.head(), version)
        self.assertEqual(len(list(self.dataset.window(START, "2026-11-01T02:00:00Z"))), 6)
        # Half-open end: the last observation on the boundary is excluded.
        clipped = self.dataset.segment(
            START, iso(T0 + 5 * MINUTE), "temperature",
            penalty=10, min_points=2, min_span_seconds=0, max_gap_seconds=90)
        self.assertEqual(clipped["groups"][0]["blocks"][0]["count"], 5)
        self.assertNotEqual(head_before, version)

    def test_historical_snapshot_binding_survives_rollback(self):
        initial = self.dataset.store.head()
        v1 = self._append_series("east", [1, 1, 5, 5])
        self.dataset.rollback(initial)
        self.assertEqual(list(self.dataset.window(START, "2026-11-02T00:00:00Z")), [])
        historical = self.dataset.segment(
            START, "2026-11-02T00:00:00Z", "temperature", version=v1,
            penalty=1, min_points=2, min_span_seconds=0, max_gap_seconds=90)
        self.assertEqual(historical["snapshot"], v1)
        self.assertEqual(historical["groups"][0]["blocks"][0]["count"], 4)

    def test_coordinate_groups_and_traversal_order(self):
        self._append_series("east", [0, 8, 9, 12])
        self._append_series("west", [5, 5, 1, 1])
        ordered = self.dataset.segment(
            START, "2026-11-02T00:00:00Z", "temperature",
            penalty=0, min_points=1, min_span_seconds=0, max_gap_seconds=90)
        raw = list(self.dataset.window(START, "2026-11-02T00:00:00Z"))
        shuffled = raw[:]
        random.Random(3).shuffle(shuffled)
        reordered = segment_rows(
            iter(shuffled), schema=self.dataset.snapshot()["schema"],
            variable="temperature", snapshot_id=ordered["snapshot"],
            start=START, end="2026-11-02T00:00:00Z",
            penalty=0, min_points=1, min_span_seconds=0, max_gap_seconds=90)
        self.assertEqual(json.dumps(ordered, sort_keys=True),
                         json.dumps(reordered, sort_keys=True))
        self.assertEqual([group["coordinates"]["station"] for group in ordered["groups"]],
                         ["east", "west"])

    def test_cli_segment_command(self):
        self._append_series("east", [0, 8, 9, 12, 12, 12])
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1] / "src"))
        completed = subprocess.run(
            [sys.executable, "-m", "arraybench.cli", "--root", str(self.root),
             "segment", START, "2026-11-02T00:00:00Z",
             "--variable", "temperature", "--penalty", "10",
             "--min-points", "2", "--min-span-seconds", "0",
             "--max-gap-seconds", "90"],
            env=env, check=True, capture_output=True, text=True)
        payload = json.loads(completed.stdout)
        self.assertAlmostEqual(payload["groups"][0]["blocks"][0]["objective"], 48.75)


class MemoryLimitTests(unittest.TestCase):
    def _two_group_rows(self):
        rows = series_rows([1, 2, 3, 4], station="east")
        rows += series_rows([9, 8, 7, 6], station="west", start_offset=100)
        return rows

    def test_spill_succeeds_when_total_exceeds_resident_cap(self):
        rows = self._two_group_rows()
        unlimited = run(rows, min_points=2)
        bounded = run(rows, min_points=2,
                      limits={"max_resident_records": 3, "max_staging_bytes": 1 << 20,
                              "max_group_observations": 100})
        self.assertGreaterEqual(bounded["execution"]["spilled_groups"], 1)
        self.assertEqual(bounded["groups"], unlimited["groups"])

    def test_staging_byte_limit_is_enforced_explicitly(self):
        rows = self._two_group_rows()
        with self.assertRaises(ValueError) as context:
            run(rows, min_points=2,
                limits={"max_resident_records": 2, "max_staging_bytes": 1,
                        "max_group_observations": 100})
        self.assertIn("staging byte limit", str(context.exception))

    def test_group_count_beyond_resident_cap_rejected(self):
        rows = [row(index, 1, station=f"s{index}") for index in range(4)]
        with self.assertRaises(ValueError) as context:
            run(rows, min_points=1,
                limits={"max_resident_records": 2, "max_staging_bytes": 1 << 20,
                        "max_group_observations": 100})
        self.assertIn("resident record limit", str(context.exception))

    def test_single_group_solver_limit_rejected_not_thresholded(self):
        rows = series_rows([1, 2, 3, 4, 5])
        with self.assertRaises(ValueError) as context:
            run(rows, min_points=1,
                limits={"max_resident_records": 100, "max_staging_bytes": 1 << 20,
                        "max_group_observations": 4})
        self.assertIn("per-group solver limit", str(context.exception))
        # No greedy fallback: the same data under a legal limit is exact.
        legal = run(rows, min_points=1,
                    limits={"max_resident_records": 100, "max_staging_bytes": 1 << 20,
                            "max_group_observations": 5})
        self.assertEqual(legal["groups"][0]["blocks"][0]["status"], "estimated")

    def test_resident_cap_within_one_group_still_solves_globally(self):
        # The resident cap is smaller than one block: streaming/spill must not
        # permanently settle local cuts before the block finishes.
        rows = series_rows([0, 8, 9, 12, 12, 12])
        bounded = run(rows,
                      limits={"max_resident_records": 2, "max_staging_bytes": 1 << 20,
                              "max_group_observations": 100})
        block = bounded["groups"][0]["blocks"][0]
        self.assertAlmostEqual(block["objective"], 48.75)
        self.assertEqual(block["autonomous_changepoints"], 1)


class ValidationTests(unittest.TestCase):
    def test_bad_arguments(self):
        rows = series_rows([1, 2])
        with self.assertRaises(ValueError):
            run(rows, penalty=-1)
        with self.assertRaises(ValueError):
            run(rows, min_points=0)
        with self.assertRaises(ValueError):
            run(rows, min_span_seconds=-1)
        with self.assertRaises(ValueError):
            run(rows, max_gap_seconds=float("nan"))
        with self.assertRaises(ValueError):
            segment_rows(rows, schema=SCHEMA, variable="unknown", snapshot_id="x",
                         start=START, end="2026-11-02T00:00:00Z",
                         penalty=1, min_points=1, min_span_seconds=0,
                         max_gap_seconds=90)
        with self.assertRaises(ValueError):
            run(rows, limits={"max_resident_records": 0})
        with self.assertRaises(ValueError):
            run(rows, limits={"unknown_limit": 1})


class MultiDimensionalCoordinateTests(unittest.TestCase):
    def test_full_coordinate_tuple_groups_float_and_string_dims(self):
        schema = {"dimensions": {"station": "str", "height": "float"},
                  "variables": {"temperature": {"unit": "degC"}}}
        rows = [
            {"time": iso(T0), "coordinates": {"station": "east", "height": 10.0},
             "values": {"temperature": 1.0}, "invalid": []},
            {"time": iso(T0 + MINUTE), "coordinates": {"station": "east", "height": 10.0},
             "values": {"temperature": 2.0}, "invalid": []},
            {"time": iso(T0), "coordinates": {"station": "east", "height": 20.0},
             "values": {"temperature": 9.0}, "invalid": []},
            {"time": iso(T0 + MINUTE), "coordinates": {"station": "west", "height": 10.0},
             "values": {"temperature": 5.0}, "invalid": []},
        ]
        result = segment_rows(rows, schema=schema, variable="temperature",
                              snapshot_id="snap-x", start=START,
                              end="2026-11-02T00:00:00Z",
                              penalty=1, min_points=1, min_span_seconds=0,
                              max_gap_seconds=90)
        self.assertEqual(len(result["groups"]), 3)
        signatures = {tuple(sorted(group["coordinates"].items())): group
                      for group in result["groups"]}
        target = signatures[(("height", 10.0), ("station", "east"))]
        self.assertEqual(target["blocks"][0]["segments"][0]["first_observation"]["coordinates"],
                         {"station": "east", "height": 10.0})

    def test_not_estimable_block_keeps_boundary_reasons(self):
        rows = [row(0, 1), row(60, None), row(300, 4)]
        result = run(rows, min_points=2)
        blocks = result["groups"][0]["blocks"]
        self.assertEqual([block["status"] for block in blocks],
                         ["not_estimable", "not_estimable"])
        self.assertEqual(blocks[0]["reason"], "min_points")
        self.assertEqual(blocks[0]["end_reason"], "forced:missing")
        self.assertEqual(blocks[1]["start_reason"], "forced:missing")
        self.assertEqual(blocks[0]["first_observation"]["time"], iso(T0))
        self.assertEqual(blocks[1]["last_observation"]["time"], iso(T0 + 300 * 1000000))


if __name__ == "__main__":
    unittest.main()
