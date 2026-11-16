import random
import tempfile
import unittest
from pathlib import Path

from arraybench import Dataset
from arraybench.align import AlignmentLimitError, align, solve_group
from arraybench.units import epoch, iso

START, END = "2026-11-01T00:00:00Z", "2026-11-02T00:00:00Z"
BASE = epoch(START)
LIMITS = {"max_resident_rows": 1000, "max_temp_bytes": 1 << 20, "max_group_cells": 10000}


def at_us(micros, station="east", value=20):
    return {"time": iso(BASE + micros), "coordinates": {"station": station},
            "values": {"temperature": value}, "units": {"temperature": "degC"}}


def brute_force(left, right, tolerance):
    """Independent exhaustive enumeration of every legal matching."""
    best = None

    def rec(i, j, pairs, cost):
        nonlocal best
        if i == len(left) or j == len(right):
            candidate = (len(pairs), cost, pairs)
            if best is None or (-candidate[0], candidate[1], candidate[2]) < (-best[0], best[1], best[2]):
                best = candidate
            return
        rec(i + 1, j, pairs, cost)
        rec(i, j + 1, pairs, cost)
        diff = abs(left[i] - right[j])
        if diff <= tolerance:
            rec(i + 1, j + 1, pairs + ((i, j),), cost + diff)

    rec(0, 0, (), 0)
    return list(best[2])


class AlignTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def make_dataset(self, name, batches, dims=None, **schema_over):
        dataset = Dataset(self.root / name)
        schema = {"dimensions": dims or {"station": "str"},
                  "variables": {"temperature": {"unit": "degC", "min": -100, "max": 100}}}
        schema.update(schema_over)
        dataset.create(schema)
        versions = [dataset.append(batch, invalid="missing") for batch in batches]
        return str(self.root / name), versions

    def plan(self, left, right, mapping=None, tolerance=0, strategy="memory", limits=None):
        return {"left": {"root": left[0], "version": left[1]},
                "right": {"root": right[0], "version": right[1]},
                "window": {"start": START, "end": END},
                "dimensions": mapping or {"station": "station"},
                "tolerance_micros": tolerance,
                "strategy": strategy,
                "limits": limits or dict(LIMITS)}

    def test_example_requires_non_greedy_two_pair_alignment(self):
        left_root, left_v = self.make_dataset("left", [[at_us(4_000_000), at_us(6_000_000)]])
        right_root, right_v = self.make_dataset("right", [[at_us(1_000_000), at_us(5_000_000)]])
        result = align(self.plan((left_root, left_v[-1]), (right_root, right_v[-1]), tolerance=3_000_000))
        # Per-row nearest-neighbour greedy pairs 4s->5s and stops at one pair;
        # the exact alignment must keep both pairs.
        self.assertEqual([(p["left"]["time"], p["right"]["time"], p["dt_micros"]) for p in result["pairs"]],
                         [(iso(BASE + 4_000_000), iso(BASE + 1_000_000), -3_000_000),
                          (iso(BASE + 6_000_000), iso(BASE + 5_000_000), -1_000_000)])
        self.assertEqual(result["unpaired"], {"left": [], "right": []})
        self.assertEqual(solve_group([4, 6], [1, 5], 3, 100)[0], [(0, 0), (1, 1)])

    def test_solver_objective_priorities(self):
        # More pairs beats a cheaper partial matching.
        self.assertEqual(solve_group([4, 6], [1, 5], 3, 100)[0], [(0, 0), (1, 1)])
        # Equal count: lower total absolute difference wins (cost 2 < cost 4).
        self.assertEqual(solve_group([0, 4, 6], [1, 5], 3, 100)[0], [(0, 0), (1, 1)])
        # Equal count and cost: lexicographically smallest pair sequence wins.
        self.assertEqual(solve_group([3, 5], [0, 4], 2, 100)[0], [(0, 1)])
        # Tolerance boundary is inclusive.
        self.assertEqual(solve_group([0], [3], 3, 100)[0], [(0, 0)])
        self.assertEqual(solve_group([0], [4], 3, 100)[0], [])

    def test_solver_matches_independent_exhaustive_enumeration(self):
        rng = random.Random(20261116)
        for _ in range(300):
            left = sorted(rng.sample(range(0, 16), rng.randint(0, 5)))
            right = sorted(rng.sample(range(0, 16), rng.randint(0, 5)))
            tolerance = rng.randint(0, 6)
            pairs, unpaired_left, unpaired_right = solve_group(left, right, tolerance, 10 ** 6)
            self.assertEqual(pairs, brute_force(left, right, tolerance))
            self.assertEqual(sorted([i for i, _ in pairs] + unpaired_left), list(range(len(left))))
            self.assertEqual(sorted([j for _, j in pairs] + unpaired_right), list(range(len(right))))

    def test_pinned_snapshots_ignore_later_head_advancement(self):
        left_root, left_v = self.make_dataset("left", [[at_us(4_000_000), at_us(6_000_000)]])
        right_root, right_v = self.make_dataset("right", [[at_us(1_000_000), at_us(5_000_000)]])
        plan = self.plan((left_root, left_v[-1]), (right_root, right_v[-1]), tolerance=3_000_000)
        before = align(plan)
        # HEAD advances on both sides after the plan inputs were pinned.
        Dataset(left_root).append([at_us(5_000_000, station="west")])
        Dataset(right_root).append([at_us(6_000_000)])
        after = align(plan)
        self.assertEqual(before, after)
        self.assertEqual(len(after["pairs"]), 2)

    def test_dimension_mapping_must_be_bijective_and_typed(self):
        def rows(coords):
            return [{"time": START, "coordinates": coords, "values": {"temperature": 20}}]

        left_root, left_v = self.make_dataset("left", [rows({"station": "east", "height": 1.5})],
                                              dims={"station": "str", "height": "float"})
        right_root, right_v = self.make_dataset("right", [rows({"site": "east", "level": 1.5})],
                                                dims={"site": "str", "level": "float"})
        base = self.plan((left_root, left_v[-1]), (right_root, right_v[-1]),
                         mapping={"station": "site", "height": "level"})
        # A renamed bijective mapping aligns normally.
        self.assertEqual(len(align(base)["pairs"]), 1)
        for broken in ({"station": "site"},                                  # height unmapped
                       {"station": "site", "height": "site"},                # not injective, level uncovered
                       {"station": "site", "height": "level", "x": "y"},     # unknown dimension
                       {"station": "level", "height": "site"}):              # type mismatch str->float
            with self.assertRaises(ValueError):
                align(dict(base, dimensions=broken))

    def test_null_measurements_are_still_pairable_observations(self):
        left_root, left_v = self.make_dataset("left", [[at_us(0, value=None)]])
        right_root, right_v = self.make_dataset("right", [[at_us(1_000_000)]])
        result = align(self.plan((left_root, left_v[-1]), (right_root, right_v[-1]), tolerance=2_000_000))
        self.assertEqual(len(result["pairs"]), 1)
        self.assertEqual(result["unpaired"], {"left": [], "right": []})

    def test_groups_never_borrow_rows_and_unpaired_reasons(self):
        left_root, left_v = self.make_dataset("left", [[
            at_us(0, station="lone"),       # no right group at all
            at_us(0), at_us(4_000_000), at_us(6_000_000),
            at_us(0, station="far"),        # right group exists but is out of tolerance
        ]])
        right_root, right_v = self.make_dataset("right", [[
            at_us(1_000_000), at_us(5_000_000),
            at_us(100_000_000, station="far"),
            at_us(0, station="solo"),       # no left group at all
        ]])
        result = align(self.plan((left_root, left_v[-1]), (right_root, right_v[-1]), tolerance=3_000_000))
        # Same timestamps in different groups ("lone"/"solo") must not pair.
        self.assertEqual([(p["left"]["coordinates"]["station"], p["dt_micros"]) for p in result["pairs"]],
                         [("east", 1_000_000), ("east", 1_000_000)])
        reasons = {(u["row"]["coordinates"]["station"], u["row"]["time"]): u["reason"]
                   for side in ("left", "right") for u in result["unpaired"][side]}
        self.assertEqual(reasons, {
            ("lone", iso(BASE)): "peer_group_missing",
            ("solo", iso(BASE)): "peer_group_missing",
            ("far", iso(BASE)): "no_candidate_within_tolerance",
            ("far", iso(BASE + 100_000_000)): "no_candidate_within_tolerance",
            # 6s had a feasible candidate (5s) but the optimal matching used it for 4s.
            ("east", iso(BASE + 6_000_000)): "candidate_claimed_by_better_match",
        })

    def test_tolerance_inclusive_and_window_end_excluded(self):
        left_root, left_v = self.make_dataset("left", [[at_us(0), at_us(10_000_000)]])
        right_root, right_v = self.make_dataset("right", [[
            at_us(3_000_000),
            {"time": END, "coordinates": {"station": "east"}, "values": {"temperature": 20}},
        ]])
        result = align(self.plan((left_root, left_v[-1]), (right_root, right_v[-1]), tolerance=3_000_000))
        # |dt| == tolerance pairs; the right row at exactly END never participates.
        self.assertEqual([(p["left"]["time"], p["right"]["time"]) for p in result["pairs"]],
                         [(iso(BASE), iso(BASE + 3_000_000))])
        unpaired_left = result["unpaired"]["left"]
        self.assertEqual(len(unpaired_left), 1)
        self.assertEqual(unpaired_left[0]["reason"], "no_candidate_within_tolerance")
        self.assertEqual(result["unpaired"]["right"], [])

    def test_negative_zero_and_zero_share_coordinate_identity(self):
        def row(micros, x):
            return {"time": iso(BASE + micros), "coordinates": {"x": x}, "values": {"temperature": 20}}

        left_root, left_v = self.make_dataset("left", [[row(0, -0.0)]], dims={"x": "float"})
        right_root, right_v = self.make_dataset("right", [[row(1_000_000, 0)]], dims={"x": "float"})
        result = align(self.plan((left_root, left_v[-1]), (right_root, right_v[-1]),
                                 mapping={"x": "x"}, tolerance=1_000_000))
        self.assertEqual(len(result["pairs"]), 1)
        self.assertEqual(result["pairs"][0]["left"]["coordinates"], {"x": 0.0})

    def test_input_traversal_order_and_buckets_do_not_change_alignment(self):
        left_times = [0, 500_000, 3_600_000_000, 7_200_000_000, 10_000_000]
        right_root, right_v = self.make_dataset("right", [[at_us(t) for t in (400_000, 3_600_500_000, 9_000_000)]])
        ordered, ordered_v = self.make_dataset("ordered", [[at_us(t) for t in left_times]])
        # Same rows, different append order and a different bucket layout.
        shuffled, shuffled_v = self.make_dataset("shuffled", [[at_us(t)] for t in reversed(left_times)],
                                                 bucket_seconds=1)
        plan_a = self.plan((ordered, ordered_v[-1]), (right_root, right_v[-1]), tolerance=1_000_000)
        plan_b = self.plan((shuffled, shuffled_v[-1]), (right_root, right_v[-1]), tolerance=1_000_000)

        def simplify(result):
            return ([(p["left"]["time"], p["right"]["time"], p["dt_micros"]) for p in result["pairs"]],
                    [(u["row"]["time"], u["reason"]) for u in result["unpaired"]["left"]])

        self.assertEqual(simplify(align(plan_a)), simplify(align(plan_b)))

    def test_limits_fail_explicitly_never_degrade_to_greedy(self):
        left_root, left_v = self.make_dataset("left", [[at_us(4_000_000), at_us(6_000_000)]])
        right_root, right_v = self.make_dataset("right", [[at_us(1_000_000), at_us(5_000_000)]])
        head_left, head_right = Dataset(left_root).store.head(), Dataset(right_root).store.head()
        sides = ((left_root, left_v[-1]), (right_root, right_v[-1]))
        with self.assertRaises(AlignmentLimitError):  # resident records: 4 rows > 3
            align(self.plan(*sides, tolerance=3_000_000, limits=dict(LIMITS, max_resident_rows=3)))
        with self.assertRaises(AlignmentLimitError):  # single group state: 2x2 needs 9 cells
            align(self.plan(*sides, tolerance=3_000_000, limits=dict(LIMITS, max_group_cells=8)))
        with self.assertRaises(AlignmentLimitError):  # staging temporaries
            align(self.plan(*sides, tolerance=3_000_000, strategy="staging",
                            limits=dict(LIMITS, max_temp_bytes=10)))
        with self.assertRaises(AlignmentLimitError):
            solve_group([1, 2, 3], [1, 2, 3], 0, 4)
        # Failures are explicit and nothing was written to the datasets.
        self.assertEqual(Dataset(left_root).store.head(), head_left)
        self.assertEqual(Dataset(right_root).store.head(), head_right)

    def test_staging_and_memory_strategies_agree(self):
        left_rows = [at_us(s * 1_000_000 + skew, station=name)
                     for s in range(8) for name, skew in (("a", 0), ("b", 300_000), ("c", 700_000))]
        right_rows = [at_us(s * 1_000_000 + 500_000, station=name)
                      for s in range(8) for name in ("a", "b", "c")]
        left_root, left_v = self.make_dataset("left", [left_rows], bucket_seconds=600)
        right_root, right_v = self.make_dataset("right", [right_rows], bucket_seconds=600)
        base = self.plan((left_root, left_v[-1]), (right_root, right_v[-1]), tolerance=600_000)
        memory = align(base)
        staging = align(dict(base, strategy="staging"))
        self.assertEqual(memory["pairs"], staging["pairs"])
        self.assertEqual(memory["unpaired"], staging["unpaired"])
        self.assertEqual(memory["summary"]["temp_bytes"], 0)
        self.assertGreater(staging["summary"]["temp_bytes"], 0)
        self.assertTrue(all(p["group"] for p in staging["pairs"]))

    def test_plan_validation_requires_pinned_explicit_inputs(self):
        left_root, left_v = self.make_dataset("left", [[at_us(0)]])
        right_root, right_v = self.make_dataset("right", [[at_us(0)]])
        sides = ((left_root, left_v[-1]), (right_root, right_v[-1]))
        for mutate in (lambda p: p["left"].update(version=None),       # HEAD is never a default
                       lambda p: p["left"].pop("version"),
                       lambda p: p.update(tolerance_micros=-1),
                       lambda p: p.update(tolerance_micros=1.5),
                       lambda p: p.update(window={"start": END, "end": START}),
                       lambda p: p.update(unknown_field=True),
                       lambda p: p.update(strategy="greedy"),
                       lambda p: p.update(limits={"max_resident_rows": 1})):
            plan = self.plan(*sides)
            mutate(plan)
            with self.assertRaises(ValueError):
                align(plan)

    def test_identical_timestamps_pair_with_zero_difference(self):
        left_root, left_v = self.make_dataset("left", [[at_us(1_000_000)]])
        right_root, right_v = self.make_dataset("right", [[at_us(1_000_000)]])
        result = align(self.plan((left_root, left_v[-1]), (right_root, right_v[-1]), tolerance=0))
        self.assertEqual([p["dt_micros"] for p in result["pairs"]], [0])

    def test_row_identity_resolves_back_through_window(self):
        left_root, left_v = self.make_dataset("left", [[at_us(0), at_us(1_000_000, station="west")]])
        right_root, right_v = self.make_dataset("right", [[at_us(500_000), at_us(1_500_000, station="west")]])
        result = align(self.plan((left_root, left_v[-1]), (right_root, right_v[-1]), tolerance=1_000_000))
        self.assertEqual(len(result["pairs"]), 2)
        versions = {"left": left_v[-1], "right": right_v[-1]}
        for pair in result["pairs"]:
            for side in ("left", "right"):
                ref = pair[side]
                self.assertEqual(ref["snapshot"], versions[side])
                rows = list(Dataset(ref["dataset"]).window(START, END, coordinates=ref["coordinates"],
                                                           version=ref["snapshot"]))
                self.assertTrue(any(row["time"] == ref["time"] for row in rows))


if __name__ == "__main__":
    unittest.main()
