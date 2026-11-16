import tempfile
import unittest
from pathlib import Path
from arraybench import Dataset

START, END = "2026-11-01T00:00:00Z", "2026-11-02T00:00:00Z"


def point(time=START, station="east", value=20, unit="degC"):
    return {"time": time, "coordinates": {"station": station}, "values": {"temperature": value}, "units": {"temperature": unit}}


class ArrayTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.dataset = Dataset(self.root)
        self.initial = self.dataset.create({"dimensions": {"station": "str"}, "variables": {"temperature": {"unit": "degC", "min": -100, "max": 100}},
                                            "chunk_rows": 2, "max_bucket_rows": 5, "max_batch_rows": 10})
        self.addCleanup(self.tmp.cleanup)

    def test_units_and_timezone_normalization(self):
        self.dataset.append([point("2026-11-01T08:00:00+08:00", value=293.15, unit="K")])
        rows = list(self.dataset.window(START, END))
        self.assertEqual(rows[0]["time"], START)
        self.assertAlmostEqual(rows[0]["values"]["temperature"], 20)

    def test_copy_on_write_and_historical_window(self):
        v1 = self.dataset.append([point(), point("2026-11-01T01:00:00Z", value=30)])
        before = self.dataset.snapshot(v1)
        v2 = self.dataset.append([point(value=25)], duplicates="update")
        after = self.dataset.snapshot(v2)
        buckets = sorted(before["buckets"], key=int)
        self.assertEqual(before["buckets"][buckets[1]], after["buckets"][buckets[1]])
        self.assertNotEqual(before["buckets"][buckets[0]], after["buckets"][buckets[0]])
        self.assertEqual(list(self.dataset.window(START, END, version=v1))[0]["values"]["temperature"], 20)

    def test_duplicate_policies_and_atomic_failure(self):
        head = self.dataset.append([point()])
        with self.assertRaises(ValueError): self.dataset.append([point(value=25)])
        self.assertEqual(self.dataset.store.head(), head)
        self.dataset.append([point(value=30)], duplicates="ignore")
        self.assertEqual(list(self.dataset.window(START, END))[0]["values"]["temperature"], 20)

    def test_missing_values_and_resampling(self):
        self.dataset.append([point(value=20), point("2026-11-01T00:10:00Z", value=30),
                             point("2026-11-01T00:20:00Z", value="broken")], invalid="missing")
        rows = list(self.dataset.resample(START, END, 3600))
        self.assertEqual(rows[0]["values"]["temperature"], 25)
        self.assertEqual(rows[0]["counts"]["temperature"], 2)
        stats = self.dataset.stats(START, END)["temperature"]
        self.assertEqual((stats["count"], stats["missing"], stats["variance"]), (2, 1, 25))

    def test_dimension_mismatch_never_silently_suppressed(self):
        with self.assertRaises(ValueError): self.dataset.append([point(unit="m")], invalid="missing")
        self.assertEqual(self.dataset.store.head(), self.initial)

    def test_memory_limits_abort_without_new_head(self):
        with self.assertRaises(ValueError):
            self.dataset.append([point(station=str(i)) for i in range(6)])
        self.assertEqual(self.dataset.store.head(), self.initial)
        with self.assertRaises(ValueError):
            self.dataset.append([point(station=str(i)) for i in range(11)])

    def test_rollback_persistence_and_optimistic_guard(self):
        v1 = self.dataset.append([point()])
        with self.assertRaises(ValueError): self.dataset.append([point(station="west")], expected=self.initial)
        self.dataset.rollback(self.initial, expected=v1)
        other = Dataset(self.root)
        self.assertEqual(list(other.window(START, END)), [])
        self.assertEqual(other.snapshot()["revision"], 2)

    def test_checksum_corruption_is_detected(self):
        self.dataset.append([point()])
        sha = next(iter(self.dataset.snapshot()["buckets"].values()))[0]
        (self.root / "chunks" / (sha + ".json")).write_text("[]")
        with self.assertRaises(ValueError): list(self.dataset.window(START, END))

    def test_window_half_open_and_coordinate_filters(self):
        self.dataset.append([point(), point(station="west"), point(END)])
        self.assertEqual(len(list(self.dataset.window(START, END, coordinates={"station": "east"}))), 1)
        with self.assertRaises(ValueError): list(self.dataset.window(START, END, coordinates={"unknown": "x"}))

    def test_naive_time_and_invalid_coordinates(self):
        with self.assertRaises(ValueError): self.dataset.append([point("2026-11-01T00:00:00")])
        with self.assertRaises(ValueError): self.dataset.append([point(station="")])

    def test_null_not_flagged_but_bad_range_flagged(self):
        self.dataset.append([point(value=None), point(station="west", value=200)], invalid="missing")
        rows = list(self.dataset.window(START, END))
        self.assertEqual([row["invalid"] for row in rows], [[], ["temperature"]])

    def test_invalid_range_contract_is_rejected_at_creation(self):
        for spec in ({"unit": "degC", "min": "cold"}, {"unit": "degC", "min": 10, "max": 0}):
            with tempfile.TemporaryDirectory() as root:
                with self.assertRaises(ValueError):
                    Dataset(root).create({"dimensions": {"x": "str"}, "variables": {"temperature": spec}})

    def test_numeric_coordinate_zero_identity(self):
        with tempfile.TemporaryDirectory() as root:
            data = Dataset(root)
            data.create({"dimensions": {"x": "float"}, "variables": {"temperature": {"unit": "degC"}}})
            a = {"time": START, "coordinates": {"x": -0.0}, "values": {"temperature": 20}}
            data.append([a])
            a["coordinates"]["x"] = 0
            with self.assertRaises(ValueError): data.append([a])


if __name__ == "__main__": unittest.main()
