"""Tests for the streamed append protocol and its resource bounds."""
import json
import multiprocessing
import os
import tempfile
import unittest
from pathlib import Path

from arraybench import Dataset
from arraybench.streaming import Staging
from arraybench.storage import SnapshotConflict
from arraybench.units import iso

START, END = "2026-11-01T00:00:00Z", "2026-11-02T00:00:00Z"


def schema(**overrides):
    value = {"dimensions": {"station": "str"},
             "variables": {"temperature": {"unit": "degC", "min": -100, "max": 100}},
             "bucket_seconds": 3600, "chunk_rows": 2,
             "max_bucket_rows": 40, "max_batch_rows": 3}
    value.update(overrides)
    return value


def point(index=0, station=None, value=20, unit="degC", hour=0):
    return {"time": f"2026-11-01T0{hour}:{index:02d}:00Z",
            "coordinates": {"station": station if station is not None else f"s{index}"},
            "values": {"temperature": value},
            "units": {"temperature": unit}}


def stream(stopper=None):
    """A one-shot generator: iterating it twice yields nothing the second time."""
    def generate():
        for item in stopper or ():
            yield item
    return generate()


def from_degf(base, celsius):
    """Same key as ``base`` but supplied in Fahrenheit."""
    copy = dict(base)
    copy["values"] = {"temperature": celsius * 9 / 5 + 32}
    copy["units"] = {"temperature": "degF"}
    return copy


class Counting:
    """Wraps an iterable and counts how many records were pulled from it."""
    def __init__(self, rows):
        self.rows = rows
        self.pulled = 0

    def __iter__(self):
        for row in self.rows:
            self.pulled += 1
            yield row


def reference_model(rows, duplicates, invalid="reject", bucket_seconds=3600):
    """Independent, self-contained one-shot semantics.

    Deliberately avoids ``Dataset`` so the streamed implementation is checked
    against a second, obviously-correct model rather than against itself.
    Duplicate detection covers both within-batch repeats and repeats against
    the pre-existing (empty here) state.
    """
    from arraybench.units import UnitRegistry, epoch

    units = UnitRegistry()
    output = {}
    seen = set()
    for row in rows:
        micros = epoch(row["time"])
        key = (micros, json.dumps(row["coordinates"], sort_keys=True, separators=(",", ":")))
        values, invalid_vars = {}, []
        for name, spec in {"temperature": {"unit": "degC", "min": -100, "max": 100}}.items():
            unit = row.get("units", {}).get(name, spec["unit"])
            units.convert(None, unit, spec["unit"])  # dimension errors propagate
            try:
                value = units.convert(row["values"][name], unit, spec["unit"])
                if value is not None and (value < spec["min"] or value > spec["max"]):
                    raise ValueError("range")
            except (ValueError, TypeError):
                if invalid == "reject":
                    raise
                value, invalid_vars = None, [name]
            values[name] = value
        normalized = {"t": micros, "coordinates": dict(row["coordinates"]),
                      "values": values, "invalid": invalid_vars}
        if key in seen:
            if duplicates == "error":
                raise ValueError("duplicate coordinate and timestamp")
            if duplicates == "ignore":
                continue
        seen.add(key)
        output[key] = normalized
    buckets = {}
    for key, row in output.items():
        buckets.setdefault(str(row["t"] // (bucket_seconds * 1000000)), []).append(row)
    return {bucket: sorted(items, key=lambda r: (r["t"], json.dumps(r["coordinates"], sort_keys=True, separators=(",", ":"))))
            for bucket, items in buckets.items()}


class StreamingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.dataset = Dataset(self.root)
        self.initial = self.dataset.create(schema())
        self.addCleanup(self.tmp.cleanup)

    def long_input(self, count=25):
        # Interleaves nine buckets and repeats keys at a distance of many
        # blocks; values rotate through three units.
        rows = []
        for i in range(count):
            value = 20 + (i % 20)
            unit = ("K", "degC", "degF")[i % 3]
            if unit == "K":
                value = value + 273.15
            elif unit == "degF":
                value = value * 9 / 5 + 32
            hour, minute, second = i % 9, (i // 9) % 60, i % 60
            rows.append({"time": f"2026-11-01T{hour:02d}:{minute:02d}:{second:02d}Z",
                         "coordinates": {"station": f"s{i % 5}"},
                         "values": {"temperature": value},
                         "units": {"temperature": unit}})
        return rows

    def streamed_copy(self, rows, **kwargs):
        other = tempfile.TemporaryDirectory()
        self.addCleanup(other.cleanup)
        data = Dataset(other.name)
        data.create(schema(max_batch_rows=1000, max_bucket_rows=1000))
        data.append(rows, **kwargs)
        return data

    def test_stream_matches_one_shot_for_every_policy(self):
        rows = self.long_input()
        for policy in ("error", "update", "ignore"):
            with self.subTest(policy=policy):
                reference = self.streamed_copy(rows, duplicates=policy)
                streamed = tempfile.TemporaryDirectory()
                self.addCleanup(streamed.cleanup)
                data = Dataset(streamed.name)
                data.create(schema())
                data.append_stream(stream(rows), duplicates=policy)
                self.assertEqual(list(data.window(START, END)),
                                 list(reference.window(START, END)))
                self.assertEqual(data.stats(START, END), reference.stats(START, END))
                self.assertEqual(list(data.resample(START, END, 3600)),
                                 list(reference.resample(START, END, 3600)))

    def test_invalid_value_becomes_missing_and_reject_is_atomic(self):
        rows = [point(0, value=20), point(1, value="broken"),
                point(2, value=200, station="s2")]
        with self.assertRaises(ValueError):
            self.dataset.append_stream(stream(rows))
        self.assertEqual(self.dataset.store.head(), self.initial)
        self.dataset.append_stream(stream(rows), invalid="missing")
        found = {row["coordinates"]["station"]: row for row in self.dataset.window(START, END)}
        self.assertIsNone(found["s1"]["values"]["temperature"])
        self.assertEqual(found["s1"]["invalid"], ["temperature"])
        self.assertEqual(found["s2"]["invalid"], ["temperature"])

    def test_bad_unit_on_the_last_row_leaves_head_untouched(self):
        rows = [point(i, value=25, unit="K") for i in range(9)]
        rows.append(point(9, value=25, unit="m"))
        with self.assertRaises(ValueError):
            self.dataset.append_stream(stream(rows))
        self.assertEqual(self.dataset.store.head(), self.initial)
        # The recovery path is a later, independent batch.
        version = self.dataset.append_stream(stream([point(0, value=25, unit="degF")]))
        self.assertEqual(self.dataset.store.head(), version)

    def test_generator_error_mid_batch_aborts_and_cleans_staging(self):
        def exploding():
            for i in range(5):
                yield point(i, value=25)
            raise RuntimeError("source stream failed")

        with self.assertRaises(RuntimeError):
            self.dataset.append_stream(exploding())
        self.assertEqual(self.dataset.store.head(), self.initial)
        staging = self.root / "staging"
        self.assertEqual(list(staging.iterdir()), [])
        # Unreferenced chunks may exist but are never a successful version.
        for sha in (self.root / "chunks").iterdir():
            self.assertNotIn(sha.stem, self.dataset.snapshot()["buckets"].get("0", []))

    def test_staging_space_exhaustion_aborts_without_head(self):
        original = Staging.add_many
        calls = {"count": 0}

        def failing(self_staging, records):
            calls["count"] += 1
            if calls["count"] == 2:
                raise OSError(28, "No space left on device")
            return original(self_staging, records)

        Staging.add_many = failing
        try:
            with self.assertRaises(OSError):
                self.dataset.append_stream(stream(self.long_input()))
        finally:
            Staging.add_many = original
        self.assertEqual(self.dataset.store.head(), self.initial)
        self.assertEqual(list((self.root / "staging").iterdir()), [])

    def test_bucket_limit_still_applies_and_duplicate_errors_win(self):
        other = tempfile.TemporaryDirectory()
        self.addCleanup(other.cleanup)
        data = Dataset(other.name)
        initial = data.create(schema(max_bucket_rows=6))
        # Six distinct keys per bucket exceed the limit of six only when a
        # seventh arrives.
        with self.assertRaises(ValueError) as error:
            data.append_stream(stream([point(i, value=25) for i in range(7)]))
        self.assertIn("bucket memory limit", str(error.exception))
        self.assertEqual(data.store.head(), initial)
        self.assertEqual(len(list(data.window(START, END))), 0)
        # A duplicate is diagnosed even though the unique cardinality also
        # overflows, matching one-shot append's ordering of checks.
        with self.assertRaises(ValueError) as error:
            data.append_stream(stream([point(i, value=25) for i in range(7)] + [point(0, value=30)]))
        self.assertIn("duplicate", str(error.exception))

    def test_input_buffer_and_merge_residency_bounds(self):
        rows = self.long_input(60)
        prepared = self.dataset.prepare_append(stream(rows))
        try:
            self.assertEqual(prepared.consumed_rows, 60)
            self.assertEqual(prepared.staged_rows, 60)
            self.assertEqual(prepared.base, self.initial)
            # Preparation never publishes: HEAD is unchanged for all buckets.
            self.assertEqual(self.dataset.store.head(), self.initial)
            self.assertEqual(prepared.snapshot["revision"], 1)
            self.assertGreater(len(prepared.snapshot["buckets"]), 1)
            # Input normalization never holds more than max_batch_rows rows.
            self.assertLessEqual(prepared.input_peak, 3)
            # One old chunk, one staged fetch window and one output chunk are
            # resident while merging, so the tight bound is 3*chunk_rows and
            # the documented max_bucket_rows + 2*chunk_rows bound holds too.
            for peak in prepared.merge_peaks.values():
                self.assertLessEqual(peak, 6)
            prepared.publish()
        finally:
            prepared.discard()
        self.assertEqual(len(list(self.dataset.window(START, END))), 60)

    def test_merge_residency_when_old_bucket_has_many_chunks(self):
        other = tempfile.TemporaryDirectory()
        self.addCleanup(other.cleanup)
        data = Dataset(other.name)
        data.create(schema(chunk_rows=3, max_bucket_rows=100, max_batch_rows=64))
        # A full old bucket already split into many chunks, then a new batch
        # that touches the same bucket plus an untouched one.
        old = [point(i, station=f"old{i}", value=10) for i in range(60)]
        data.append(old)
        prepared = data.prepare_append(stream([
            point(i, station=f"new{i}", value=20) for i in range(12)]))
        try:
            self.assertEqual(prepared.bucket_merges, 1)
            # Old chunk + staged window + output chunk: 3 * chunk_rows.
            self.assertLessEqual(prepared.merge_peak, 9)
            self.assertLessEqual(prepared.merge_peak, 60 + 2 * 3)
            prepared.publish()
        finally:
            prepared.discard()
        self.assertEqual(len(list(data.window(START, END))), 72)

    def test_long_input_across_many_buckets_stays_bounded(self):
        rows = [{"time": f"2026-11-01T{hour:02d}:{i % 60:02d}:{i % 60:02d}Z",
                 "coordinates": {"station": f"s{i}"},
                 "values": {"temperature": 10}, "units": {"temperature": "degC"}}
                for i, hour in ((i, i % 9) for i in range(180))]
        prepared = self.dataset.prepare_append(stream(rows))
        try:
            self.assertLessEqual(prepared.input_peak, 3)
            self.assertEqual(prepared.bucket_merges, 9)
            self.assertTrue(all(peak <= 6 for peak in prepared.merge_peaks.values()))
            prepared.publish()
        finally:
            prepared.discard()
        self.assertEqual(len(list(self.dataset.window(START, END))), 180)

    def test_independent_model_agrees_on_repeats_and_units(self):
        rows = self.long_input(30)
        # Repeat an early key far later (many blocks away) with a different
        # unit, so duplicate semantics and unit normalization interact.
        rows.append(dict(point(0, station="s0", value=30, unit="degF"),
                         time="2026-11-01T00:00:00Z"))
        rows.append({"time": "2026-11-01T00:00:00Z", "coordinates": {"station": "s1"},
                     "values": {"temperature": 25}, "units": {"temperature": "degF"}})
        for policy in ("error", "update", "ignore"):
            with self.subTest(policy=policy):
                other = tempfile.TemporaryDirectory()
                self.addCleanup(other.cleanup)
                data = Dataset(other.name)
                initial = data.create(schema(max_bucket_rows=1000, max_batch_rows=1000))
                if policy == "error":
                    # Both the independent model and the streamed path must
                    # reject the batch, and HEAD must not move.
                    with self.assertRaises(ValueError):
                        reference_model(rows, policy)
                    with self.assertRaises(ValueError):
                        data.append_stream(stream(rows), duplicates=policy)
                    self.assertEqual(data.store.head(), initial)
                    continue
                model = reference_model(rows, policy)
                data.append_stream(stream(rows), duplicates=policy)
                expected = [{"time": iso(row["t"]), "coordinates": row["coordinates"],
                             "values": row["values"], "invalid": row["invalid"]}
                            for bucket in sorted(model, key=int) for row in model[bucket]]
                self.assertEqual(list(data.window(START, END)), expected)

    def test_streamed_chunks_are_byte_identical_to_one_shot(self):
        rows = self.long_input(36)
        # A key repeated in another bucket (different timestamp) is not a
        # duplicate; the same coordinate in the same bucket is.
        rows.append(dict(point(0, station="s0", value=21), time="2026-11-01T05:00:00Z"))
        for policy, duplicates in (("error", rows),
                                   ("update", rows + [from_degf(point(0, station="s0", value=20), 20)]),
                                   ("ignore", rows + [from_degf(point(0, station="s0", value=20), 20)])):
            with self.subTest(policy=policy):
                reference = self.streamed_copy(duplicates, duplicates=policy)
                other = tempfile.TemporaryDirectory()
                self.addCleanup(other.cleanup)
                data = Dataset(other.name)
                data.create(schema(max_batch_rows=3))
                data.append_stream(stream(duplicates), duplicates=policy)
                self.assertEqual(data.snapshot()["buckets"],
                                 reference.snapshot()["buckets"])

    def test_repeat_with_different_units_and_late_bad_value(self):
        base = {"time": "2026-11-01T00:30:00Z", "coordinates": {"station": "s0"},
                "values": {"temperature": 20}, "units": {"temperature": "degC"}}
        filler = [point(i, station=f"s{i}", value=20) for i in range(1, 8)]
        same_key_far_away = dict(base, values={"temperature": 293.15},
                                 units={"temperature": "K"})
        broken_later = dict(base, values={"temperature": "not-a-number"})

        def run(rows, policy):
            other = tempfile.TemporaryDirectory()
            self.addCleanup(other.cleanup)
            data = Dataset(other.name)
            data.create(schema(max_bucket_rows=100, max_batch_rows=1000))
            data.append_stream(stream(rows), duplicates=policy, invalid="missing")
            found = {row["coordinates"]["station"]: row
                     for row in data.window(START, END)}
            return found["s0"]

        # ``update`` keeps the last occurrence, so a trailing bad value makes
        # the stored measurement missing and flagged, exactly as one-shot.
        self.assertIsNone(run([base, *filler, same_key_far_away, broken_later],
                              "update")["values"]["temperature"])
        # ``ignore`` keeps the first (original) occurrence, despite the later
        # valid Kelvin representation.
        self.assertEqual(run([base, *filler, same_key_far_away], "ignore")
                         ["values"]["temperature"], 20)

    def test_merge_peak_accounts_for_old_chunks_too(self):
        self.dataset.append([point(0, value=10), point(1, value=11)])
        prepared = self.dataset.prepare_append(stream([point(2, value=20)]))
        try:
            for peak in prepared.merge_peaks.values():
                self.assertLessEqual(peak, 6)
            prepared.publish()
        finally:
            prepared.discard()
        self.assertEqual(len(list(self.dataset.window(START, END))), 3)

    def test_single_consumption_of_a_one_shot_generator(self):
        counting = Counting(stream(self.long_input(20)))
        self.dataset.append_stream(counting)
        self.assertEqual(counting.pulled, 20)
        self.assertEqual(len(list(self.dataset.window(START, END))), 20)

    def test_unchanged_buckets_share_objects_after_streaming(self):
        self.dataset.append([point(0, value=10, hour=0), point(1, value=11, hour=1)])
        before = self.dataset.snapshot()
        version = self.dataset.append_stream(stream([point(2, value=12, hour=1)]))
        after = self.dataset.snapshot(version)
        self.assertEqual(before["buckets"]["498192"], after["buckets"]["498192"])
        self.assertNotEqual(before["buckets"]["498193"], after["buckets"]["498193"])
        # Historic window/stats still read the old snapshot verbatim.
        old = list(self.dataset.window(START, END, version=self.dataset.snapshot()["parent"]))
        self.assertEqual(len(old), 2)

    def test_one_multi_bucket_batch_publishes_exactly_one_revision(self):
        rows = [point(i, station=f"s{i}", value=25, hour=i % 4) for i in range(12)]
        version = self.dataset.append_stream(stream(rows))
        snapshot = self.dataset.snapshot(version)
        # Four buckets were written, but the batch is one atomic revision, so
        # raising max_batch_rows or committing per bucket is not a workaround.
        self.assertEqual(len(snapshot["buckets"]), 4)
        self.assertEqual(snapshot["revision"], 1)
        self.assertEqual(snapshot["parent"], self.initial)
        self.assertEqual(self.dataset.store.head(), version)
        self.assertEqual(len(self.dataset.snapshot()["buckets"]), 4)

    def test_append_still_fails_when_logical_batch_exceeds_limit(self):
        with self.assertRaises(ValueError) as error:
            self.dataset.append([point(i, value=10) for i in range(4)])
        self.assertIn("batch memory limit", str(error.exception))
        self.assertEqual(self.dataset.store.head(), self.initial)

    def test_staging_root_directory_is_used_and_cleaned(self):
        scratch = self.root / "scratch"
        self.dataset.append_stream(stream([point(0, value=25)]), staging_root=scratch)
        self.assertEqual(list(scratch.iterdir()), [])


def _write_from_another_process(root):
    data = Dataset(root)
    version = data.append_stream(stream([point(i, station=f"other{i}", value=25) for i in range(4)]))
    return version


def _prepare_then_exit_hard(root, done):
    """Normalize and chunk a batch, then die without publishing."""
    data = Dataset(root)
    data.prepare_append(stream([point(i, station=f"ghost{i}", value=25) for i in range(5)]))
    done.set()          # parent may now inspect HEAD while we are still alive
    os._exit(0)         # skip interpreter cleanup, as an abrupt crash would


def _read_snapshot_head(root):
    data = Dataset(root)
    return data.store.head(), len(list(data.window(START, END)))


class ConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.dataset = Dataset(self.root)
        self.initial = self.dataset.create(schema(max_bucket_rows=100, max_batch_rows=100))
        self.addCleanup(self.tmp.cleanup)

    def test_prepared_batch_is_rejected_when_head_moved(self):
        base = self.dataset.store.head()
        prepared = self.dataset.prepare_append(stream([point(i, value=25) for i in range(5)]))
        try:
            with multiprocessing.get_context("spawn").Pool(1) as pool:
                other_version = pool.apply(_write_from_another_process, (str(self.root),))
            self.assertNotEqual(other_version, base)
            # This process still reads the snapshot it fixed at preparation.
            self.assertEqual(list(self.dataset.window(START, END, version=base)), [])
            with self.assertRaises(SnapshotConflict):
                prepared.publish()
        finally:
            prepared.discard()
        # The other writer's data is intact: no overwrite, no partial merge.
        self.assertEqual(self.dataset.store.head(), other_version)
        rows = list(self.dataset.window(START, END))
        self.assertEqual(len(rows), 4)
        self.assertTrue(all(row["coordinates"]["station"].startswith("other") for row in rows))
        self.assertIsNotNone(self.dataset.snapshot(other_version))

    def test_expected_revision_guard_uses_snapshot_conflict(self):
        with self.assertRaises(SnapshotConflict):
            self.dataset.prepare_append(stream([point(0, value=25)]), expected=self.initial + "0")
        self.assertEqual(self.dataset.store.head(), self.initial)

    def test_explicit_discard_never_publishes(self):
        prepared = self.dataset.prepare_append(stream([point(0, value=25)]))
        prepared.discard()
        self.assertIsNone(prepared.version)
        self.assertEqual(self.dataset.store.head(), self.initial)
        with self.assertRaises(ValueError):
            prepared.publish()

    def test_process_exit_before_publish_leaves_snapshot_readable(self):
        context = multiprocessing.get_context("spawn")
        done = context.Event()
        process = context.Process(target=_prepare_then_exit_hard,
                                  args=(str(self.root), done))
        process.start()
        try:
            self.assertTrue(done.wait(30), "child never finished preparation")
        finally:
            process.join(30)
        self.assertEqual(process.exitcode, 0)
        # The child wrote chunks and staged rows but never a snapshot; HEAD is
        # still the prepared base and remains readable from a fresh process.
        head, count = _read_snapshot_head(str(self.root))
        self.assertEqual(head, self.initial)
        self.assertEqual(count, 0)
        # A stale staging directory is inert: a new batch still publishes.
        version = self.dataset.append_stream(stream([point(0, value=25)]))
        self.assertEqual(self.dataset.store.head(), version)
        self.assertEqual(len(list(self.dataset.window(START, END))), 1)


if __name__ == "__main__":
    unittest.main()
