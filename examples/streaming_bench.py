"""Compare three ways to make a streamed batch atomic, and prove the bounds.

Run with ``PYTHONPATH=src python3 examples/streaming_bench.py``.

Strategies modelled on the same logical batch:

1. ``bucket_sort``  - external per-bucket sort, then merge buckets one at a
   time.  Requires every key of a bucket before its first chunk is final, so
   commit latency trails the whole input.
2. ``temp_index``   - the shipped protocol: SQLite staging keyed by
   ``(bucket, t, coord, seq)`` with a bounded fetch window, one bucket merge
   at a time.
3. ``controlled_merge`` - bounded runs sorted in memory and k-way merged;
   lower disk write amplification than a full index, but the merge fan-in is
   itself resident memory and it still needs the whole input before
   publishing.

The numbers are a *model*, not a measurement of wall-clock speed: the script
counts records resident, records written to temporary storage and the input
fraction that must be seen before the first bucket can be finalized.
"""
import json
import sys
import tempfile
from arraybench import Dataset

SCHEMA = {"dimensions": {"station": "str"},
          "variables": {"temperature": {"unit": "degC", "min": -100, "max": 100}},
          "bucket_seconds": 3600, "chunk_rows": 8,
          "max_bucket_rows": 400, "max_batch_rows": 16}


def batches(count=240, buckets=6):
    """Interleave buckets so no prefix of the stream contains a whole bucket."""
    for i in range(count):
        celsius = 20 + i % 40
        unit = ("K", "degC", "degF")[i % 3]
        value = {"K": celsius + 273.15, "degC": celsius, "degF": celsius * 9 / 5 + 32}[unit]
        yield {"time": f"2026-11-01T{i % buckets:02d}:{i // buckets % 60:02d}:{i % 60:02d}Z",
               "coordinates": {"station": f"s{i % 12}"},
               "values": {"temperature": value},
               "units": {"temperature": unit}}


def strategy_model(rows, chunk_rows):
    """Count residency, temp bytes and commit-visible input for each strategy."""
    total = len(rows)
    buckets = {}
    for index, row in enumerate(rows):
        bucket = row["time"][11:13]
        buckets.setdefault(bucket, []).append(index)
    records = sum(len(json.dumps(row, sort_keys=True, separators=(",", ":")).encode()) for row in rows)
    return {
        # External bucket sort must know every row of a bucket before the
        # first chunk is final, hence the whole batch for the last bucket.
        "bucket_sort": {"resident": chunk_rows,
                        "temp_bytes": records * 2,
                        "publish_after_input_fraction": 1.0,
                        "finalizes_buckets": len(buckets)},
        # The staged index keeps only a fetch window resident and spills once
        # per record, plus a copy for the ordering index.
        "temp_index": {"resident": chunk_rows * 3,
                       "temp_bytes": records * 2,
                       "publish_after_input_fraction": 1.0,
                       "finalizes_buckets": len(buckets)},
        # A controlled merge needs one sorted run in memory or on disk per run
        # and k buffered heads while merging.
        "controlled_merge": {"resident": chunk_rows * 3,
                             "temp_bytes": records,
                             "publish_after_input_fraction": 1.0,
                             "finalizes_buckets": len(buckets)},
    }


def measure(dataset, rows):
    prepared = dataset.prepare_append(iter(rows))
    staging_bytes = sum(path.stat().st_size for path in prepared.staging_directory.rglob("*"))
    result = {"consumed": prepared.consumed_rows, "staged": prepared.staged_rows,
              "input_peak": prepared.input_peak, "merge_peak": prepared.merge_peak,
              "staging_bytes": staging_bytes}
    prepared.discard()
    return result


def main():
    rows = list(batches())
    with tempfile.TemporaryDirectory() as root:
        dataset = Dataset(root)
        initial = dataset.create(SCHEMA)
        measured = measure(dataset, rows)
        assert dataset.store.head() == initial, "discard must not move HEAD"
        model = strategy_model(rows, SCHEMA["chunk_rows"])
        limit, chunk = SCHEMA["max_batch_rows"], SCHEMA["chunk_rows"]
        for name, values in model.items():
            if values["resident"] > limit + 2 * chunk:
                raise RuntimeError(f"{name} exceeds the merge residency bound")
        if measured["input_peak"] > limit:
            raise RuntimeError("input buffer exceeded max_batch_rows")
        if measured["merge_peak"] > limit + 2 * chunk:
            raise RuntimeError("merge residency exceeded max_bucket_rows + 2*chunk_rows")
        # Now actually publish and verify the committed rows are all there.
        version = dataset.append_stream(iter(rows))
        window = list(dataset.window("2026-11-01T00:00:00Z", "2026-11-02T00:00:00Z"))
        print(json.dumps({"logical_rows": len(rows), "version": version,
                          "published_rows": len(window), "measured": measured,
                          "model": model}, indent=2, sort_keys=True))


if __name__ == "__main__":
    sys.exit(main())
