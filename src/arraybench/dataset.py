import json
import math
import shutil
import tempfile
from collections import defaultdict
from pathlib import Path

from .storage import ChunkStore, SnapshotConflict
from .streaming import ResidencyProbe, Staging
from .units import UnitRegistry, epoch, iso


class PreparedAppend:
    """A fully normalized and chunked batch that has not touched HEAD yet.

    Preparing consumes the source generator exactly once, spills rows to
    staging and writes immutable chunks.  Publishing re-checks that HEAD still
    points at the snapshot fixed during preparation; any concurrent writer
    makes the whole batch fail without merging with the newer state.
    """

    def __init__(self, store, base, snapshot, staging, consumed, bucket_merges,
                 input_peak, merge_peaks):
        self.store = store
        self.base = base
        self.snapshot = snapshot
        self.consumed_rows = consumed
        self.staged_rows = staging.row_count
        self.bucket_merges = bucket_merges
        self.input_peak = input_peak
        self.merge_peaks = merge_peaks
        self.merge_peak = max(merge_peaks.values(), default=0)
        self.version = None
        self._staging = staging
        self._done = False

    @property
    def staging_directory(self):
        """Temporary index location; removed by ``publish``/``discard``."""
        return self._staging.directory

    def publish(self):
        if self._done:
            raise ValueError("prepared batch is already finished")
        self._done = True
        try:
            # Writing the snapshot object is not a commit: HEAD still names
            # ``base`` until publish_object succeeds under the write lock.
            sha = self.store.put("snapshots", self.snapshot)
            self.version = self.store.publish_object(sha, self.base)
            return self.version
        finally:
            self.close()

    def discard(self):
        """Remove this batch's staging; never touches HEAD.

        Chunks already written stay in the content-addressed store because they
        may be shared with other revisions or in-flight batches.  Unreferenced
        chunks are inert: no snapshot names them, so they are not a version.
        """
        if not self._done:
            self._done = True
            self.close()

    def close(self):
        self._staging.close()
        shutil.rmtree(self._staging.directory, ignore_errors=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        # A context manager guards cleanup only; publishing is always explicit
        # so an interrupted ``with`` body cannot accidentally commit.
        self.discard()
        return False


class Dataset:
    """Sparse arrays indexed by UTC time plus typed spatial coordinates.

    ``append`` loads one bounded logical batch at a time.  ``prepare_append``
    streams an arbitrarily long batch through a disk-backed index, then merges
    one bucket at a time.  Immutable chunks provide snapshots and copy-on-write
    updates; unchanged buckets keep their original chunk objects.
    """
    def __init__(self, root, registry=None):
        self.store = ChunkStore(root)
        self.units = registry or UnitRegistry()

    def create(self, schema):
        schema = json.loads(json.dumps(schema))
        dimensions, variables = schema.get("dimensions"), schema.get("variables")
        if not isinstance(dimensions, dict) or not dimensions or any(t not in ("str", "float") for t in dimensions.values()):
            raise ValueError("declare typed dimensions")
        if not isinstance(variables, dict) or not variables:
            raise ValueError("declare variables")
        if any(not isinstance(name, str) or not name for name in [*dimensions, *variables]):
            raise ValueError("dimension and variable names must be strings")
        for spec in variables.values():
            if not isinstance(spec, dict) or spec.get("unit") not in self.units.units:
                raise ValueError("declare known output units")
            for bound in ("min", "max"):
                if bound in spec and (isinstance(spec[bound], bool) or not isinstance(spec[bound], (int, float)) or not math.isfinite(spec[bound])):
                    raise ValueError("measurement bounds must be finite numbers")
            if "min" in spec and "max" in spec and spec["min"] > spec["max"]:
                raise ValueError("inverted measurement bounds")
        for key, default in (("bucket_seconds", 3600), ("chunk_rows", 256), ("max_bucket_rows", 10000), ("max_batch_rows", 10000)):
            schema.setdefault(key, default)
            if type(schema[key]) is not int or schema[key] <= 0:
                raise ValueError("storage limits must be positive integers")
        if schema["chunk_rows"] > schema["max_bucket_rows"]:
            raise ValueError("chunk row limit exceeds bucket row limit")
        with self.store.lock():
            if self.store.head() is not None: raise ValueError("dataset already exists")
            return self.store.commit({"format": 1, "revision": 0, "parent": None, "schema": schema, "buckets": {}})

    def snapshot(self, version=None):
        selected = version or self.store.head()
        if selected is None: raise ValueError("dataset is not initialized")
        return self.store.get("snapshots", selected)

    @staticmethod
    def _key(row):
        return row["t"], json.dumps(row["coordinates"], sort_keys=True, separators=(",", ":"))

    def _normalize(self, row, schema, invalid, registry=None):
        units = registry or self.units
        if set(row.get("coordinates", {})) != set(schema["dimensions"]):
            raise ValueError("coordinates do not match declared dimensions")
        if set(row.get("values", {})) != set(schema["variables"]):
            raise ValueError("measurements do not match declared variables")
        supplied_units = row.get("units", {})
        if set(supplied_units) - set(schema["variables"]): raise ValueError("undeclared variable unit")
        coordinates = {}
        for dim, kind in schema["dimensions"].items():
            value = row["coordinates"][dim]
            if kind == "str":
                if not isinstance(value, str) or not value.strip(): raise ValueError("invalid string coordinate")
                value = value.strip()
            else:
                if isinstance(value, bool): raise ValueError("boolean coordinate")
                try: value = float(value)
                except (ValueError, TypeError) as exc: raise ValueError("invalid numeric coordinate") from exc
                if not math.isfinite(value): raise ValueError("non-finite coordinate")
                if value == 0: value = 0.0
            coordinates[dim] = value
        values, invalid_variables = {}, []
        for variable, spec in schema["variables"].items():
            unit = supplied_units.get(variable, spec["unit"])
            # Dimensional errors are never converted to missing data.
            units.convert(None, unit, spec["unit"])
            try:
                value = units.convert(row["values"][variable], unit, spec["unit"])
                if value is not None and (("min" in spec and value < spec["min"]) or ("max" in spec and value > spec["max"])):
                    raise ValueError("measurement outside valid range")
            except (ValueError, TypeError):
                if invalid == "reject": raise
                value = None
                invalid_variables.append(variable)
            values[variable] = value
        return {"t": epoch(row["time"]), "coordinates": coordinates, "values": values, "invalid": invalid_variables}

    def append(self, rows, *, duplicates="error", invalid="reject", expected=None):
        if duplicates not in ("error", "update", "ignore") or invalid not in ("reject", "missing"):
            raise ValueError("invalid ingestion policy")
        with self.store.lock():
            parent = self.store.head()
            previous = self.snapshot(parent)
            if expected is not None and expected != parent: raise ValueError("snapshot revision conflict")
            schema = previous["schema"]
            pending = defaultdict(list)
            count = 0
            for row in rows:
                count += 1
                if count > schema["max_batch_rows"]: raise ValueError("batch memory limit exceeded")
                normalized = self._normalize(row, schema, invalid)
                bucket = str(normalized["t"] // (schema["bucket_seconds"] * 1000000))
                pending[bucket].append(normalized)
            updated = dict(previous["buckets"])
            for bucket, incoming in pending.items():
                existing = {}
                for chunk in updated.get(bucket, []):
                    for row in self.store.get("chunks", chunk): existing[self._key(row)] = row
                for row in incoming:
                    key = self._key(row)
                    if key in existing:
                        if duplicates == "error": raise ValueError("duplicate coordinate and timestamp")
                        if duplicates == "ignore": continue
                    existing[key] = row
                if len(existing) > schema["max_bucket_rows"]: raise ValueError("bucket memory limit exceeded")
                ordered = sorted(existing.values(), key=self._key)
                size = schema["chunk_rows"]
                updated[bucket] = [self.store.put("chunks", ordered[i:i + size]) for i in range(0, len(ordered), size)]
            return self.store.commit({**previous, "revision": previous["revision"] + 1, "parent": parent, "buckets": updated})

    # ------------------------------------------------------------------
    # Streaming append protocol
    #
    # ``append`` above still enforces ``max_batch_rows`` by failing the whole
    # call, which is the historic contract.  ``prepare_append`` accepts a
    # logical batch of unbounded length: it consumes the iterable once,
    # spills normalized rows to disk, and only then merges bucket by bucket.
    # ``max_batch_rows`` now bounds the input buffer that is resident at any
    # instant rather than the logical batch size.
    # ------------------------------------------------------------------

    @staticmethod
    def _key_text(coordinates):
        # Must stay byte-identical to ``_key`` so staged rows dedupe against
        # rows written by older versions and SQLite's BINARY order matches.
        return json.dumps(coordinates, sort_keys=True, separators=(",", ":"))

    def _iter_old_rows(self, chunk_shas, probe):
        """Yield old rows in key order, holding at most one chunk at a time."""
        for sha in chunk_shas:
            chunk = self.store.get("chunks", sha)
            probe.hold(len(chunk), "old")
            try:
                for row in chunk:
                    yield row
            finally:
                probe.release(len(chunk), "old")

    @staticmethod
    def _pair(record):
        """Project a staged ``(t, coord, ...)`` row onto its merge key."""
        return record[0], record[1]

    def _validate_error_mode(self, old_shas, staging, bucket, schema, probe):
        """Reject duplicates across old chunks and the whole staged bucket.

        Only ``(t, coord, count)`` groups are compared, so a key repeated
        through a long stream never has more than one record resident.  The
        bucket row limit is checked only after duplicates, matching the
        one-shot append where duplicate detection runs before the size check.
        """
        old_iter = self._iter_old_rows(old_shas, probe)
        group_iter = staging.key_counts(bucket, probe)
        old_row, group = next(old_iter, None), next(group_iter, None)
        total = 0
        try:
            while old_row is not None or group is not None:
                old_key = self._key(old_row) if old_row is not None else None
                group_key = self._pair(group) if group is not None else None
                if group_key is not None and group[2] > 1:
                    raise ValueError("duplicate coordinate and timestamp")
                if old_key is not None and group_key is not None and old_key == group_key:
                    raise ValueError("duplicate coordinate and timestamp")
                if group_key is not None and (old_key is None or group_key < old_key):
                    total += 1
                    group = next(group_iter, None)
                else:
                    total += 1
                    old_row = next(old_iter, None)
        finally:
            old_iter.close()
            group_iter.close()
        if total > schema["max_bucket_rows"]:
            raise ValueError("bucket memory limit exceeded")
        return total

    def _merge_bucket(self, previous, bucket, staging, schema, duplicates, probe):
        """Merge one old bucket with staged rows into fresh chunks.

        Old rows stream chunk by chunk and staged winners stream through a
        bounded fetch window; one output chunk is buffered before hashing.
        Resident records therefore stay below ``3 * chunk_rows``.
        """
        old_shas = previous["buckets"].get(bucket, [])
        if duplicates == "error":
            # Duplicate validation must precede any bucket-size error, and it
            # must see the entire bucket before chunks are emitted.
            self._validate_error_mode(old_shas, staging, bucket, schema, probe)
        size = schema["chunk_rows"]
        written, buffer, count = [], [], 0

        def flush():
            if buffer:
                written.append(self.store.put("chunks", list(buffer)))
                probe.release(len(buffer), "out")
                buffer.clear()

        def emit(row):
            nonlocal count
            count += 1
            if count > schema["max_bucket_rows"]:
                raise ValueError("bucket memory limit exceeded")
            buffer.append(row)
            probe.hold(1, "out")
            if len(buffer) >= size:
                flush()

        old_iter = self._iter_old_rows(old_shas, probe)
        winner_iter = staging.winners(bucket, newest=(duplicates == "update"), probe=probe)
        completed = False
        try:
            old_row, winner = next(old_iter, None), next(winner_iter, None)
            while old_row is not None or winner is not None:
                old_key = self._key(old_row) if old_row is not None else None
                new_key = self._pair(winner) if winner is not None else None
                if old_key is not None and new_key is not None and old_key == new_key:
                    # ``update`` takes the batch winner, ``ignore`` keeps old,
                    # and ``error`` already validated that no collision exists.
                    emit(json.loads(winner[2]) if duplicates == "update" else old_row)
                    old_row, winner = next(old_iter, None), next(winner_iter, None)
                elif new_key is not None and (old_key is None or new_key < old_key):
                    emit(json.loads(winner[2]))
                    winner = next(winner_iter, None)
                else:
                    emit(old_row)
                    old_row = next(old_iter, None)
            flush()
            completed = True
        finally:
            old_iter.close()
            winner_iter.close()
            if not completed:
                # A failed merge must not report a partial bucket as its result.
                probe.release(len(buffer), "out")
                buffer.clear()
        return written

    def prepare_append(self, rows, *, duplicates="error", invalid="reject", expected=None,
                       staging_root=None):
        """Normalize one logical batch without holding the global write lock.

        The source iterable is consumed exactly once.  Up to ``max_batch_rows``
        normalized records are resident before being spilled; at most one
        fetch window plus one output chunk (bounded by ``chunk_rows``) is
        resident while a bucket is merged.  The returned object publishes
        atomically or raises :class:`SnapshotConflict` if HEAD moved.
        """
        if duplicates not in ("error", "update", "ignore") or invalid not in ("reject", "missing"):
            raise ValueError("invalid ingestion policy")
        base = self.store.head()
        if base is None:
            raise ValueError("dataset is not initialized")
        previous = self.snapshot(base)
        if expected is not None and expected != base:
            raise SnapshotConflict("snapshot revision conflict")
        # Fix schema and unit definitions for the whole preparation phase.
        schema = json.loads(json.dumps(previous["schema"]))
        registry = self.units.frozen()
        probe = ResidencyProbe()
        directory = Path(staging_root) if staging_root is not None else self.store.root / "staging"
        directory.mkdir(parents=True, exist_ok=True)
        staging_dir = Path(tempfile.mkdtemp(prefix="batch-", dir=directory))
        staging = Staging(staging_dir, schema["chunk_rows"])
        width = schema["bucket_seconds"] * 1000000
        limit = schema["max_batch_rows"]
        buffer, seq, consumed = [], 0, 0
        try:
            try:
                for row in rows:
                    consumed += 1
                    normalized = self._normalize(row, schema, invalid, registry)
                    bucket = str(normalized["t"] // width)
                    buffer.append((bucket, normalized["t"], self._key_text(normalized["coordinates"]),
                                   seq, json.dumps(normalized, ensure_ascii=False, sort_keys=True,
                                                   separators=(",", ":"), allow_nan=False)))
                    probe.hold(1, "input")
                    seq += 1
                    if len(buffer) >= limit:
                        staging.add_many(buffer)
                        probe.release(len(buffer), "input")
                        buffer.clear()
                if buffer:
                    staging.add_many(buffer)
                    probe.release(len(buffer), "input")
                    buffer.clear()
            except BaseException:
                probe.release(len(buffer), "input")
                buffer.clear()
                raise
            staging.finish()
            input_peak = probe.peak
            updated = dict(previous["buckets"])
            merge_peaks = {}
            for bucket in staging.buckets():
                probe.reset_peak()
                updated[bucket] = self._merge_bucket(previous, bucket, staging, schema, duplicates, probe)
                merge_peaks[bucket] = probe.peak
                probe.reset_peak()
            if probe.live != 0:
                raise AssertionError("residency accounting leaked records")
            snapshot = {**previous, "revision": previous["revision"] + 1,
                        "parent": base, "buckets": updated}
            return PreparedAppend(self.store, base, snapshot, staging, consumed,
                                  len(merge_peaks), input_peak, merge_peaks)
        except BaseException:
            staging.close()
            shutil.rmtree(staging_dir, ignore_errors=True)
            raise

    def append_stream(self, rows, **kwargs):
        """Prepare and atomically publish one streamed logical batch."""
        return self.prepare_append(rows, **kwargs).publish()

    def window(self, start, end, *, coordinates=None, version=None):
        start, end = epoch(start), epoch(end)
        if end <= start: raise ValueError("window must be nonempty")
        snapshot = self.snapshot(version)
        schema = snapshot["schema"]
        coordinates = coordinates or {}
        if set(coordinates) - set(schema["dimensions"]): raise ValueError("unknown dimension filter")
        width = schema["bucket_seconds"] * 1000000
        for bucket, chunks in sorted(snapshot["buckets"].items(), key=lambda item: int(item[0])):
            if int(bucket) * width >= end or (int(bucket) + 1) * width <= start: continue
            for chunk in chunks:
                for row in self.store.get("chunks", chunk):
                    if start <= row["t"] < end and all(row["coordinates"][k] == v for k, v in coordinates.items()):
                        yield {"time": iso(row["t"]), "coordinates": row["coordinates"], "values": row["values"], "invalid": row["invalid"]}

    def resample(self, start, end, seconds, *, coordinates=None, version=None):
        if type(seconds) is not int or seconds < 1: raise ValueError("resampling interval must be positive")
        version = version or self.store.head()
        # Flush aggregates per time bin to keep memory bounded by coordinate count
        # within that bin. The configured bucket row limit also bounds each bin.
        schema = self.snapshot(version)["schema"]
        limit = schema["max_bucket_rows"]
        current_bin, groups = None, {}
        def flush(bin_number):
            for (_, coord), measures in sorted(groups.items()):
                yield {"time": iso(bin_number * seconds * 1000000), "coordinates": json.loads(coord),
                       "values": {name: total / n if n else None for name, (total, n) in measures.items()},
                       "counts": {name: n for name, (_, n) in measures.items()}}
        for row in self.window(start, end, coordinates=coordinates, version=version):
            bin_number = epoch(row["time"]) // (seconds * 1000000)
            if current_bin is not None and current_bin != bin_number:
                yield from flush(current_bin)
                groups.clear()
            current_bin = bin_number
            key = bin_number, json.dumps(row["coordinates"], sort_keys=True)
            if key not in groups:
                if len(groups) >= limit: raise ValueError("resampling group memory limit exceeded")
                groups[key] = {name: [0.0, 0] for name in schema["variables"]}
            for name, value in row["values"].items():
                if value is not None:
                    groups[key][name][0] += value
                    groups[key][name][1] += 1
        if current_bin is not None: yield from flush(current_bin)

    def stats(self, start, end, **kwargs):
        accumulators = {}
        for row in self.window(start, end, **kwargs):
            for name, value in row["values"].items():
                a = accumulators.setdefault(name, {"count": 0, "missing": 0, "mean": 0.0, "m2": 0.0, "min": None, "max": None})
                if value is None:
                    a["missing"] += 1
                    continue
                a["count"] += 1
                delta = value - a["mean"]
                a["mean"] += delta / a["count"]
                a["m2"] += delta * (value - a["mean"])
                a["min"] = value if a["min"] is None else min(value, a["min"])
                a["max"] = value if a["max"] is None else max(value, a["max"])
        return {name: {"count": a["count"], "missing": a["missing"], "mean": a["mean"] if a["count"] else None,
                       "variance": a["m2"] / a["count"] if a["count"] else None, "min": a["min"], "max": a["max"]}
                for name, a in accumulators.items()}

    def rollback(self, target, expected=None):
        with self.store.lock():
            parent = self.store.head()
            if expected is not None and expected != parent: raise ValueError("snapshot revision conflict")
            historical, current = self.snapshot(target), self.snapshot(parent)
            return self.store.commit({**historical, "revision": current["revision"] + 1, "parent": parent, "restored_from": target})
