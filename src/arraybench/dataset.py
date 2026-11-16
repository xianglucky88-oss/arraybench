import json
import math
from collections import defaultdict
from .storage import ChunkStore
from .units import UnitRegistry, epoch, iso


class Dataset:
    """Sparse arrays indexed by UTC time plus typed spatial coordinates.

    One time bucket is loaded at a time. Configured batch/bucket limits bound
    memory; immutable chunks provide snapshots and partial copy-on-write updates.
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

    def _normalize(self, row, schema, invalid):
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
            self.units.convert(None, unit, spec["unit"])
            try:
                value = self.units.convert(row["values"][variable], unit, spec["unit"])
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
