"""Affine unit registry: (physical dimension, scale, offset to base unit)."""
import copy
import math
from datetime import datetime, timezone


class UnitRegistry:
    def __init__(self):
        self.units = {
            "K": ("temperature", 1, 0), "degC": ("temperature", 1, 273.15),
            "degF": ("temperature", 5 / 9, 255.3722222222222),
            "m": ("length", 1, 0), "cm": ("length", 0.01, 0),
            "Pa": ("pressure", 1, 0), "kPa": ("pressure", 1000, 0),
            "1": ("dimensionless", 1, 0),
        }

    def register(self, name, dimension, scale, offset=0):
        if name in self.units or not math.isfinite(scale) or scale == 0 or not math.isfinite(offset):
            raise ValueError("invalid or duplicate unit")
        self.units[name] = dimension, scale, offset

    def frozen(self):
        """Copy the unit table so a prepared batch cannot observe later changes."""
        frozen = copy.copy(self)
        frozen.units = dict(self.units)
        return frozen

    def convert(self, value, source, target):
        if source not in self.units or target not in self.units:
            raise ValueError("unknown unit")
        a, b = self.units[source], self.units[target]
        if a[0] != b[0]:
            raise ValueError("incompatible physical dimensions")
        if value is None: return None
        if isinstance(value, bool): raise ValueError("boolean measurement")
        numeric = float(value)
        if not math.isfinite(numeric): raise ValueError("non-finite measurement")
        result = (numeric * a[1] + a[2] - b[2]) / b[1]
        if not math.isfinite(result): raise ValueError("measurement overflow")
        return result


def epoch(value):
    if not isinstance(value, str): raise ValueError("time must be an ISO8601 string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None: raise ValueError("time requires an explicit timezone")
    utc = parsed.astimezone(timezone.utc)
    delta = utc - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds


def iso(micros):
    from datetime import timedelta
    return (datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(microseconds=micros)).isoformat().replace("+00:00", "Z")
