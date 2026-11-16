import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

project = Path(__file__).resolve().parents[1]
env = dict(os.environ, PYTHONPATH=str(project / "src"))
START, END = "2026-11-01T00:00:00Z", "2026-11-02T00:00:00Z"


def cli(*args):
    completed = subprocess.run([sys.executable, "-m", "arraybench.cli", *map(str, args)],
                               env=env, check=True, capture_output=True, text=True)
    return json.loads(completed.stdout)


def row(seconds, station="east"):
    return {"time": f"2026-11-01T00:00:{seconds:02d}Z", "coordinates": {"station": station},
            "values": {"temperature": 20}}


with tempfile.TemporaryDirectory() as directory:
    work = Path(directory)
    schema = work / "schema.json"
    schema.write_text(json.dumps({"dimensions": {"station": "str"},
                                  "variables": {"temperature": {"unit": "degC", "min": -100, "max": 100}}}))
    left, right = work / "left", work / "right"
    cli("--root", left, "init", schema)
    cli("--root", right, "init", schema)
    left_rows, right_rows = work / "left.json", work / "right.json"
    left_rows.write_text(json.dumps([row(4), row(6)]))
    right_rows.write_text(json.dumps([row(1), row(5)]))
    left_version = cli("--root", left, "append", left_rows)["version"]
    right_version = cli("--root", right, "append", right_rows)["version"]
    plan = work / "align-plan.json"
    plan.write_text(json.dumps({
        "left": {"root": str(left), "version": left_version},
        "right": {"root": str(right), "version": right_version},
        "window": {"start": START, "end": END},
        "dimensions": {"station": "station"},
        "tolerance_micros": 3_000_000,
        "strategy": "memory",
        "limits": {"max_resident_rows": 1000, "max_temp_bytes": 1 << 20, "max_group_cells": 10000}}))
    result = cli("align", plan)
    pairs = [(p["left"]["time"], p["right"]["time"], p["dt_micros"]) for p in result["pairs"]]
    expected = [("2026-11-01T00:00:04Z", "2026-11-01T00:00:01Z", -3_000_000),
                ("2026-11-01T00:00:06Z", "2026-11-01T00:00:05Z", -1_000_000)]
    if pairs != expected:
        raise RuntimeError(f"greedy nearest would drop a pair; exact alignment must keep both: {pairs}")
    # HEAD advances on both sides after the plan pinned the snapshots:
    # left 5s/right 5s and left 6s/right 6s would pair if new rows leaked in.
    left_extra, right_extra = work / "left-extra.json", work / "right-extra.json"
    left_extra.write_text(json.dumps([row(5)]))
    right_extra.write_text(json.dumps([row(6)]))
    cli("--root", left, "append", left_extra)
    cli("--root", right, "append", right_extra)
    if cli("align", plan)["pairs"] != result["pairs"]:
        raise RuntimeError("pinned snapshots must ignore later HEAD advancement")
    print(json.dumps({"ok": True, "pairs": 2, "greedy_pairs": 1, "pinned_snapshot_stable": True}))
