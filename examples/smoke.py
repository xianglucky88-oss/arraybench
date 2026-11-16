import json
import math
import os
import subprocess
import sys
import tempfile
from pathlib import Path

project = Path(__file__).resolve().parents[1]
env = dict(os.environ, PYTHONPATH=str(project / "src"))
start, end = "2026-11-01T00:00:00Z", "2026-11-02T00:00:00Z"


def cli(root, *args):
    completed = subprocess.run([sys.executable, "-m", "arraybench.cli", "--root", str(root), *map(str, args)],
                               env=env, check=True, capture_output=True, text=True)
    return json.loads(completed.stdout)


with tempfile.TemporaryDirectory() as directory:
    root = Path(directory) / "dataset"
    initial = cli(root, "init", project / "examples" / "schema.json")["version"]
    v1 = cli(root, "append", project / "examples" / "measurements.csv")["version"]
    rows = cli(root, "window", start, end)
    if len(rows) != 3 or not math.isclose(rows[0]["values"]["temperature"], 20): raise RuntimeError("normalization failed")
    stats = cli(root, "stats", start, end)
    if stats["temperature"]["mean"] != 21 or stats["temperature"]["missing"] != 1: raise RuntimeError("statistics failed")
    resampled = cli(root, "resample", start, end, "--seconds", "3600")
    if len(resampled) != 2: raise RuntimeError("spatial dimensions lost during resampling")
    streamed = cli(root, "append-stream", project / "examples" / "measurements.csv",
                   "--duplicates", "update", "--invalid", "missing")["version"]
    if len(cli(root, "window", start, end)) != 3: raise RuntimeError("streamed append changed cardinality")
    cli(root, "rollback", initial)
    if cli(root, "window", start, end): raise RuntimeError("rollback did not clear rows")
    if len(cli(root, "window", start, end, "--version", v1)) != 3: raise RuntimeError("historical snapshot mutated")
    if len(cli(root, "window", start, end, "--version", streamed)) != 3: raise RuntimeError("streamed snapshot mutated")
    print(json.dumps({"ok": True, "measurements": 3, "temperature_mean": 21, "resampled_series": 2, "historical_read": True, "cli_processes": 10}))
