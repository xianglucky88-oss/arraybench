import argparse
import csv
import json
from .dataset import Dataset


def input_rows(path, schema):
    if path.endswith(".csv"):
        with open(path, encoding="utf-8", newline="") as stream:
            for row in csv.DictReader(stream):
                yield {"time": row["time"], "coordinates": {d: row[d] for d in schema["dimensions"]},
                       "values": {v: row[v] or None for v in schema["variables"]},
                       "units": {v: row[v + "_unit"] for v in schema["variables"] if row.get(v + "_unit")}}
    else:
        with open(path, encoding="utf-8") as stream:
            if path.endswith(".jsonl"):
                for line in stream:
                    if line.strip(): yield json.loads(line)
            else: yield from json.load(stream)


def main():
    parser = argparse.ArgumentParser(description="Snapshot multidimensional measurements")
    parser.add_argument("--root", required=True)
    sub = parser.add_subparsers(dest="command", required=True)
    init = sub.add_parser("init")
    init.add_argument("schema")
    ingest = sub.add_parser("append")
    ingest.add_argument("input")
    ingest.add_argument("--duplicates", choices=["error", "update", "ignore"], default="error")
    ingest.add_argument("--invalid", choices=["reject", "missing"], default="reject")
    sub.add_parser("inspect")
    restore = sub.add_parser("rollback")
    restore.add_argument("version")
    for name in ("window", "stats", "resample", "segment"):
        item = sub.add_parser(name)
        item.add_argument("start")
        item.add_argument("end")
        item.add_argument("--version")
        if name == "resample":
            item.add_argument("--seconds", type=int, required=True)
        if name == "segment":
            item.add_argument("--variable", required=True)
            item.add_argument("--penalty", type=float, required=True)
            item.add_argument("--min-points", type=int, required=True)
            item.add_argument("--min-span-seconds", type=float, required=True)
            item.add_argument("--max-gap-seconds", type=float, required=True)
            item.add_argument("--coordinate", action="append", default=[],
                             help="dimension=value filter, repeatable")
            item.add_argument("--max-resident-records", type=int)
            item.add_argument("--max-staging-bytes", type=int)
            item.add_argument("--max-group-observations", type=int)
    args = parser.parse_args()
    try:
        dataset = Dataset(args.root)
        if args.command == "init":
            with open(args.schema, encoding="utf-8") as stream: result = {"version": dataset.create(json.load(stream))}
        elif args.command == "append":
            result = {"version": dataset.append(input_rows(args.input, dataset.snapshot()["schema"]), duplicates=args.duplicates, invalid=args.invalid)}
        elif args.command == "inspect": result = dataset.snapshot()
        elif args.command == "rollback": result = {"version": dataset.rollback(args.version)}
        elif args.command == "stats": result = dataset.stats(args.start, args.end, version=args.version)
        elif args.command == "window": result = list(dataset.window(args.start, args.end, version=args.version))
        elif args.command == "segment":
            schema = dataset.snapshot()["schema"]
            filters = {}
            for pair in args.coordinate:
                dim, raw = pair.split("=", 1)
                filters[dim] = float(raw) if schema["dimensions"].get(dim) == "float" else raw
            limits = {name: value for name, value in (
                ("max_resident_records", args.max_resident_records),
                ("max_staging_bytes", args.max_staging_bytes),
                ("max_group_observations", args.max_group_observations)) if value is not None}
            result = dataset.segment(
                args.start, args.end, args.variable, penalty=args.penalty,
                min_points=args.min_points, min_span_seconds=args.min_span_seconds,
                max_gap_seconds=args.max_gap_seconds,
                coordinates=filters or None, version=args.version,
                limits=limits or None)
        else: result = list(dataset.resample(args.start, args.end, args.seconds, version=args.version))
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except (ValueError, KeyError, OSError) as exc:
        parser.exit(2, f"error: {exc}\n")


if __name__ == "__main__": main()
