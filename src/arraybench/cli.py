import argparse
import csv
import json
from .align import AlignmentLimitError, align
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
    parser.add_argument("--root")
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
    aligner = sub.add_parser("align")
    aligner.add_argument("plan")
    for name in ("window", "stats", "resample"):
        item = sub.add_parser(name)
        item.add_argument("start")
        item.add_argument("end")
        item.add_argument("--version")
        if name == "resample": item.add_argument("--seconds", type=int, required=True)
    args = parser.parse_args()
    try:
        if args.command == "align":
            with open(args.plan, encoding="utf-8") as stream: result = align(json.load(stream))
        else:
            if not args.root: parser.exit(2, "error: --root is required\n")
            dataset = Dataset(args.root)
            if args.command == "init":
                with open(args.schema, encoding="utf-8") as stream: result = {"version": dataset.create(json.load(stream))}
            elif args.command == "append":
                result = {"version": dataset.append(input_rows(args.input, dataset.snapshot()["schema"]), duplicates=args.duplicates, invalid=args.invalid)}
            elif args.command == "inspect": result = dataset.snapshot()
            elif args.command == "rollback": result = {"version": dataset.rollback(args.version)}
            elif args.command == "stats": result = dataset.stats(args.start, args.end, version=args.version)
            elif args.command == "window": result = list(dataset.window(args.start, args.end, version=args.version))
            else: result = list(dataset.resample(args.start, args.end, args.seconds, version=args.version))
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except (ValueError, KeyError, OSError, AlignmentLimitError) as exc:
        parser.exit(2, f"error: {exc}\n")


if __name__ == "__main__": main()
