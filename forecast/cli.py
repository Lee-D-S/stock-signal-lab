from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from forecast.data.storage import default_artifact_root, write_parquet
from forecast.pipelines.daily import run_daily_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Numeric point-in-time stock forecasting research lab")
    subparsers = parser.add_subparsers(dest="command", required=True)
    collect = subparsers.add_parser("collect", help="Validate/copy raw numeric input")
    collect.add_argument("--input", type=Path)
    collect.add_argument("--output", type=Path)
    collect.add_argument("--dry-run", action="store_true")
    daily = subparsers.add_parser("daily", help="Build features and mature labels")
    daily.add_argument("--input", type=Path)
    daily.add_argument("--artifact-root", type=Path)
    daily.add_argument("--as-of")
    daily.add_argument("--dry-run", action="store_true")
    for name in ("build-features", "build-labels", "predict", "train", "evaluate"):
        command = subparsers.add_parser(name, help=f"{name} is part of the staged pipeline")
        command.add_argument("--input", type=Path, required=name != "evaluate")
        command.add_argument("--artifact-root", type=Path)
        command.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "collect":
        if args.dry_run:
            print("collect: dry-run accepted; no KIS/DART request was sent")
            return 0
        if args.input is None:
            raise SystemExit("collect requires --input until the point-in-time universe manifest is configured")
        frame = pd.read_parquet(args.input) if args.input.suffix.lower() == ".parquet" else pd.read_csv(args.input)
        print(write_parquet(frame, args.output or (default_artifact_root() / "raw_input.parquet"), artifact_type="raw_numeric", code_version="forecast-0.1.0"))
        return 0
    if args.command == "daily":
        print(run_daily_pipeline(args.input, as_of=args.as_of, artifact_root=args.artifact_root, dry_run=args.dry_run))
        return 0
    if args.dry_run:
        print(f"{args.command}: dry-run accepted; no external API or model artifact was created")
        return 0
    raise SystemExit(f"{args.command} is not implemented for online execution yet; use --dry-run or the staged Python API")


if __name__ == "__main__":
    raise SystemExit(main())
