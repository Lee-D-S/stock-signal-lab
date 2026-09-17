from __future__ import annotations

import argparse
from pathlib import Path

from forecast.data.storage import read_frame

from .universe import UniverseConfig, build_fixed_universe, write_universe_files


def main() -> int:
    parser = argparse.ArgumentParser(description="Create the dated fixed-company universe manifest")
    parser.add_argument("--input", type=Path, required=True, help="Candidate snapshot CSV or Parquet")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rejected-output", type=Path)
    parser.add_argument("--selection-date", default="2024-12-31")
    parser.add_argument("--size", type=int, default=50)
    parser.add_argument("--min-history-days", type=int, default=250)
    parser.add_argument("--min-avg-value-20d", type=float, default=0.0)
    args = parser.parse_args()
    result = build_fixed_universe(
        read_frame(args.input),
        config=UniverseConfig(
            selection_date=args.selection_date,
            size=args.size,
            min_history_days=args.min_history_days,
            min_avg_value_20d=args.min_avg_value_20d,
        ),
    )
    write_universe_files(result, csv_path=args.output, rejected_path=args.rejected_output)
    print({"selected": len(result.manifest), "rejected": len(result.rejected), "output": str(args.output)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
