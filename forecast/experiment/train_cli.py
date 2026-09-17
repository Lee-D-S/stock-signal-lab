from __future__ import annotations

import argparse
from pathlib import Path

from forecast.data.storage import read_frame

from .daily import TrainingConfig, train_and_save_rosters


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate candidates and save fixed top-k model rosters")
    parser.add_argument("--input", type=Path, required=True, help="Raw numeric long-panel CSV or Parquet")
    parser.add_argument("--model-root", type=Path, required=True)
    parser.add_argument("--train-start", default="2015-01-01")
    parser.add_argument("--train-end", default="2024-12-31")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--complete-year", type=int, action="append", dest="complete_years")
    args = parser.parse_args()
    result = train_and_save_rosters(
        read_frame(args.input),
        model_root=args.model_root,
        config=TrainingConfig(train_start=args.train_start, train_end=args.train_end, top_k=args.top_k),
        complete_years=args.complete_years,
    )
    print({"snapshot_id": result["snapshot_id"], "model_root": result["model_root"], "rosters": {key: len(value) for key, value in result["rosters"].items()}})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
