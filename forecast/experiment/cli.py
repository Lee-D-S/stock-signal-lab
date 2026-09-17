from __future__ import annotations

import argparse
from pathlib import Path

from forecast.data.storage import read_frame
from .core import ExperimentConfig, run_historical_replay


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the fixed-universe daily multi-model experiment")
    parser.add_argument("command", choices=("replay",), help="Experiment command")
    parser.add_argument("--input", type=Path, required=True, help="Numeric long-panel CSV or Parquet")
    parser.add_argument("--artifact-root", type=Path, default=Path("data/forecast_experiment"))
    parser.add_argument("--train-end", default="2024-12-31")
    parser.add_argument("--test-start", default="2025-01-01")
    parser.add_argument("--top-k", type=int, default=5)
    parser.add_argument("--complete-year", type=int, action="append", dest="complete_years")
    args = parser.parse_args()

    frame = read_frame(args.input)
    complete_years = args.complete_years or list(range(2015, 2026))
    result = run_historical_replay(
        frame,
        config=ExperimentConfig(
            train_end=args.train_end,
            test_start=args.test_start,
            top_k=args.top_k,
        ),
        artifact_root=args.artifact_root,
        complete_years=complete_years,
    )
    print({
        "snapshot_id": result["snapshot_id"],
        "prediction_rows": len(result["predictions"]),
        "artifact_root": str(args.artifact_root),
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

