from __future__ import annotations

import argparse
from pathlib import Path

from forecast.data.storage import read_frame

from .daily import run_daily_prediction


def main() -> int:
    parser = argparse.ArgumentParser(description="Run fixed-universe daily predictions from saved model rosters")
    parser.add_argument("--input", type=Path, required=True, help="Raw numeric long-panel CSV or Parquet")
    parser.add_argument("--model-root", type=Path, required=True, help="Directory containing rosters.json and model bundles")
    parser.add_argument("--prediction-date", required=True)
    parser.add_argument("--feature-asof", required=True)
    parser.add_argument("--artifact-root", type=Path, default=Path("data/forecast_experiment"))
    parser.add_argument("--complete-year", type=int, action="append", dest="complete_years")
    args = parser.parse_args()
    result = run_daily_prediction(
        read_frame(args.input),
        model_root=args.model_root,
        prediction_date=args.prediction_date,
        feature_asof=args.feature_asof,
        artifact_root=args.artifact_root,
        complete_years=args.complete_years,
    )
    print({key: str(value) for key, value in result.items()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
