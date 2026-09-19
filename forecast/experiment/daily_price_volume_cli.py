from __future__ import annotations

import argparse
from pathlib import Path

from forecast.data.storage import read_frame

from .daily_price_volume import (
    DEFAULT_ARTIFACT_ROOT,
    DEFAULT_MODEL_ROOT,
    DEFAULT_ROSTER_PATH,
    DEFAULT_TRAIN_PATH,
    build_daily_price_volume_predictions,
    score_daily_price_volume_file,
    write_daily_price_volume_artifacts,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run and score daily predictions from the active v2 price-volume roster"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    predict = subparsers.add_parser("predict", help="Create one daily prediction snapshot")
    predict.add_argument("--raw", type=Path, required=True, help="Raw OHLCV history through feature_asof")
    predict.add_argument("--train", type=Path, default=DEFAULT_TRAIN_PATH)
    predict.add_argument("--model-root", type=Path, default=DEFAULT_MODEL_ROOT)
    predict.add_argument("--roster", type=Path, default=DEFAULT_ROSTER_PATH)
    predict.add_argument("--prediction-date", required=True)
    predict.add_argument("--feature-asof", required=True)
    predict.add_argument("--artifact-root", type=Path, default=DEFAULT_ARTIFACT_ROOT)

    score = subparsers.add_parser("score", help="Attach realized next-day labels and refresh the scorecard")
    score.add_argument("--predictions", type=Path, required=True)
    score.add_argument("--raw", type=Path, required=True, help="Raw history including realized target dates")
    score.add_argument("--artifact-root", type=Path, default=None)

    args = parser.parse_args()
    if args.command == "predict":
        predictions = build_daily_price_volume_predictions(
            read_frame(args.raw),
            read_frame(args.train),
            model_root=args.model_root,
            roster_path=args.roster,
            prediction_date=args.prediction_date,
            feature_asof=args.feature_asof,
        )
        artifacts = write_daily_price_volume_artifacts(
            predictions,
            artifact_root=args.artifact_root,
        )
    else:
        artifacts = score_daily_price_volume_file(
            args.predictions,
            read_frame(args.raw),
            artifact_root=args.artifact_root,
        )
    print({key: str(value) for key, value in artifacts.items()})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
