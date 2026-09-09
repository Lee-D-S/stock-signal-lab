from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import pandas as pd

from forecast.data.storage import default_artifact_root, read_frame, snapshot_id, write_markdown, write_parquet
from forecast.evaluation.reports import write_evaluation_report
from forecast.features.builder import build_numeric_features, feature_columns
from forecast.labels.returns import add_return_labels
from forecast.models.registry import save_model
from forecast.models.sklearn_models import fit_model, predict_model
from forecast.pipelines.daily import _dry_run_frame
from forecast.pipelines.weekly_train import train_and_evaluate


def run_shadow(input_path: Path | None = None, *, artifact_root: Path | None = None, as_of: str | None = None) -> dict[str, object]:
    """Run the complete numeric path without orders, notifications, or text inputs."""
    raw = read_frame(input_path) if input_path else _shadow_frame()
    features = build_numeric_features(raw)
    labels = add_return_labels(features)
    root = artifact_root or default_artifact_root()
    run_date = as_of or str(pd.to_datetime(labels["date"]).max().date())
    snapshot = snapshot_id(run_date, tuple(labels.columns), len(labels))

    feature_manifest = write_parquet(features, root / f"shadow_features_{run_date}.parquet", artifact_type="shadow_features", as_of=run_date, code_version="forecast-0.1.0")
    label_manifest = write_parquet(labels, root / f"shadow_labels_{run_date}.parquet", artifact_type="shadow_labels", as_of=run_date, code_version="forecast-0.1.0")
    columns = feature_columns(labels)
    evaluation: dict[str, object] = {}
    prediction_frames: list[pd.DataFrame] = []

    for horizon in (1, 5, 20):
        metrics = train_and_evaluate(labels, horizon=horizon, model_name="logistic", n_splits=3, test_size=20, purge=horizon)
        evaluation[str(horizon)] = metrics
        trainable = labels.dropna(subset=columns + [f"direction_{horizon}d", f"future_return_{horizon}d"])
        classifier = fit_model(trainable, feature_columns=columns, target_column=f"direction_{horizon}d", task="classification", model_name="logistic")
        regressor = fit_model(trainable, feature_columns=columns, target_column=f"future_return_{horizon}d", task="regression", model_name="ridge")
        model_version = f"shadow-{horizon}d-{snapshot}"
        save_model(classifier, root / f"{model_version}-classifier.pkl", model_version=model_version, snapshot_id=snapshot)
        save_model(regressor, root / f"{model_version}-regressor.pkl", model_version=model_version, snapshot_id=snapshot)

        latest = labels.sort_values(["ticker", "date"]).groupby("ticker", as_index=False).tail(1).copy()
        classification = predict_model(classifier, latest).rename(columns={"probability_up": "probability_up"})
        regression = predict_model(regressor, latest)[["ticker", "date", "predicted_return", "prediction_available"]]
        prediction = classification.merge(regression, on=["ticker", "date", "prediction_available"], how="outer")
        prediction["horizon"] = horizon
        prediction["as_of"] = run_date
        prediction["model_version"] = model_version
        prediction["snapshot_id"] = snapshot
        prediction_frames.append(prediction)

    predictions = pd.concat(prediction_frames, ignore_index=True)
    prediction_manifest = write_parquet(predictions, root / f"shadow_predictions_{run_date}.parquet", artifact_type="shadow_predictions", as_of=run_date, code_version="forecast-0.1.0")
    report_path = root / f"shadow_evaluation_{run_date}.md"
    write_evaluation_report(report_path, as_of=run_date, snapshot_id=snapshot, metrics=evaluation, model_status="shadow-candidate")
    write_markdown(root / f"shadow_summary_{run_date}.md", "Forecast shadow summary", {
        "Run": f"- as_of: `{run_date}`\n- snapshot_id: `{snapshot}`\n- rows: `{len(labels)}`\n- tickers: `{labels['ticker'].nunique()}`",
        "Artifacts": f"- features: `{feature_manifest.path}`\n- labels: `{label_manifest.path}`\n- predictions: `{prediction_manifest.path}`\n- evaluation: `{report_path}`",
        "Safety": "No order, broker, Telegram, news, keyword, or LLM path was called.",
    })
    return {"as_of": run_date, "snapshot_id": snapshot, "rows": len(labels), "predictions": len(predictions), "artifact_root": str(root)}


def _shadow_frame() -> pd.DataFrame:
    dates = pd.date_range(end=date.today(), periods=620, freq="B")
    rows: list[dict[str, object]] = []
    for ticker, base in (("000001", 10_000), ("000002", 20_000), ("000003", 30_000)):
        for index, current_date in enumerate(dates):
            close = base + index * 2 + ((index % 40) - 20) * 35 + (index % 7) * 3
            rows.append({
                "ticker": ticker,
                "date": current_date,
                "open": close - 5,
                "high": close + 20,
                "low": close - 20,
                "close": close,
                "volume": 1_000_000 + index * 100,
                "foreign_net": (index % 11 - 5) * 100,
                "institution_net": (index % 7 - 3) * 80,
                "individual_net": -(index % 5 - 2) * 120,
            })
    return pd.DataFrame(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the complete numeric forecast Shadow pipeline")
    parser.add_argument("--input", type=Path)
    parser.add_argument("--artifact-root", type=Path)
    parser.add_argument("--as-of")
    args = parser.parse_args()
    print(run_shadow(args.input, artifact_root=args.artifact_root, as_of=args.as_of))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
