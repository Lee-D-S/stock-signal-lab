from __future__ import annotations

import json
import pickle
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from forecast.data.storage import snapshot_id, write_markdown, write_parquet
from forecast.evaluation.metrics import classification_metrics
from forecast.models.baselines import majority_probability
from forecast.models.sklearn_models import FittedModel, fit_model, predict_model

from .training_dataset import (
    DEFAULT_OUTPUT_DIR,
    build_price_volume_features,
    _model_feature_columns,
)


DEFAULT_TRAIN_PATH = DEFAULT_OUTPUT_DIR / "train_price_volume_1d_2015_2024.parquet"
DEFAULT_FEATURES_PATH = DEFAULT_OUTPUT_DIR / "features_price_volume_2015_2024.parquet"
DEFAULT_RAW_TRAIN_PATH = DEFAULT_OUTPUT_DIR / "raw_prices_train_2013-12-01_2024-12-31.parquet"
DEFAULT_RAW_2025_PATH = DEFAULT_OUTPUT_DIR / "raw_prices_2025.parquet"
DEFAULT_LABELS_2025_PATH = DEFAULT_OUTPUT_DIR / "labels_2025.parquet"
DEFAULT_MODEL_DIR = DEFAULT_OUTPUT_DIR / "models_price_volume_2025"
DEFAULT_VALIDATION_YEARS = (2020, 2021, 2022, 2023, 2024)


@dataclass(frozen=True)
class CandidateSpec:
    name: str
    model_name: str | None


@dataclass(frozen=True)
class ModelExperimentArtifacts:
    evaluations_path: Path
    roster_path: Path
    predictions_path: Path
    predictions_csv_path: Path
    summary_path: Path
    evaluations: pd.DataFrame
    predictions: pd.DataFrame
    summary: pd.DataFrame


DEFAULT_CANDIDATES = (
    CandidateSpec("baseline", None),
    CandidateSpec("logistic", "logistic"),
    CandidateSpec("random_forest", "random_forest"),
    CandidateSpec("hist_gradient_boosting", "hist_gradient_boosting"),
)


def _year_folds(frame: pd.DataFrame, validation_years: tuple[int, ...]) -> list[tuple[int, pd.DataFrame, pd.DataFrame]]:
    feature_dates = pd.to_datetime(frame["feature_asof"], errors="raise").dt.normalize()
    target_dates = pd.to_datetime(frame["target_end"], errors="raise").dt.normalize()
    folds: list[tuple[int, pd.DataFrame, pd.DataFrame]] = []
    for year in validation_years:
        year_start = pd.Timestamp(year=year, month=1, day=1)
        train_mask = (feature_dates < year_start) & (target_dates < year_start)
        test_mask = feature_dates.dt.year.eq(year)
        train = frame.loc[train_mask].copy()
        test = frame.loc[test_mask].copy()
        if not train.empty and not test.empty:
            folds.append((year, train, test))
    return folds


def _accuracy(y_true: pd.Series, probability_up: pd.Series) -> float | None:
    frame = pd.DataFrame({"actual": y_true, "probability": probability_up}).dropna()
    if frame.empty:
        return None
    predicted = (frame["probability"].astype(float) >= 0.5).astype(int)
    return float((predicted == frame["actual"].astype(int)).mean())


def _predict_candidate(
    spec: CandidateSpec,
    train: pd.DataFrame,
    test: pd.DataFrame,
    *,
    feature_columns: list[str],
    model_root: Path | None = None,
    model_filename: str | None = None,
) -> tuple[pd.Series, FittedModel | None]:
    if spec.model_name is None:
        probability = pd.Series(majority_probability(train["direction"]), index=test.index, dtype="float64")
        return probability, None
    prepared_train = train.copy()
    prepared_test = test.copy()
    for column in feature_columns:
        prepared_train[column] = pd.to_numeric(prepared_train[column], errors="coerce").replace([float("inf"), float("-inf")], float("nan"))
        prepared_test[column] = pd.to_numeric(prepared_test[column], errors="coerce").replace([float("inf"), float("-inf")], float("nan"))
    medians = prepared_train[feature_columns].median()
    prepared_train[feature_columns] = prepared_train[feature_columns].fillna(medians)
    prepared_test[feature_columns] = prepared_test[feature_columns].fillna(medians)
    fitted = fit_model(
        prepared_train,
        feature_columns=feature_columns,
        target_column="direction",
        task="classification",
        model_name=spec.model_name,
    )
    prediction = predict_model(fitted, prepared_test)
    probability = pd.to_numeric(prediction["probability_up"], errors="coerce")
    if model_root is not None and model_filename is not None:
        model_root.mkdir(parents=True, exist_ok=True)
        with (model_root / model_filename).open("wb") as handle:
            pickle.dump(fitted, handle, protocol=pickle.HIGHEST_PROTOCOL)
    return probability.set_axis(test.index), fitted


def evaluate_candidates(
    train: pd.DataFrame,
    *,
    feature_columns: list[str],
    validation_years: tuple[int, ...] = DEFAULT_VALIDATION_YEARS,
) -> tuple[pd.DataFrame, dict[str, list[dict[str, object]]]]:
    """Evaluate price-volume candidates with expanding year-based folds."""
    rows: list[dict[str, object]] = []
    fold_details: dict[str, list[dict[str, object]]] = {}
    folds = _year_folds(train, validation_years)
    if not folds:
        raise ValueError("No usable year-based validation folds were found")

    for spec in DEFAULT_CANDIDATES:
        details: list[dict[str, object]] = []
        for year, fold_train, fold_test in folds:
            probability, _ = _predict_candidate(spec, fold_train, fold_test, feature_columns=feature_columns)
            metrics = classification_metrics(fold_test["direction"], probability)
            details.append({
                "validation_year": year,
                "train_rows": len(fold_train),
                "test_rows": len(fold_test),
                "accuracy": _accuracy(fold_test["direction"], probability),
                **metrics,
            })
        fold_details[spec.name] = details
        metric_frame = pd.DataFrame(details)
        rows.append({
            "candidate": spec.name,
            "model_name": spec.model_name or "majority_probability",
            "folds": len(details),
            "rows": int(metric_frame["test_rows"].sum()),
            "accuracy": float(metric_frame["accuracy"].mean()),
            "balanced_accuracy": float(metric_frame["balanced_accuracy"].mean()),
            "roc_auc": float(metric_frame["roc_auc"].dropna().mean()) if metric_frame["roc_auc"].notna().any() else None,
            "pr_auc": float(metric_frame["pr_auc"].dropna().mean()) if metric_frame["pr_auc"].notna().any() else None,
            "brier": float(metric_frame["brier"].mean()),
            "calibration_error": float(metric_frame["calibration_error"].dropna().mean()) if metric_frame["calibration_error"].notna().any() else None,
        })
    evaluations = pd.DataFrame(rows).sort_values(
        ["balanced_accuracy", "roc_auc", "brier"],
        ascending=[False, False, True],
        na_position="last",
    ).reset_index(drop=True)
    evaluations["rank"] = evaluations.index + 1
    return evaluations, fold_details


def _load_2025_prediction_frame(
    raw_training: pd.DataFrame,
    raw_2025: pd.DataFrame,
    labels_2025: pd.DataFrame,
    *,
    snapshot: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw = pd.concat([raw_training, raw_2025], ignore_index=True, sort=False)
    features = build_price_volume_features(
        raw,
        feature_start=date(2024, 12, 30),
        feature_end=date(2025, 12, 30),
        snapshot=snapshot,
    )
    labels = labels_2025.copy()
    labels["ticker"] = labels["ticker"].astype(str).str.zfill(6)
    labels["feature_asof"] = pd.to_datetime(labels["feature_asof"], errors="raise").dt.tz_localize(None)
    labels["target_date"] = pd.to_datetime(labels["target_date"], errors="raise").dt.tz_localize(None)
    prediction = features.merge(
        labels[["ticker", "feature_asof", "target_date", "future_return_1d", "direction_1d", "maturity_status"]],
        on=["ticker", "feature_asof"],
        how="inner",
        validate="one_to_one",
    )
    if prediction.empty:
        raise ValueError("No 2025 feature rows matched the 2025 labels")

    if not prediction["maturity_status"].eq("mature").all():
        raise ValueError("2025 prediction frame contains immature labels")
    return features, prediction.sort_values(["ticker", "feature_asof"]).reset_index(drop=True)


def _build_2025_predictions(
    train: pd.DataFrame,
    prediction_frame: pd.DataFrame,
    roster: pd.DataFrame,
    *,
    feature_columns: list[str],
    model_root: Path,
    snapshot: str,
) -> pd.DataFrame:
    outputs: list[pd.DataFrame] = []
    for selected in roster.itertuples(index=False):
        spec = CandidateSpec(str(selected.candidate), None if selected.candidate == "baseline" else str(selected.model_name))
        probability, _ = _predict_candidate(
            spec,
            train,
            prediction_frame,
            feature_columns=feature_columns,
            model_root=model_root,
            model_filename=f"{spec.name}.pkl",
        )
        output = prediction_frame[["ticker", "name", "market", "feature_asof", "target_date", "future_return_1d", "direction_1d"]].copy()
        output = output.rename(columns={"future_return_1d": "actual_return", "direction_1d": "actual_direction"})
        output["candidate"] = spec.name
        output["probability_up"] = probability.to_numpy()
        output["prediction_direction"] = (output["probability_up"] >= 0.5).astype("int8")
        output["prediction_available"] = output["probability_up"].notna()
        output["model_version"] = f"price-volume-2025-{snapshot}"
        output["snapshot_id"] = snapshot
        outputs.append(output)

    model_outputs = pd.concat(outputs, ignore_index=True)
    group_columns = ["ticker", "name", "market", "feature_asof", "target_date", "actual_return", "actual_direction"]
    ensemble = (
        model_outputs.groupby(group_columns, as_index=False, dropna=False)
        .agg(
            probability_up=("probability_up", "mean"),
            model_count=("candidate", "nunique"),
            prediction_available=("prediction_available", "all"),
        )
    )
    ensemble["candidate"] = "ensemble"
    ensemble["prediction_direction"] = (ensemble["probability_up"] >= 0.5).astype("int8")
    ensemble["model_version"] = f"price-volume-2025-ensemble-{snapshot}"
    ensemble["snapshot_id"] = snapshot
    return pd.concat([model_outputs, ensemble], ignore_index=True, sort=False).sort_values(
        ["target_date", "ticker", "candidate"]
    ).reset_index(drop=True)


def _prediction_summary(predictions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for candidate, frame in predictions.groupby("candidate", sort=True):
        metrics = classification_metrics(frame["actual_direction"], frame["probability_up"])
        rows.append({
            "candidate": candidate,
            "rows": len(frame),
            "model_count": int(frame["model_count"].dropna().iloc[0]) if "model_count" in frame and frame["model_count"].notna().any() else 1,
            "hit_rate": float((frame["prediction_direction"] == frame["actual_direction"]).mean()),
            **metrics,
        })
    return pd.DataFrame(rows).sort_values(["balanced_accuracy", "roc_auc"], ascending=[False, False], na_position="last").reset_index(drop=True)


def run_price_volume_experiment(
    train: pd.DataFrame,
    features: pd.DataFrame,
    raw_training: pd.DataFrame,
    raw_2025: pd.DataFrame,
    labels_2025: pd.DataFrame,
    *,
    output_dir: Path,
    model_dir: Path,
    top_k: int = 5,
    validation_years: tuple[int, ...] = DEFAULT_VALIDATION_YEARS,
) -> ModelExperimentArtifacts:
    feature_columns = _model_feature_columns(features)
    if not feature_columns:
        raise ValueError("No model feature columns were found")
    snapshot = snapshot_id("price-volume-model-experiment", len(train), len(features), len(labels_2025), tuple(feature_columns))
    evaluations, fold_details = evaluate_candidates(train, feature_columns=feature_columns, validation_years=validation_years)
    roster = evaluations.head(max(1, top_k)).copy()
    model_dir.mkdir(parents=True, exist_ok=True)
    prediction_features, prediction_frame = _load_2025_prediction_frame(raw_training, raw_2025, labels_2025, snapshot=snapshot)
    predictions = _build_2025_predictions(
        train,
        prediction_frame,
        roster,
        feature_columns=feature_columns,
        model_root=model_dir,
        snapshot=snapshot,
    )
    summary = _prediction_summary(predictions)

    output_dir.mkdir(parents=True, exist_ok=True)
    evaluations_path = output_dir / "price_volume_model_evaluations.json"
    roster_path = output_dir / "price_volume_model_roster.json"
    predictions_path = output_dir / "predictions_2025_price_volume.parquet"
    predictions_csv_path = output_dir / "predictions_2025_price_volume.csv"
    summary_path = output_dir / "price_volume_model_experiment_2025.md"
    evaluations_path.write_text(json.dumps({"summary": evaluations.to_dict(orient="records"), "folds": fold_details}, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    roster_path.write_text(json.dumps({"snapshot_id": snapshot, "feature_columns": feature_columns, "models": roster.to_dict(orient="records")}, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    write_parquet(predictions, predictions_path, artifact_type="experiment_2025_price_volume_predictions", schema_version="price-volume-predictions-1", as_of="2025-12-31", code_version="forecast-price-volume-models-1")
    predictions.to_csv(predictions_csv_path, index=False, encoding="utf-8-sig")
    summary.to_csv(output_dir / "price_volume_model_summary_2025.csv", index=False, encoding="utf-8-sig")
    write_markdown(
        summary_path,
        "2025 price-volume model experiment",
        {
            "Method": (
                "- candidates: majority baseline, Logistic Regression, Random Forest, HistGradientBoosting\n"
                f"- validation years: `{list(validation_years)}`\n"
                f"- selected roster size: `{len(roster)}`\n"
                "- training cutoff: 2024-12-31\n"
                "- prediction features: price-volume only"
            ),
            "Validation": f"```\n{evaluations.to_string(index=False)}\n```",
            "2025 result": f"```\n{summary.to_string(index=False)}\n```",
            "Artifacts": (
                f"- evaluations: `{evaluations_path}`\n"
                f"- roster: `{roster_path}`\n"
                f"- predictions Parquet: `{predictions_path}`\n"
                f"- predictions CSV: `{predictions_csv_path}`\n"
                f"- model bundles: `{model_dir}`"
            ),
        },
    )
    return ModelExperimentArtifacts(
        evaluations_path=evaluations_path,
        roster_path=roster_path,
        predictions_path=predictions_path,
        predictions_csv_path=predictions_csv_path,
        summary_path=summary_path,
        evaluations=evaluations,
        predictions=predictions,
        summary=summary,
    )


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Benchmark price-volume models and replay 2025 predictions")
    parser.add_argument("--train", type=Path, default=DEFAULT_TRAIN_PATH)
    parser.add_argument("--features", type=Path, default=DEFAULT_FEATURES_PATH)
    parser.add_argument("--raw-training", type=Path, default=DEFAULT_RAW_TRAIN_PATH)
    parser.add_argument("--raw-2025", type=Path, default=DEFAULT_RAW_2025_PATH)
    parser.add_argument("--labels-2025", type=Path, default=DEFAULT_LABELS_2025_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--model-dir", type=Path, default=DEFAULT_MODEL_DIR)
    parser.add_argument("--top-k", type=int, default=5)
    args = parser.parse_args()

    train = pd.read_parquet(args.train)
    features = pd.read_parquet(args.features)
    raw_training = pd.read_parquet(args.raw_training)
    raw_2025 = pd.read_parquet(args.raw_2025)
    labels_2025 = pd.read_parquet(args.labels_2025)
    artifacts = run_price_volume_experiment(
        train,
        features,
        raw_training,
        raw_2025,
        labels_2025,
        output_dir=args.output_dir,
        model_dir=args.model_dir,
        top_k=args.top_k,
    )
    print(json.dumps({
        "evaluations": str(artifacts.evaluations_path),
        "roster": str(artifacts.roster_path),
        "predictions": str(artifacts.predictions_path),
        "predictions_csv": str(artifacts.predictions_csv_path),
        "summary": str(artifacts.summary_path),
        "selected_models": artifacts.roster_path.read_text(encoding="utf-8"),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
