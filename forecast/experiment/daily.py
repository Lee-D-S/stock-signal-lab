from __future__ import annotations

import json
import pickle
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

from forecast.data.storage import snapshot_id, write_parquet
from forecast.evaluation.metrics import classification_metrics, regression_metrics
from forecast.models.baselines import majority_probability, mean_return
from forecast.models.sklearn_models import predict_model

from . import core
from .contracts import build_dataset_contract, labels_for_prediction_date, latest_feature_rows


@dataclass(frozen=True)
class TrainingConfig:
    train_start: str = "2015-01-01"
    train_end: str = "2024-12-31"
    top_k: int = 5
    random_state: int = 42
    n_splits: int = 3
    test_size: int = 20
    purge: int = 20
    horizons: tuple[core.Horizon, ...] = ("1d", "5d", "20d", "year_end")


def _baseline_evaluation(frame: pd.DataFrame, *, horizon: core.Horizon, config: TrainingConfig) -> dict[str, object]:
    direction_column, return_column = core._target_columns(horizon)
    folds: list[dict[str, object]] = []
    evaluation_config = core.ExperimentConfig(
        n_splits=config.n_splits,
        test_size=config.test_size,
        purge=config.purge,
    )
    for train_index, test_index in core._splits(frame, horizon, evaluation_config):
        train = frame.loc[train_index]
        test = frame.loc[test_index]
        probability = majority_probability(train[direction_column])
        expected_return = mean_return(train[return_column])
        probability_series = pd.Series(probability, index=test.index)
        return_series = pd.Series(expected_return, index=test.index)
        folds.append({
            "classification": classification_metrics(test[direction_column], probability_series),
            "regression": regression_metrics(test[return_column], return_series),
            "train_rows": len(train),
            "test_rows": len(test),
        })
    return {
        "candidate": "baseline",
        "feature_group": "none",
        "horizon": horizon,
        "folds": folds,
        "balanced_accuracy": core._mean_metric(folds, "classification", "balanced_accuracy"),
        "roc_auc": core._mean_metric(folds, "classification", "roc_auc"),
        "brier": core._mean_metric(folds, "classification", "brier"),
        "mae": core._mean_metric(folds, "regression", "mae"),
        "rank_ic": core._mean_metric(folds, "regression", "rank_ic"),
    }


def _available_candidates(frame: pd.DataFrame) -> tuple[core.CandidateSpec, ...]:
    return tuple(
        spec
        for spec in core.DEFAULT_CANDIDATES
        if core.feature_group_columns(frame, spec.feature_group)
    )


def train_and_save_rosters(
    raw: pd.DataFrame,
    *,
    model_root: Path,
    config: TrainingConfig | None = None,
    complete_years: Iterable[int] | None = None,
) -> dict[str, object]:
    """Evaluate fixed candidates, select top-k per horizon, and persist model bundles."""
    config = config or TrainingConfig()
    dataset = build_dataset_contract(raw, complete_years=complete_years)
    labelled = core.build_experiment_labels(dataset.features, complete_years=complete_years)
    dates = pd.to_datetime(labelled["date"])
    train = labelled[(dates >= config.train_start) & (dates <= config.train_end)].copy()
    if train.empty:
        raise ValueError("Roster training requires non-empty training data")

    candidates = _available_candidates(train)
    if not candidates:
        raise ValueError("No candidate feature groups are available for roster training")
    evaluation_config = core.ExperimentConfig(
        train_start=config.train_start,
        train_end=config.train_end,
        top_k=config.top_k,
        random_state=config.random_state,
        n_splits=config.n_splits,
        test_size=config.test_size,
        purge=config.purge,
        horizons=config.horizons,
    )
    snapshot = snapshot_id(config.train_start, config.train_end, tuple(dataset.features.columns), len(dataset.features))
    model_root.mkdir(parents=True, exist_ok=True)
    evaluations: dict[str, list[dict[str, object]]] = {}
    rosters: dict[str, list[dict[str, object]]] = {}

    for horizon in config.horizons:
        candidate_evaluations = [
            core.evaluate_candidate(train, horizon=horizon, spec=spec, config=evaluation_config)
            for spec in candidates
        ]
        baseline = _baseline_evaluation(train, horizon=horizon, config=config)
        roster = core.select_top_candidates(candidate_evaluations, top_k=config.top_k)
        evaluations[horizon] = [baseline, *candidate_evaluations]
        rosters[horizon] = roster
        for selected in roster:
            spec = next(spec for spec in candidates if spec.name == selected["candidate"])
            classifier, regressor, feature_columns = core._fit_candidate(
                train,
                horizon=horizon,
                spec=spec,
                config=evaluation_config,
            )
            model_version = f"weekly-{horizon}-{snapshot}"
            bundle = {
                "candidate": spec.name,
                "horizon": horizon,
                "feature_group": spec.feature_group,
                "feature_columns": feature_columns,
                "classifier": classifier,
                "regressor": regressor,
                "model_version": model_version,
                "snapshot_id": snapshot,
            }
            bundle_path = model_root / f"{horizon}__{spec.name}.pkl"
            with bundle_path.open("wb") as handle:
                pickle.dump(bundle, handle, protocol=pickle.HIGHEST_PROTOCOL)
            selected["bundle_path"] = str(bundle_path)
            selected["model_version"] = model_version
            selected["snapshot_id"] = snapshot

    (model_root / "rosters.json").write_text(json.dumps(rosters, ensure_ascii=False, indent=2), encoding="utf-8")
    (model_root / "evaluations.json").write_text(json.dumps(evaluations, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "features": dataset.features,
        "labels": dataset.labels,
        "evaluations": evaluations,
        "rosters": rosters,
        "snapshot_id": snapshot,
        "model_root": str(model_root),
    }


def _prediction_direction(probability: pd.Series) -> pd.Series:
    values = pd.to_numeric(probability, errors="coerce")
    result = pd.Series(pd.NA, index=probability.index, dtype="Int64")
    available = values.notna()
    result.loc[available] = (values.loc[available] >= 0.5).astype("int64")
    return result


def _load_bundle(path: Path) -> dict[str, object]:
    with path.open("rb") as handle:
        bundle = pickle.load(handle)
    required = {"candidate", "horizon", "classifier", "regressor", "model_version", "snapshot_id"}
    missing = required - set(bundle)
    if missing:
        raise ValueError(f"Model bundle is missing keys: {sorted(missing)}")
    return bundle


def predict_with_saved_rosters(
    features: pd.DataFrame,
    labels: pd.DataFrame,
    *,
    model_root: Path,
    prediction_date: str,
    feature_asof: str,
    horizons: Iterable[core.Horizon] = ("1d", "5d", "20d", "year_end"),
    scored_at: str | None = None,
) -> pd.DataFrame:
    latest = latest_feature_rows(features, feature_asof=feature_asof)
    label_rows = labels_for_prediction_date(labels, feature_asof=feature_asof)
    prediction_date_value = pd.Timestamp(prediction_date).normalize()
    scored_at_value = scored_at or datetime.now(timezone.utc).isoformat()
    outputs: list[pd.DataFrame] = []

    rosters_path = model_root / "rosters.json"
    if not rosters_path.exists():
        raise FileNotFoundError(f"Roster manifest not found: {rosters_path}")
    rosters = json.loads(rosters_path.read_text(encoding="utf-8"))
    for horizon in horizons:
        selected = rosters.get(horizon, [])
        for entry in selected:
            bundle_path = Path(str(entry["bundle_path"]))
            bundle = _load_bundle(bundle_path)
            class_prediction = predict_model(bundle["classifier"], latest)
            reg_prediction = predict_model(bundle["regressor"], latest)
            output = latest[["ticker", "date"]].copy().rename(columns={"date": "feature_asof"})
            output["candidate"] = bundle["candidate"]
            output["model_version"] = bundle["model_version"]
            output["horizon"] = horizon
            output["probability_up"] = class_prediction["probability_up"].to_numpy()
            output["predicted_return"] = reg_prediction["predicted_return"].to_numpy()
            output["prediction_available"] = (
                class_prediction["prediction_available"].astype(bool).to_numpy()
                & reg_prediction["prediction_available"].astype(bool).to_numpy()
            )
            output["prediction_direction"] = _prediction_direction(output["probability_up"])
            output["prediction_date"] = prediction_date_value
            output["scored_at"] = scored_at_value
            output["snapshot_id"] = bundle["snapshot_id"]
            outputs.append(output)

    if not outputs:
        raise ValueError("Roster manifest contains no model bundles")
    model_outputs = pd.concat(outputs, ignore_index=True)
    group_columns = ["prediction_date", "feature_asof", "ticker", "horizon"]
    ensemble = (
        model_outputs.groupby(group_columns, as_index=False)
        .agg(
            probability_up=("probability_up", "mean"),
            predicted_return=("predicted_return", "mean"),
            model_count=("candidate", "nunique"),
            prediction_available=("prediction_available", "all"),
            model_version=("model_version", lambda values: "+".join(sorted(set(values)))),
            snapshot_id=("snapshot_id", "first"),
            scored_at=("scored_at", "first"),
        )
    )
    ensemble["candidate"] = "ensemble"
    ensemble["prediction_direction"] = _prediction_direction(ensemble["probability_up"])
    result = pd.concat([model_outputs, ensemble], ignore_index=True, sort=False)

    actuals = label_rows.rename(
        columns={"future_return": "actual_return", "direction": "actual_direction"}
    )[["ticker", "feature_asof", "horizon", "target_end", "actual_return", "actual_direction", "maturity_status"]]
    result = result.merge(actuals, on=["ticker", "feature_asof", "horizon"], how="left")
    return result.sort_values(["prediction_date", "horizon", "ticker", "candidate"]).reset_index(drop=True)


def summarise_predictions(predictions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for (horizon, candidate), frame in predictions.groupby(["horizon", "candidate"], sort=True):
        class_metrics = classification_metrics(frame["actual_direction"], frame["probability_up"])
        reg_metrics = regression_metrics(frame["actual_return"], frame["predicted_return"])
        rows.append({
            "horizon": horizon,
            "candidate": candidate,
            "rows": len(frame),
            "available_rows": int(frame["prediction_available"].sum()),
            "mature_rows": int(frame["maturity_status"].eq("mature").sum()),
            **class_metrics,
            **reg_metrics,
        })
    return pd.DataFrame(rows)


def run_daily_prediction(
    raw: pd.DataFrame,
    *,
    model_root: Path,
    prediction_date: str,
    feature_asof: str,
    artifact_root: Path,
    complete_years: Iterable[int] | None = None,
) -> dict[str, object]:
    dataset = build_dataset_contract(raw, complete_years=complete_years)
    predictions = predict_with_saved_rosters(
        dataset.features,
        dataset.labels,
        model_root=model_root,
        prediction_date=prediction_date,
        feature_asof=feature_asof,
    )
    artifact_root.mkdir(parents=True, exist_ok=True)
    feature_path = artifact_root / f"features_{feature_asof}.parquet"
    label_path = artifact_root / f"labels_{feature_asof}.parquet"
    prediction_path = artifact_root / f"predictions_{prediction_date}.parquet"
    summary_path = artifact_root / f"daily_summary_{prediction_date}.csv"
    write_parquet(dataset.features, feature_path, artifact_type="experiment_features", as_of=feature_asof, code_version="forecast-experiment-0.2.0")
    write_parquet(dataset.labels, label_path, artifact_type="experiment_labels", as_of=feature_asof, code_version="forecast-experiment-0.2.0")
    write_parquet(predictions, prediction_path, artifact_type="experiment_predictions", as_of=prediction_date, code_version="forecast-experiment-0.2.0")
    summary = summarise_predictions(predictions)
    summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
    return {
        "prediction_date": prediction_date,
        "feature_asof": feature_asof,
        "prediction_rows": len(predictions),
        "summary_rows": len(summary),
        "predictions": prediction_path,
        "summary": summary_path,
    }
