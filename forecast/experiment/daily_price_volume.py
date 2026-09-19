from __future__ import annotations

import json
import pickle
from pathlib import Path

import pandas as pd

from forecast.data.storage import snapshot_id, write_parquet
from forecast.evaluation.metrics import classification_metrics
from forecast.models.baselines import majority_probability
from forecast.models.sklearn_models import predict_model

from .training_dataset import (
    _normalise_price_frame,
    build_price_volume_features,
)


ACTIVE_SCHEMA_VERSION = "price-volume-v2"
DEFAULT_MODEL_ROOT = Path("data/forecast_experiment/models_price_volume_2025")
DEFAULT_ROSTER_PATH = Path("data/forecast_experiment/price_volume_model_roster.json")
DEFAULT_TRAIN_PATH = Path("data/forecast_experiment/train_price_volume_1d_2015_2024.parquet")
DEFAULT_ARTIFACT_ROOT = Path("data/forecast_experiment/daily_price_volume")


def _next_trading_day_labels(raw: pd.DataFrame) -> pd.DataFrame:
    normalised = _normalise_price_frame(raw)
    groups = normalised.groupby("ticker", sort=False)
    result = normalised[["ticker", "date", "close"]].copy()
    result["feature_asof"] = result["date"]
    result["target_date"] = groups["date"].shift(-1)
    result["target_close"] = groups["close"].shift(-1)
    result["actual_return"] = result["target_close"] / result["close"] - 1
    result["actual_direction"] = pd.Series(pd.NA, index=result.index, dtype="Int64")
    mature = result["target_date"].notna() & result["target_close"].notna()
    result.loc[mature, "actual_direction"] = (
        result.loc[mature, "actual_return"] > 0
    ).astype("int8")
    result["maturity_status"] = mature.map({True: "mature", False: "pending"})
    return result[
        [
            "ticker",
            "feature_asof",
            "target_date",
            "actual_return",
            "actual_direction",
            "maturity_status",
        ]
    ]


def attach_daily_actuals(predictions: pd.DataFrame, raw: pd.DataFrame) -> pd.DataFrame:
    """Attach next-trading-day outcomes without changing prediction features."""
    actuals = _next_trading_day_labels(raw)
    old_actual_columns = [
        "target_date",
        "actual_return",
        "actual_direction",
        "maturity_status",
    ]
    result = predictions.drop(columns=old_actual_columns, errors="ignore").merge(
        actuals,
        on=["ticker", "feature_asof"],
        how="left",
        validate="many_to_one",
    )
    return result.sort_values(["feature_asof", "ticker", "candidate"]).reset_index(drop=True)


def _load_roster(roster_path: Path) -> dict[str, object]:
    if not roster_path.exists():
        raise FileNotFoundError(f"Model roster not found: {roster_path}")
    roster = json.loads(roster_path.read_text(encoding="utf-8"))
    feature_columns = roster.get("feature_columns")
    models = roster.get("models")
    if not isinstance(feature_columns, list) or not feature_columns:
        raise ValueError("Model roster does not contain feature_columns")
    if not isinstance(models, list) or not models:
        raise ValueError("Model roster does not contain models")
    return roster


def _load_model(model_root: Path, candidate: str) -> object:
    model_path = model_root / f"{candidate}.pkl"
    if not model_path.exists():
        raise FileNotFoundError(f"Saved model not found for {candidate}: {model_path}")
    with model_path.open("rb") as handle:
        return pickle.load(handle)


def _validate_cutoff(prediction_date: str, feature_asof: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    prediction_cutoff = pd.Timestamp(prediction_date).normalize()
    feature_cutoff = pd.Timestamp(feature_asof).normalize()
    if prediction_cutoff <= feature_cutoff:
        raise ValueError("prediction_date must be after feature_asof")
    return prediction_cutoff, feature_cutoff


def build_daily_price_volume_predictions(
    raw: pd.DataFrame,
    train: pd.DataFrame,
    *,
    model_root: Path = DEFAULT_MODEL_ROOT,
    roster_path: Path = DEFAULT_ROSTER_PATH,
    prediction_date: str,
    feature_asof: str,
) -> pd.DataFrame:
    """Predict one trading day using only raw rows on or before feature_asof."""
    prediction_cutoff, feature_cutoff = _validate_cutoff(prediction_date, feature_asof)
    normalised = _normalise_price_frame(raw)
    history = normalised[normalised["date"].le(feature_cutoff)].copy()
    if history.empty:
        raise ValueError(f"Raw input contains no rows on or before {feature_asof}")

    roster = _load_roster(roster_path)
    feature_columns = [str(column) for column in roster["feature_columns"]]
    feature_snapshot = snapshot_id(
        "price-volume-daily-v2",
        feature_cutoff.isoformat(),
        len(history),
        tuple(feature_columns),
    )
    features = build_price_volume_features(
        history,
        feature_start=feature_cutoff.date(),
        feature_end=feature_cutoff.date(),
        snapshot=feature_snapshot,
    )
    if features.empty:
        raise ValueError(f"No feature rows exist for feature_asof={feature_asof}")
    if features["feature_schema_version"].ne(ACTIVE_SCHEMA_VERSION).any():
        raise ValueError(
            f"Daily runner requires {ACTIVE_SCHEMA_VERSION}, got {features['feature_schema_version'].unique().tolist()}"
        )
    missing_columns = sorted(set(feature_columns) - set(features.columns))
    if missing_columns:
        raise ValueError(f"Daily feature frame is missing roster columns: {missing_columns}")
    if not features["ticker"].isin(normalised["ticker"].unique()).all():
        raise ValueError("Daily feature frame contains an unknown ticker")

    outputs: list[pd.DataFrame] = []
    model_snapshot = str(roster.get("snapshot_id", "unknown"))
    for entry in roster["models"]:
        candidate = str(entry["candidate"])
        if candidate == "baseline":
            probability = pd.Series(
                majority_probability(train["direction"]),
                index=features.index,
                dtype="float64",
            )
            prediction_available = pd.Series(True, index=features.index, dtype="bool")
        else:
            model = _load_model(model_root, candidate)
            if tuple(feature_columns) != tuple(getattr(model, "feature_columns", ())):
                raise ValueError(f"Roster/model feature columns disagree for {candidate}")
            prediction = predict_model(model, features)
            probability = pd.to_numeric(prediction["probability_up"], errors="coerce")
            prediction_available = prediction["prediction_available"].astype(bool)

        output = features[["ticker", "name", "market", "feature_asof"]].copy()
        output["candidate"] = candidate
        output["probability_up"] = probability.to_numpy()
        output["prediction_direction"] = (
            output["probability_up"] >= 0.5
        ).astype("int8")
        output["prediction_available"] = prediction_available.to_numpy()
        output["prediction_date"] = prediction_cutoff
        output["model_snapshot_id"] = model_snapshot
        output["feature_snapshot_id"] = feature_snapshot
        output["model_count"] = 1
        outputs.append(output)

    model_outputs = pd.concat(outputs, ignore_index=True)
    group_columns = [
        "ticker",
        "name",
        "market",
        "feature_asof",
        "prediction_date",
        "model_snapshot_id",
        "feature_snapshot_id",
    ]
    ensemble = (
        model_outputs.groupby(group_columns, as_index=False, dropna=False)
        .agg(
            probability_up=("probability_up", "mean"),
            prediction_available=("prediction_available", "all"),
            model_count=("candidate", "nunique"),
        )
    )
    ensemble["candidate"] = "ensemble"
    ensemble["prediction_direction"] = (ensemble["probability_up"] >= 0.5).astype("int8")
    result = pd.concat([model_outputs, ensemble], ignore_index=True, sort=False)
    return attach_daily_actuals(result, normalised)


def summarise_daily_price_volume_predictions(predictions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for candidate, frame in predictions.groupby("candidate", sort=True):
        mature = frame[frame["actual_direction"].notna()].copy()
        metrics = classification_metrics(
            mature["actual_direction"],
            mature["probability_up"],
        )
        rows.append(
            {
                "feature_asof": frame["feature_asof"].iloc[0],
                "prediction_date": frame["prediction_date"].iloc[0],
                "candidate": candidate,
                "rows": len(frame),
                "available_rows": int(frame["prediction_available"].sum()),
                "mature_rows": len(mature),
                **metrics,
            }
        )
    return pd.DataFrame(rows).sort_values("candidate").reset_index(drop=True)


def refresh_daily_price_volume_scorecard(artifact_root: Path) -> pd.DataFrame:
    prediction_paths = sorted(artifact_root.glob("daily_price_volume_predictions_*.parquet"))
    if not prediction_paths:
        return pd.DataFrame()
    frames = [pd.read_parquet(path) for path in prediction_paths]
    predictions = pd.concat(frames, ignore_index=True)
    predictions = predictions.drop_duplicates(
        ["feature_asof", "ticker", "candidate"],
        keep="last",
    )
    rows: list[dict[str, object]] = []
    for candidate, frame in predictions.groupby("candidate", sort=True):
        mature = frame[frame["actual_direction"].notna()].copy()
        rows.append(
            {
                "candidate": candidate,
                "prediction_days": int(frame["feature_asof"].nunique()),
                "rows": len(frame),
                "available_rows": int(frame["prediction_available"].sum()),
                "mature_rows": len(mature),
                **classification_metrics(mature["actual_direction"], mature["probability_up"]),
            }
        )
    scorecard = pd.DataFrame(rows).sort_values("candidate").reset_index(drop=True)
    scorecard.to_csv(
        artifact_root / "daily_price_volume_scorecard.csv",
        index=False,
        encoding="utf-8-sig",
    )
    return scorecard


def write_daily_price_volume_artifacts(
    predictions: pd.DataFrame,
    *,
    artifact_root: Path,
) -> dict[str, Path]:
    feature_asof = pd.Timestamp(predictions["feature_asof"].iloc[0]).date().isoformat()
    artifact_root.mkdir(parents=True, exist_ok=True)
    predictions_path = artifact_root / f"daily_price_volume_predictions_{feature_asof}.parquet"
    predictions_csv_path = artifact_root / f"daily_price_volume_predictions_{feature_asof}.csv"
    summary_path = artifact_root / f"daily_price_volume_summary_{feature_asof}.csv"
    write_parquet(
        predictions,
        predictions_path,
        artifact_type="experiment_daily_price_volume_predictions",
        schema_version="price-volume-daily-v1",
        as_of=feature_asof,
        code_version="forecast-daily-price-volume-1",
    )
    predictions.to_csv(predictions_csv_path, index=False, encoding="utf-8-sig")
    summarise_daily_price_volume_predictions(predictions).to_csv(
        summary_path,
        index=False,
        encoding="utf-8-sig",
    )
    refresh_daily_price_volume_scorecard(artifact_root)
    return {
        "predictions": predictions_path,
        "predictions_csv": predictions_csv_path,
        "summary": summary_path,
        "scorecard": artifact_root / "daily_price_volume_scorecard.csv",
    }


def score_daily_price_volume_file(
    predictions_path: Path,
    raw: pd.DataFrame,
    *,
    artifact_root: Path | None = None,
) -> dict[str, Path]:
    predictions = pd.read_parquet(predictions_path)
    scored = attach_daily_actuals(predictions, raw)
    write_parquet(
        scored,
        predictions_path,
        artifact_type="experiment_daily_price_volume_predictions",
        schema_version="price-volume-daily-v1",
        as_of=str(scored["feature_asof"].max().date()),
        code_version="forecast-daily-price-volume-1",
    )
    scored.to_csv(predictions_path.with_suffix(".csv"), index=False, encoding="utf-8-sig")
    root = artifact_root or predictions_path.parent
    summary_path = root / f"daily_price_volume_summary_{pd.Timestamp(scored['feature_asof'].iloc[0]).date().isoformat()}.csv"
    summarise_daily_price_volume_predictions(scored).to_csv(
        summary_path,
        index=False,
        encoding="utf-8-sig",
    )
    refresh_daily_price_volume_scorecard(root)
    return {"predictions": predictions_path, "summary": summary_path, "scorecard": root / "daily_price_volume_scorecard.csv"}
