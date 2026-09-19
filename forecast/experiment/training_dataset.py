from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from forecast.data.storage import snapshot_id, write_markdown, write_parquet
from forecast.features.price_volume import add_price_volume_features


DEFAULT_OUTPUT_DIR = Path("data/forecast_experiment")
DEFAULT_RAW_PATH = DEFAULT_OUTPUT_DIR / "raw_prices_train_2013-12-01_2024-12-31.parquet"
DEFAULT_FEATURE_START = date(2015, 1, 1)
DEFAULT_FEATURE_END = date(2024, 12, 31)
PRICE_WINDOWS = (1, 5, 20, 60, 120, 252)
RATIO_WINDOWS = (5, 20, 60, 120)


@dataclass(frozen=True)
class TrainingDatasetArtifacts:
    features_path: Path
    labels_path: Path
    train_path: Path
    train_csv_path: Path
    summary_path: Path
    features_rows: int
    labels_rows: int
    train_rows: int
    feature_count: int
    snapshot_id: str


def _normalise_price_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"ticker", "date", "close", "volume"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Raw price frame is missing required columns: {sorted(missing)}")
    result = frame.copy()
    result["ticker"] = result["ticker"].astype(str).str.zfill(6)
    result["date"] = pd.to_datetime(result["date"], errors="raise").dt.tz_localize(None)
    numeric_columns = [column for column in ("open", "high", "low", "close", "volume", "turnover") if column in result]
    for column in numeric_columns:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    result = result.drop_duplicates(["ticker", "date"], keep="last").sort_values(["ticker", "date"])
    if result["close"].isna().any() or result["close"].le(0).any():
        raise ValueError("Raw prices contain missing or non-positive close values")
    return result.reset_index(drop=True)


def _add_ratio_features(result: pd.DataFrame) -> pd.DataFrame:
    groups = result.groupby("ticker", sort=False)
    for window in RATIO_WINDOWS:
        volume_mean = groups["volume"].transform(lambda values: values.rolling(window, min_periods=window).mean())
        result[f"volume_ratio_{window}d"] = result["volume"] / volume_mean - 1
        if "turnover" in result:
            turnover_mean = groups["turnover"].transform(lambda values: values.rolling(window, min_periods=window).mean())
            result[f"turnover_ratio_{window}d"] = result["turnover"] / turnover_mean - 1
    for window in (20, 60):
        volume_mean = groups["volume"].transform(lambda values: values.rolling(window, min_periods=window).mean())
        volume_std = groups["volume"].transform(lambda values: values.rolling(window, min_periods=window).std())
        safe_volume_std = volume_std.where(volume_std.ne(0))
        result[f"volume_zscore_{window}d"] = ((result["volume"] - volume_mean) / safe_volume_std).fillna(0.0)
        if "turnover" in result:
            turnover_mean = groups["turnover"].transform(lambda values: values.rolling(window, min_periods=window).mean())
            turnover_std = groups["turnover"].transform(lambda values: values.rolling(window, min_periods=window).std())
            safe_turnover_std = turnover_std.where(turnover_std.ne(0))
            result[f"turnover_zscore_{window}d"] = ((result["turnover"] - turnover_mean) / safe_turnover_std).fillna(0.0)

    total_volume = groups["volume"].transform(lambda values: values.rolling(20, min_periods=20).sum())
    up_volume = result["volume"].where(result["daily_return"].gt(0), 0.0)
    down_volume = result["volume"].where(result["daily_return"].le(0), 0.0)
    up_volume_sum = up_volume.groupby(result["ticker"], sort=False).transform(lambda values: values.rolling(20, min_periods=20).sum())
    down_volume_sum = down_volume.groupby(result["ticker"], sort=False).transform(lambda values: values.rolling(20, min_periods=20).sum())
    safe_total_volume = total_volume.where(total_volume.ne(0))
    result["up_volume_share_20d"] = up_volume_sum / safe_total_volume
    result["down_volume_share_20d"] = down_volume_sum / safe_total_volume
    result["return_volume_interaction_1d"] = result["daily_return"] * result["volume_ratio_5d"]
    result["return_volume_interaction_5d"] = result["return_5d"] * result["volume_ratio_20d"]
    if {"high", "low", "close"}.issubset(result.columns):
        result["intraday_range"] = (result["high"] - result["low"]) / result["close"]
        candle_range = (result["high"] - result["low"]).where(lambda values: values.ne(0))
        result["close_position"] = (result["close"] - result["low"]) / candle_range
    return result


def build_price_volume_features(
    raw_prices: pd.DataFrame,
    *,
    feature_start: date,
    feature_end: date,
    snapshot: str,
) -> pd.DataFrame:
    """Build point-in-time price/volume features from the full warm-up history."""
    raw = _normalise_price_frame(raw_prices)
    result = add_price_volume_features(raw, windows=PRICE_WINDOWS)
    result = _add_ratio_features(result)
    result["feature_asof"] = result["date"]
    result["feature_schema_version"] = "price-volume-v3"

    excluded = {
        "ticker", "name", "market", "date", "feature_asof", "price_basis", "price_source",
        "feature_schema_version", "snapshot_id",
    }
    feature_columns = [
        column for column in result.columns
        if column not in excluded and pd.api.types.is_numeric_dtype(result[column])
    ]
    result["feature_missing_count"] = result[feature_columns].isna().sum(axis=1).astype("int16")
    result["feature_ready"] = result["feature_missing_count"].eq(0)
    result["snapshot_id"] = snapshot
    result = result[
        result["date"].between(pd.Timestamp(feature_start), pd.Timestamp(feature_end))
    ].copy()
    return result.sort_values(["ticker", "feature_asof"]).reset_index(drop=True)


def build_one_day_labels(
    raw_prices: pd.DataFrame,
    *,
    feature_start: date,
    target_cutoff: date,
    snapshot: str,
) -> pd.DataFrame:
    """Build mature next-trading-day direction labels without future leakage."""
    raw = _normalise_price_frame(raw_prices)
    groups = raw.groupby("ticker", sort=False)
    result = raw[["ticker", "name", "market", "date", "close"]].copy()
    result["feature_asof"] = result["date"]
    result["target_end"] = groups["date"].shift(-1)
    result["target_close"] = groups["close"].shift(-1)
    result["close_asof"] = result["close"]
    result["future_return"] = result["target_close"] / result["close_asof"] - 1
    result["direction"] = pd.Series(pd.NA, index=result.index, dtype="Int64")
    mature = result["target_end"].notna() & result["target_close"].notna()
    result.loc[mature, "direction"] = (result.loc[mature, "future_return"] > 0).astype("int8")
    result["maturity_status"] = result["target_end"].notna().map({True: "mature", False: "pending"})
    result["horizon"] = "1d"
    result["future_return_1d"] = result["future_return"]
    result["direction_1d"] = result["direction"]
    result["label_schema_version"] = "direction-1d-v1"
    result["snapshot_id"] = snapshot
    result = result[
        result["feature_asof"].between(pd.Timestamp(feature_start), pd.Timestamp(target_cutoff))
        & result["target_end"].le(pd.Timestamp(target_cutoff))
    ].copy()
    columns = [
        "ticker", "name", "market", "date", "feature_asof", "horizon", "target_end",
        "close_asof", "target_close", "future_return", "direction", "future_return_1d",
        "direction_1d", "maturity_status", "label_schema_version", "snapshot_id",
    ]
    return result[columns].sort_values(["ticker", "feature_asof"]).reset_index(drop=True)


def build_training_frame(features: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    """Join ready features to mature labels using ticker and feature_asof."""
    if features.duplicated(["ticker", "feature_asof"]).any():
        raise ValueError("Features contain duplicate ticker/feature_asof keys")
    if labels.duplicated(["ticker", "feature_asof"]).any():
        raise ValueError("Labels contain duplicate ticker/feature_asof keys")
    feature_part = features[features["feature_ready"]].copy()
    label_part = labels[labels["maturity_status"].eq("mature")].copy()
    train = feature_part.merge(
        label_part.drop(columns=["name", "market", "date"], errors="ignore"),
        on=["ticker", "feature_asof"],
        how="inner",
        validate="one_to_one",
    )
    if not train.empty and not (train["target_end"] > train["feature_asof"]).all():
        raise ValueError("Training labels contain a target_end that is not after feature_asof")
    future_columns = [column for column in train.columns if column.startswith("future_")]
    feature_like_future = [column for column in future_columns if column not in {"future_return", "future_return_1d"}]
    if feature_like_future:
        raise ValueError(f"Unexpected future feature columns entered training frame: {feature_like_future}")
    return train.sort_values(["ticker", "feature_asof"]).reset_index(drop=True)


def _model_feature_columns(features: pd.DataFrame) -> list[str]:
    excluded = {
        "ticker", "name", "market", "date", "feature_asof", "price_basis", "price_source",
        "feature_schema_version", "feature_missing_count", "feature_ready", "snapshot_id",
    }
    return [
        str(column) for column in features.columns
        if column not in excluded and pd.api.types.is_numeric_dtype(features[column])
    ]


def write_training_dataset_artifacts(
    features: pd.DataFrame,
    labels: pd.DataFrame,
    train: pd.DataFrame,
    *,
    output_dir: Path,
    feature_start: date,
    feature_end: date,
    snapshot: str,
) -> TrainingDatasetArtifacts:
    output_dir.mkdir(parents=True, exist_ok=True)
    features_path = output_dir / "features_price_volume_2015_2024.parquet"
    labels_path = output_dir / "labels_1d_2015_2024.parquet"
    train_path = output_dir / "train_price_volume_1d_2015_2024.parquet"
    train_csv_path = output_dir / "train_price_volume_1d_2015_2024.csv"
    summary_path = output_dir / "training_dataset_2015_2024.md"

    write_parquet(features, features_path, artifact_type="experiment_price_volume_features", schema_version="price-volume-features-3", as_of=feature_end.isoformat(), code_version="forecast-training-dataset-3")
    write_parquet(labels, labels_path, artifact_type="experiment_1d_labels", schema_version="direction-1d-labels-1", as_of=feature_end.isoformat(), code_version="forecast-training-dataset-3")
    write_parquet(train, train_path, artifact_type="experiment_training_dataset", schema_version="price-volume-direction-1d-1", as_of=feature_end.isoformat(), code_version="forecast-training-dataset-3")
    train.to_csv(train_csv_path, index=False, encoding="utf-8-sig")

    feature_columns = _model_feature_columns(features)
    direction_counts = train["direction"].value_counts(dropna=False).to_dict() if "direction" in train else {}
    write_markdown(
        summary_path,
        "2015-2024 price-volume training dataset",
        {
            "Contract": (
                f"- feature_start: `{feature_start.isoformat()}`\n"
                f"- feature_end: `{feature_end.isoformat()}`\n"
                "- row key: `ticker + feature_asof`\n"
                "- label: next available trading-day close direction\n"
                "- direction: `1` for up, `0` for down or unchanged"
            ),
            "Validation": (
                f"- snapshot_id: `{snapshot}`\n"
                f"- feature_rows: `{len(features)}`\n"
                f"- label_rows: `{len(labels)}`\n"
                f"- train_rows: `{len(train)}`\n"
                f"- ready_feature_rows: `{int(features['feature_ready'].sum())}`\n"
                f"- direction_counts: `{direction_counts}`\n"
                f"- model_feature_count: `{len(feature_columns)}`"
            ),
            "Artifacts": (
                f"- features: `{features_path}`\n"
                f"- labels: `{labels_path}`\n"
                f"- train Parquet: `{train_path}`\n"
                f"- train CSV: `{train_csv_path}`"
            ),
            "Feature columns": json.dumps(feature_columns, ensure_ascii=False),
        },
    )
    return TrainingDatasetArtifacts(
        features_path=features_path,
        labels_path=labels_path,
        train_path=train_path,
        train_csv_path=train_csv_path,
        summary_path=summary_path,
        features_rows=len(features),
        labels_rows=len(labels),
        train_rows=len(train),
        feature_count=len(feature_columns),
        snapshot_id=snapshot,
    )


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Build 2015-2024 price-volume features and one-day labels")
    parser.add_argument("--raw", type=Path, default=DEFAULT_RAW_PATH)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--feature-start", default=DEFAULT_FEATURE_START.isoformat())
    parser.add_argument("--feature-end", default=DEFAULT_FEATURE_END.isoformat())
    args = parser.parse_args()

    feature_start = date.fromisoformat(args.feature_start)
    feature_end = date.fromisoformat(args.feature_end)
    raw = pd.read_parquet(args.raw)
    snapshot = snapshot_id("price-volume-direction-1d-v3", args.raw, feature_start, feature_end, len(raw))
    features = build_price_volume_features(raw, feature_start=feature_start, feature_end=feature_end, snapshot=snapshot)
    labels = build_one_day_labels(raw, feature_start=feature_start, target_cutoff=feature_end, snapshot=snapshot)
    train = build_training_frame(features, labels)
    artifacts = write_training_dataset_artifacts(
        features,
        labels,
        train,
        output_dir=args.output_dir,
        feature_start=feature_start,
        feature_end=feature_end,
        snapshot=snapshot,
    )
    print(json.dumps({
        "features": str(artifacts.features_path),
        "labels": str(artifacts.labels_path),
        "train": str(artifacts.train_path),
        "train_csv": str(artifacts.train_csv_path),
        "feature_rows": artifacts.features_rows,
        "label_rows": artifacts.labels_rows,
        "train_rows": artifacts.train_rows,
        "feature_count": artifacts.feature_count,
        "snapshot_id": artifacts.snapshot_id,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
