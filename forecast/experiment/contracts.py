from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from .core import build_experiment_labels, build_feature_frame


@dataclass(frozen=True)
class ExperimentDataset:
    features: pd.DataFrame
    labels: pd.DataFrame


def build_dataset_contract(
    raw: pd.DataFrame,
    *,
    complete_years: Iterable[int] | None = None,
    feature_schema_version: str = "experiment-features-1",
) -> ExperimentDataset:
    """Create PIT numeric features and a separate long-panel label table."""
    features = build_feature_frame(raw)
    features = features.copy()
    features["feature_asof"] = pd.to_datetime(features["date"], errors="raise").dt.tz_localize(None)
    features["feature_schema_version"] = feature_schema_version

    numeric_columns = [
        column
        for column in features.columns
        if column not in {"ticker", "date", "feature_asof", "public_at", "feature_schema_version"}
        and pd.api.types.is_numeric_dtype(features[column])
    ]
    for column in numeric_columns:
        features[f"missing_{column}"] = features[column].isna().astype("int8")
    if "public_at" in features:
        public_at = pd.to_datetime(features["public_at"], errors="coerce")
        observed = pd.to_datetime(features["feature_asof"], errors="raise")
        features["days_since_public"] = (observed - public_at).dt.days

    labelled = build_experiment_labels(features, complete_years=complete_years)
    label_frames: list[pd.DataFrame] = []
    for horizon in ("1d", "5d", "20d", "year_end"):
        label_frames.append(
            labelled[
                [
                    "ticker", "date", "feature_asof", "feature_schema_version",
                    f"target_end_{horizon}", f"future_return_{horizon}",
                    f"direction_{horizon}", f"maturity_{horizon}",
                ]
            ]
            .rename(
                columns={
                    f"target_end_{horizon}": "target_end",
                    f"future_return_{horizon}": "future_return",
                    f"direction_{horizon}": "direction",
                    f"maturity_{horizon}": "maturity_status",
                }
            )
            .assign(horizon=horizon)
        )
    labels = pd.concat(label_frames, ignore_index=True)
    labels = labels[
        [
            "ticker", "date", "feature_asof", "feature_schema_version", "horizon",
            "target_end", "future_return", "direction", "maturity_status",
        ]
    ].sort_values(["ticker", "feature_asof", "horizon"]).reset_index(drop=True)
    return ExperimentDataset(features=features, labels=labels)


def latest_feature_rows(features: pd.DataFrame, *, feature_asof: str) -> pd.DataFrame:
    """Return one row per fixed-universe ticker at or before the requested as-of date."""
    frame = features.copy()
    frame["date"] = pd.to_datetime(frame["date"], errors="raise").dt.tz_localize(None)
    cutoff = pd.Timestamp(feature_asof).normalize()
    frame = frame[frame["date"] <= cutoff]
    if frame.empty:
        raise ValueError(f"No feature rows exist on or before {feature_asof}")
    return (
        frame.sort_values(["ticker", "date"])
        .groupby("ticker", as_index=False, sort=False)
        .tail(1)
        .sort_values("ticker")
        .reset_index(drop=True)
    )


def labels_for_prediction_date(labels: pd.DataFrame, *, feature_asof: str) -> pd.DataFrame:
    frame = labels.copy()
    cutoff = pd.Timestamp(feature_asof).normalize()
    frame["feature_asof"] = pd.to_datetime(frame["feature_asof"], errors="raise").dt.tz_localize(None)
    return frame[frame["feature_asof"].eq(cutoff)].copy()
