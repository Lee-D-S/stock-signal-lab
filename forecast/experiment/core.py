from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Iterable, Literal

import pandas as pd

from forecast.data.storage import snapshot_id, write_markdown, write_parquet
from forecast.evaluation.metrics import classification_metrics, regression_metrics
from forecast.evaluation.walk_forward import walk_forward_splits
from forecast.features.builder import build_numeric_features
from forecast.models.sklearn_models import FittedModel, fit_model, predict_model

Horizon = Literal["1d", "5d", "20d", "year_end"]


@dataclass(frozen=True)
class ExperimentConfig:
    train_start: str = "2015-01-01"
    train_end: str = "2024-12-31"
    test_start: str = "2025-01-01"
    top_k: int = 5
    random_state: int = 42
    n_splits: int = 3
    test_size: int = 20
    purge: int = 20
    horizons: tuple[Horizon, ...] = ("1d", "5d", "20d", "year_end")


@dataclass(frozen=True)
class CandidateSpec:
    name: str
    feature_group: str
    classifier: str
    regressor: str


DEFAULT_CANDIDATES: tuple[CandidateSpec, ...] = (
    CandidateSpec("logistic_price_volume", "price_volume", "logistic", "ridge"),
    CandidateSpec("logistic_flow", "flow", "logistic", "ridge"),
    CandidateSpec("logistic_fundamentals", "fundamentals", "logistic", "ridge"),
    CandidateSpec("logistic_valuation", "valuation", "logistic", "ridge"),
    CandidateSpec("logistic_all", "all", "logistic", "ridge"),
    CandidateSpec("random_forest_all", "all", "random_forest", "random_forest"),
    CandidateSpec("random_forest_price_volume", "price_volume", "random_forest", "random_forest"),
    CandidateSpec("hist_gradient_boosting_all", "all", "hist_gradient_boosting", "hist_gradient_boosting"),
    CandidateSpec("hist_gradient_boosting_flow", "flow", "hist_gradient_boosting", "hist_gradient_boosting"),
)


def _suffix(horizon: Horizon) -> str:
    return horizon


def _excluded_column(column: str) -> bool:
    lowered = column.lower()
    return (
        column in {"ticker", "date", "public_at", "as_of", "name", "market"}
        or lowered.startswith("future_")
        or lowered.startswith("direction_")
        or lowered.endswith("_maturity")
        or lowered in {"target_end_year_end"}
    )


def feature_group_columns(frame: pd.DataFrame, group: str) -> list[str]:
    numeric = [
        str(column)
        for column in frame.columns
        if not _excluded_column(str(column)) and pd.api.types.is_numeric_dtype(frame[column])
    ]
    if group == "all":
        return numeric

    prefixes = {
        "price_volume": ("daily_return", "return_", "volume_change_", "volatility_", "close_ma_gap_"),
        "candle": ("candle_", "upper_shadow_", "lower_shadow_", "close_position"),
        "flow": ("foreign_", "institution_", "individual_"),
        "fundamentals": ("revenue", "operating_income", "net_income", "assets", "equity", "eps", "bps", "operating_margin", "roe"),
        "valuation": ("per", "pbr", "psr", "dividend_yield"),
    }
    selected_prefixes = prefixes.get(group)
    if selected_prefixes is None:
        raise ValueError(f"Unknown feature group: {group}")
    return [column for column in numeric if column.startswith(selected_prefixes)]


def build_feature_frame(raw: pd.DataFrame) -> pd.DataFrame:
    return build_numeric_features(raw).sort_values(["ticker", "date"]).reset_index(drop=True)


def build_experiment_labels(
    features: pd.DataFrame,
    *,
    complete_years: Iterable[int] | None = None,
) -> pd.DataFrame:
    result = features.sort_values(["ticker", "date"]).copy()
    result["date"] = pd.to_datetime(result["date"], errors="raise").dt.tz_localize(None)
    groups = result.groupby("ticker", sort=False)

    for horizon in (1, 5, 20):
        result[f"future_close_{horizon}d"] = groups["close"].shift(-horizon)
        result[f"target_end_{horizon}d"] = groups["date"].shift(-horizon)
        result[f"future_return_{horizon}d"] = result[f"future_close_{horizon}d"] / result["close"] - 1
        result[f"direction_{horizon}d"] = (result[f"future_return_{horizon}d"] > 0).astype("Float64")
        result.loc[result[f"future_return_{horizon}d"].isna(), f"direction_{horizon}d"] = pd.NA
        result[f"maturity_{horizon}d"] = result[f"future_return_{horizon}d"].notna().map({True: "mature", False: "pending"})

    result["_label_year"] = result["date"].dt.year
    year_groups = result.groupby(["ticker", "_label_year"], sort=False)
    result["target_end_year_end"] = year_groups["date"].transform("last")
    result["future_close_year_end"] = year_groups["close"].transform("last")
    result["future_return_year_end"] = result["future_close_year_end"] / result["close"] - 1
    result["direction_year_end"] = (result["future_return_year_end"] > 0).astype("Float64")

    years = sorted(result["_label_year"].unique())
    complete = set(int(year) for year in complete_years) if complete_years is not None else set(years[:-1])
    result["maturity_year_end"] = result["_label_year"].isin(complete).map({True: "mature", False: "pending"})
    result.loc[~result["maturity_year_end"].eq("mature"), "future_return_year_end"] = pd.NA
    result.loc[~result["maturity_year_end"].eq("mature"), "direction_year_end"] = pd.NA
    return result.drop(columns=["_label_year"]).reset_index(drop=True)


def _target_columns(horizon: Horizon) -> tuple[str, str]:
    return f"direction_{_suffix(horizon)}", f"future_return_{_suffix(horizon)}"


def _year_end_splits(frame: pd.DataFrame, n_splits: int) -> list[tuple[pd.Index, pd.Index]]:
    years = sorted(pd.to_datetime(frame["date"]).dt.year.unique())
    splits: list[tuple[pd.Index, pd.Index]] = []
    for year in years[-n_splits:]:
        train_mask = pd.to_datetime(frame["date"]).dt.year < year
        test_mask = pd.to_datetime(frame["date"]).dt.year == year
        train_index = frame.index[train_mask]
        test_index = frame.index[test_mask]
        if len(train_index) and len(test_index):
            splits.append((train_index, test_index))
    return splits


def _splits(frame: pd.DataFrame, horizon: Horizon, config: ExperimentConfig) -> list[tuple[pd.Index, pd.Index]]:
    if horizon == "year_end":
        return _year_end_splits(frame, config.n_splits)
    numeric_horizon = int(horizon.removesuffix("d"))
    return list(
        walk_forward_splits(
            frame,
            n_splits=config.n_splits,
            test_size=config.test_size,
            purge=max(config.purge, numeric_horizon),
            min_train_dates=60,
        )
    )


def _mean_metric(folds: list[dict[str, object]], section: str, key: str) -> float | None:
    values = [
        float(fold[section][key])
        for fold in folds
        if fold.get(section, {}).get(key) is not None
    ]
    return float(sum(values) / len(values)) if values else None


def evaluate_candidate(
    frame: pd.DataFrame,
    *,
    horizon: Horizon,
    spec: CandidateSpec,
    config: ExperimentConfig,
) -> dict[str, object]:
    direction_column, return_column = _target_columns(horizon)
    columns = feature_group_columns(frame, spec.feature_group)
    if not columns:
        raise ValueError(f"No numeric features available for group {spec.feature_group}")

    folds: list[dict[str, object]] = []
    for train_index, test_index in _splits(frame, horizon, config):
        train = frame.loc[train_index]
        test = frame.loc[test_index]
        classifier = fit_model(
            train,
            feature_columns=columns,
            target_column=direction_column,
            task="classification",
            model_name=spec.classifier,
            random_state=config.random_state,
        )
        regressor = fit_model(
            train,
            feature_columns=columns,
            target_column=return_column,
            task="regression",
            model_name=spec.regressor,
            random_state=config.random_state,
        )
        class_prediction = predict_model(classifier, test)
        reg_prediction = predict_model(regressor, test)
        folds.append({
            "classification": classification_metrics(test[direction_column], class_prediction["probability_up"]),
            "regression": regression_metrics(test[return_column], reg_prediction["predicted_return"]),
            "train_rows": len(train),
            "test_rows": len(test),
        })

    return {
        "candidate": spec.name,
        "feature_group": spec.feature_group,
        "horizon": horizon,
        "folds": folds,
        "balanced_accuracy": _mean_metric(folds, "classification", "balanced_accuracy"),
        "roc_auc": _mean_metric(folds, "classification", "roc_auc"),
        "brier": _mean_metric(folds, "classification", "brier"),
        "mae": _mean_metric(folds, "regression", "mae"),
        "rank_ic": _mean_metric(folds, "regression", "rank_ic"),
    }


def select_top_candidates(evaluations: list[dict[str, object]], *, top_k: int) -> list[dict[str, object]]:
    return sorted(
        evaluations,
        key=lambda item: (
            item["balanced_accuracy"] is not None,
            item["balanced_accuracy"] if item["balanced_accuracy"] is not None else float("-inf"),
            item["roc_auc"] if item["roc_auc"] is not None else float("-inf"),
        ),
        reverse=True,
    )[:top_k]


def _fit_candidate(
    frame: pd.DataFrame,
    *,
    horizon: Horizon,
    spec: CandidateSpec,
    config: ExperimentConfig,
) -> tuple[FittedModel, FittedModel, list[str]]:
    direction_column, return_column = _target_columns(horizon)
    columns = feature_group_columns(frame, spec.feature_group)
    classifier = fit_model(
        frame,
        feature_columns=columns,
        target_column=direction_column,
        task="classification",
        model_name=spec.classifier,
        random_state=config.random_state,
    )
    regressor = fit_model(
        frame,
        feature_columns=columns,
        target_column=return_column,
        task="regression",
        model_name=spec.regressor,
        random_state=config.random_state,
    )
    return classifier, regressor, columns


def predict_roster(
    train: pd.DataFrame,
    prediction_frame: pd.DataFrame,
    *,
    horizon: Horizon,
    roster: list[dict[str, object]],
    config: ExperimentConfig,
    model_version: str,
    snapshot: str,
) -> pd.DataFrame:
    outputs: list[pd.DataFrame] = []
    specs = {spec.name: spec for spec in DEFAULT_CANDIDATES}
    for selected in roster:
        spec = specs[str(selected["candidate"])]
        classifier, regressor, _ = _fit_candidate(train, horizon=horizon, spec=spec, config=config)
        class_prediction = predict_model(classifier, prediction_frame)
        reg_prediction = predict_model(regressor, prediction_frame)
        output = prediction_frame[["ticker", "date"]].copy()
        output["candidate"] = spec.name
        output["probability_up"] = class_prediction["probability_up"]
        output["predicted_return"] = reg_prediction["predicted_return"]
        output["prediction_available"] = (
            class_prediction["prediction_available"].astype(bool)
            & reg_prediction["prediction_available"].astype(bool)
        )
        output["horizon"] = horizon
        output["model_version"] = model_version
        output["snapshot_id"] = snapshot
        outputs.append(output)

    if not outputs:
        return pd.DataFrame()
    model_outputs = pd.concat(outputs, ignore_index=True)
    ensemble = (
        model_outputs.groupby(["ticker", "date", "horizon"], as_index=False)
        .agg(
            probability_up=("probability_up", "mean"),
            predicted_return=("predicted_return", "mean"),
            model_count=("candidate", "nunique"),
            prediction_available=("prediction_available", "all"),
        )
    )
    ensemble["candidate"] = "ensemble"
    ensemble["model_version"] = model_version
    ensemble["snapshot_id"] = snapshot
    return pd.concat([model_outputs, ensemble], ignore_index=True)


def run_historical_replay(
    raw: pd.DataFrame,
    *,
    config: ExperimentConfig | None = None,
    artifact_root: Path | None = None,
    complete_years: Iterable[int] = range(2015, 2026),
) -> dict[str, object]:
    config = config or ExperimentConfig()
    features = build_feature_frame(raw)
    labelled = build_experiment_labels(features, complete_years=complete_years)
    dates = pd.to_datetime(labelled["date"])
    train = labelled[(dates >= config.train_start) & (dates <= config.train_end)].copy()
    test = labelled[dates >= config.test_start].copy()
    if train.empty or test.empty:
        raise ValueError("Historical replay requires non-empty train and test periods")

    snapshot = snapshot_id(config.train_end, config.test_start, tuple(features.columns), len(features))
    evaluations: dict[str, list[dict[str, object]]] = {}
    roster_manifest: dict[str, list[dict[str, object]]] = {}
    predictions: list[pd.DataFrame] = []

    for horizon in config.horizons:
        horizon_evaluations = [
            evaluate_candidate(train, horizon=horizon, spec=spec, config=config)
            for spec in DEFAULT_CANDIDATES
        ]
        roster = select_top_candidates(horizon_evaluations, top_k=config.top_k)
        evaluations[horizon] = horizon_evaluations
        roster_manifest[horizon] = roster
        predictions.append(
            predict_roster(
                train,
                test,
                horizon=horizon,
                roster=roster,
                config=config,
                model_version=f"replay-{horizon}-{snapshot}",
                snapshot=snapshot,
            )
        )

    prediction_frame = pd.concat(predictions, ignore_index=True)
    for horizon in config.horizons:
        direction_column, return_column = _target_columns(horizon)
        mask = prediction_frame["horizon"].eq(horizon) & prediction_frame["candidate"].eq("ensemble")
        actual = test[["ticker", "date", direction_column, return_column]].rename(
            columns={direction_column: "actual_direction", return_column: "actual_return"}
        )
        joined = prediction_frame.loc[mask, ["ticker", "date"]].merge(actual, on=["ticker", "date"], how="left")
        prediction_frame.loc[mask, "actual_direction"] = joined["actual_direction"].to_numpy()
        prediction_frame.loc[mask, "actual_return"] = joined["actual_return"].to_numpy()

    if artifact_root is not None:
        artifact_root.mkdir(parents=True, exist_ok=True)
        write_parquet(features, artifact_root / "replay_features.parquet", artifact_type="experiment_features", as_of=config.test_start, code_version="forecast-experiment-0.1.0")
        write_parquet(labelled, artifact_root / "replay_labels.parquet", artifact_type="experiment_labels", as_of=config.test_start, code_version="forecast-experiment-0.1.0")
        write_parquet(prediction_frame, artifact_root / "replay_predictions.parquet", artifact_type="experiment_predictions", as_of=config.test_start, code_version="forecast-experiment-0.1.0")
        (artifact_root / "replay_rosters.json").write_text(json.dumps(roster_manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        (artifact_root / "replay_evaluations.json").write_text(json.dumps(evaluations, ensure_ascii=False, indent=2), encoding="utf-8")
        write_markdown(
            artifact_root / "replay_summary.md",
            "Fixed-universe historical replay",
            {
                "Config": json.dumps(asdict(config), ensure_ascii=False, indent=2),
                "Result": f"train_rows={len(train)}; test_rows={len(test)}; prediction_rows={len(prediction_frame)}; snapshot_id={snapshot}",
            },
        )

    return {
        "features": features,
        "labels": labelled,
        "predictions": prediction_frame,
        "evaluations": evaluations,
        "rosters": roster_manifest,
        "snapshot_id": snapshot,
    }

