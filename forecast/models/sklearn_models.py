from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor, RandomForestClassifier, RandomForestRegressor
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

Task = Literal["classification", "regression"]


@dataclass(frozen=True)
class FittedModel:
    name: str
    task: Task
    feature_columns: tuple[str, ...]
    estimator: object
    rows: int


def make_estimator(name: str, task: Task, *, random_state: int = 42) -> object:
    if task == "classification":
        factories = {
            "logistic": lambda: make_pipeline(StandardScaler(), LogisticRegression(max_iter=1_000, random_state=random_state)),
            "random_forest": lambda: RandomForestClassifier(n_estimators=200, min_samples_leaf=10, n_jobs=-1, random_state=random_state),
            "hist_gradient_boosting": lambda: HistGradientBoostingClassifier(max_iter=200, random_state=random_state),
        }
    else:
        factories = {
            "ridge": lambda: make_pipeline(StandardScaler(), Ridge(alpha=1.0)),
            "random_forest": lambda: RandomForestRegressor(n_estimators=200, min_samples_leaf=10, n_jobs=-1, random_state=random_state),
            "hist_gradient_boosting": lambda: HistGradientBoostingRegressor(max_iter=200, random_state=random_state),
        }
    try:
        return factories[name]()
    except KeyError as exc:
        raise ValueError(f"Unsupported {task} model: {name}") from exc


def fit_model(frame: pd.DataFrame, *, feature_columns: list[str], target_column: str, task: Task, model_name: str, random_state: int = 42) -> FittedModel:
    training = frame.dropna(subset=feature_columns + [target_column]).copy()
    if training.empty:
        raise ValueError(f"No complete rows available for {target_column}")
    if task == "classification" and training[target_column].nunique() < 2:
        raise ValueError(f"Classification target has fewer than two classes: {target_column}")
    estimator = make_estimator(model_name, task, random_state=random_state)
    estimator.fit(training[feature_columns], training[target_column])
    return FittedModel(model_name, task, tuple(feature_columns), estimator, len(training))


def predict_model(model: FittedModel, frame: pd.DataFrame) -> pd.DataFrame:
    result = frame[["ticker", "date"]].copy()
    complete = frame[list(model.feature_columns)].notna().all(axis=1)
    result["prediction_available"] = complete
    if model.task == "classification":
        result["probability_up"] = pd.NA
        if complete.any():
            result.loc[complete, "probability_up"] = model.estimator.predict_proba(frame.loc[complete, list(model.feature_columns)])[:, 1]
    else:
        result["predicted_return"] = pd.NA
        if complete.any():
            result.loc[complete, "predicted_return"] = model.estimator.predict(frame.loc[complete, list(model.feature_columns)])
    return result
