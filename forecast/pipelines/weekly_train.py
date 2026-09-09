from __future__ import annotations

import pandas as pd

from forecast.evaluation.metrics import classification_metrics, regression_metrics
from forecast.evaluation.walk_forward import walk_forward_splits
from forecast.features.builder import feature_columns
from forecast.models.sklearn_models import fit_model, predict_model


def train_and_evaluate(frame: pd.DataFrame, *, horizon: int, model_name: str, n_splits: int = 3, test_size: int = 20, purge: int | None = None) -> dict[str, object]:
    columns = feature_columns(frame)
    fold_metrics: list[dict[str, object]] = []
    for train_index, test_index in walk_forward_splits(frame, n_splits=n_splits, test_size=test_size, purge=purge if purge is not None else horizon):
        train, test = frame.loc[train_index], frame.loc[test_index]
        classifier = fit_model(train, feature_columns=columns, target_column=f"direction_{horizon}d", task="classification", model_name=model_name)
        regressor = fit_model(train, feature_columns=columns, target_column=f"future_return_{horizon}d", task="regression", model_name="ridge" if model_name == "logistic" else model_name)
        classification_prediction = predict_model(classifier, test)
        regression_prediction = predict_model(regressor, test)
        fold_metrics.append({
            "classification": classification_metrics(test[f"direction_{horizon}d"], classification_prediction["probability_up"]),
            "regression": regression_metrics(test[f"future_return_{horizon}d"], regression_prediction["predicted_return"]),
            "train_rows": len(train), "test_rows": len(test),
        })
    return {"horizon": horizon, "model": model_name, "feature_count": len(columns), "folds": fold_metrics}
