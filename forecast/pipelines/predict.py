from __future__ import annotations

import pandas as pd

from forecast.models.sklearn_models import FittedModel, predict_model


def build_predictions(frame: pd.DataFrame, model: FittedModel, *, horizon: int, model_version: str, snapshot_id: str) -> pd.DataFrame:
    predictions = predict_model(model, frame)
    predictions["horizon"] = horizon
    predictions["model_version"] = model_version
    predictions["snapshot_id"] = snapshot_id
    predictions["as_of"] = pd.to_datetime(frame["date"]).max().date().isoformat()
    return predictions
