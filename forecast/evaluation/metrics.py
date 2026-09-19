from __future__ import annotations

import math

import pandas as pd
from sklearn.metrics import average_precision_score, balanced_accuracy_score, brier_score_loss, log_loss, mean_absolute_error, mean_squared_error, roc_auc_score


def classification_metrics(y_true: pd.Series, probability_up: pd.Series) -> dict[str, float | None]:
    frame = pd.DataFrame({"y": y_true, "p": probability_up}).dropna()
    if frame.empty:
        return {"balanced_accuracy": None, "roc_auc": None, "pr_auc": None, "brier": None, "log_loss": None, "calibration_error": None}
    y = frame["y"].astype(int)
    probability = frame["p"].astype(float)
    predicted = (probability >= .5).astype(int)
    bins = pd.cut(probability, bins=[-0.01, .1, .2, .3, .4, .5, .6, .7, .8, .9, 1.01], include_lowest=True)
    grouped = frame.assign(p=probability).groupby(bins, observed=False)
    calibration = (grouped["p"].mean() - grouped["y"].mean()).abs().dropna().mean()
    balanced = float((y == predicted).mean()) if y.nunique() < 2 else float(balanced_accuracy_score(y, predicted))
    return {
        "balanced_accuracy": balanced,
        "roc_auc": float(roc_auc_score(y, probability)) if y.nunique() > 1 else None,
        "pr_auc": float(average_precision_score(y, probability)) if y.nunique() > 1 else None,
        "brier": float(brier_score_loss(y, probability)),
        "log_loss": float(log_loss(y, probability, labels=[0, 1])),
        "calibration_error": float(calibration) if not math.isnan(float(calibration)) else None,
    }


def regression_metrics(y_true: pd.Series, predicted_return: pd.Series) -> dict[str, float | None]:
    frame = pd.DataFrame({"y": y_true, "p": predicted_return}).dropna()
    if frame.empty:
        return {"mae": None, "rmse": None, "rank_ic": None}
    rank_ic = frame["y"].corr(frame["p"], method="spearman") if len(frame) > 1 else None
    return {"mae": float(mean_absolute_error(frame["y"], frame["p"])), "rmse": float(mean_squared_error(frame["y"], frame["p"]) ** .5), "rank_ic": float(rank_ic) if pd.notna(rank_ic) else None}
