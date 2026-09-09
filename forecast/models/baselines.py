from __future__ import annotations

import pandas as pd


def majority_probability(target: pd.Series) -> float:
    clean = pd.to_numeric(target, errors="coerce").dropna()
    return float(clean.mean()) if not clean.empty else 0.5


def mean_return(target: pd.Series) -> float:
    clean = pd.to_numeric(target, errors="coerce").dropna()
    return float(clean.mean()) if not clean.empty else 0.0


def baseline_predictions(train: pd.DataFrame, test: pd.DataFrame, *, horizon: int) -> pd.DataFrame:
    return pd.DataFrame({
        "ticker": test["ticker"].values,
        "date": test["date"].values,
        "probability_up": majority_probability(train[f"direction_{horizon}d"]),
        "predicted_return": mean_return(train[f"future_return_{horizon}d"]),
    })
