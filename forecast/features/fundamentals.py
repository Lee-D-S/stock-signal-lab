from __future__ import annotations

import pandas as pd


def add_fundamental_features(frame: pd.DataFrame, *, period_lag: int = 4) -> pd.DataFrame:
    result = frame.sort_values(["ticker", "date"]).copy()
    groups = result.groupby("ticker", sort=False)
    for column in ("revenue", "operating_income", "net_income", "assets", "equity", "eps", "bps"):
        if column not in result:
            continue
        result[column] = pd.to_numeric(result[column], errors="coerce")
        result[f"{column}_growth"] = groups[column].pct_change(period_lag)
        result[f"{column}_missing"] = result[column].isna().astype("int8")
    if {"revenue", "operating_income"}.issubset(result.columns):
        result["operating_margin"] = result["operating_income"] / result["revenue"].replace(0, pd.NA)
    if {"net_income", "equity"}.issubset(result.columns):
        result["roe"] = result["net_income"] / result["equity"].replace(0, pd.NA)
    return result
