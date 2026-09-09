from __future__ import annotations

import pandas as pd


def add_valuation_features(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.sort_values(["ticker", "date"]).copy()
    groups = result.groupby("ticker", sort=False)
    for column in ("per", "pbr", "psr", "dividend_yield"):
        if column not in result:
            continue
        result[column] = pd.to_numeric(result[column], errors="coerce")
        result[f"{column}_change_20d"] = groups[column].pct_change(20)
        result[f"{column}_missing"] = result[column].isna().astype("int8")
    return result
