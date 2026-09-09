from __future__ import annotations

import pandas as pd


def add_investor_flow_features(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    for column in ("foreign_net", "institution_net", "individual_net"):
        if column not in result:
            result[column] = pd.NA
        result[column] = pd.to_numeric(result[column], errors="coerce")
        result[f"{column}_missing"] = result[column].isna().astype("int8")
        if "volume" in result:
            result[f"{column}_to_volume"] = result[column] / result["volume"].abs().replace(0, pd.NA)
    result["foreign_institution_net"] = result["foreign_net"] + result["institution_net"]
    groups = result.groupby("ticker", sort=False)
    for column in ("foreign_net", "institution_net", "individual_net"):
        result[f"{column}_5d"] = groups[column].transform(lambda values: values.rolling(5, min_periods=5).sum())
        result[f"{column}_20d"] = groups[column].transform(lambda values: values.rolling(20, min_periods=20).sum())
    return result
