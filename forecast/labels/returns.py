from __future__ import annotations

import pandas as pd


def add_return_labels(frame: pd.DataFrame, *, horizons: tuple[int, ...] = (1, 5, 20)) -> pd.DataFrame:
    result = frame.sort_values(["ticker", "date"]).copy()
    groups = result.groupby("ticker", sort=False)
    for horizon in horizons:
        result[f"future_close_{horizon}d"] = groups["close"].shift(-horizon)
        result[f"future_return_{horizon}d"] = result[f"future_close_{horizon}d"] / result["close"] - 1
        result[f"direction_{horizon}d"] = (result[f"future_return_{horizon}d"] > 0).astype("float")
        result.loc[result[f"future_return_{horizon}d"].isna(), f"direction_{horizon}d"] = pd.NA
    return result.reset_index(drop=True)
