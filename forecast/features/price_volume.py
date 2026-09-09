from __future__ import annotations

import pandas as pd


def add_price_volume_features(frame: pd.DataFrame, *, windows: tuple[int, ...] = (1, 5, 20, 60, 120, 252)) -> pd.DataFrame:
    result = frame.sort_values(["ticker", "date"]).copy()
    groups = result.groupby("ticker", sort=False)
    result["daily_return"] = groups["close"].pct_change()
    for window in windows:
        result[f"return_{window}d"] = groups["close"].pct_change(window)
        result[f"volume_change_{window}d"] = groups["volume"].pct_change(window) if "volume" in result else pd.NA
        rolling_volatility = groups["daily_return"].transform(lambda values: values.rolling(window, min_periods=window).std())
        result[f"volatility_{window}d"] = rolling_volatility.fillna(0.0) if window == 1 else rolling_volatility
        result[f"close_ma_gap_{window}d"] = result["close"] / groups["close"].transform(lambda values: values.rolling(window, min_periods=window).mean()) - 1
    return result
