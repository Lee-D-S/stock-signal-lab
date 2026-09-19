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
    if {"open", "high", "low", "close"}.issubset(result.columns):
        previous_close = groups["close"].shift(1)
        safe_open = result["open"].where(result["open"].ne(0))
        result["gap_return"] = result["open"] / previous_close - 1
        result["open_close_return"] = result["close"] / safe_open - 1
        result["candle_body_pct"] = (result["close"] - result["open"]) / result["close"]
        candle_top = result[["open", "close"]].max(axis=1)
        candle_bottom = result[["open", "close"]].min(axis=1)
        result["upper_wick_pct"] = (result["high"] - candle_top) / result["close"]
        result["lower_wick_pct"] = (candle_bottom - result["low"]) / result["close"]
        for window in (20, 60):
            rolling_high = groups["high"].transform(lambda values: values.rolling(window, min_periods=window).max())
            rolling_low = groups["low"].transform(lambda values: values.rolling(window, min_periods=window).min())
            rolling_range = (rolling_high - rolling_low).where(lambda values: values.ne(0))
            result[f"close_position_{window}d"] = (result["close"] - rolling_low) / rolling_range
            if window == 20:
                result["distance_to_high_20d"] = result["close"] / rolling_high - 1
                result["distance_from_low_20d"] = result["close"] / rolling_low - 1
    return result
