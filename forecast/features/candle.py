from __future__ import annotations

import pandas as pd


def add_candle_features(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    candle_range = (result["high"] - result["low"]).replace(0, pd.NA)
    result["candle_body_pct"] = (result["close"] - result["open"]) / result["open"].replace(0, pd.NA)
    result["candle_range_pct"] = candle_range / result["open"].replace(0, pd.NA)
    result["upper_shadow_pct"] = (result["high"] - result[["open", "close"]].max(axis=1)) / result["open"]
    result["lower_shadow_pct"] = (result[["open", "close"]].min(axis=1) - result["low"]) / result["open"]
    result["close_position"] = (result["close"] - result["low"]) / candle_range
    return result
