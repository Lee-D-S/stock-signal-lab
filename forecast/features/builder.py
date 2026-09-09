from __future__ import annotations

import pandas as pd

from forecast.data.point_in_time import enforce_public_date
from forecast.data.schemas import validate_market_frame, validate_numeric_columns

from .candle import add_candle_features
from .fundamentals import add_fundamental_features
from .investor_flow import add_investor_flow_features
from .price_volume import add_price_volume_features
from .valuation import add_valuation_features


def build_numeric_features(frame: pd.DataFrame) -> pd.DataFrame:
    result = validate_market_frame(frame)
    result = enforce_public_date(result)
    validate_numeric_columns(result)
    result = add_price_volume_features(result)
    result = add_investor_flow_features(result)
    result = add_candle_features(result)
    result = add_fundamental_features(result)
    result = add_valuation_features(result)
    return result.sort_values(["ticker", "date"]).reset_index(drop=True)


def feature_columns(frame: pd.DataFrame) -> list[str]:
    excluded = {"ticker", "date", "public_at", "as_of", "close", "future_close_1d", "future_close_5d", "future_close_20d", "future_return_1d", "future_return_5d", "future_return_20d", "direction_1d", "direction_5d", "direction_20d"}
    return [str(column) for column in frame.columns if column not in excluded and pd.api.types.is_numeric_dtype(frame[column])]
