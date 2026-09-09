"""지표 사전 벡터화 계산 — 백테스트 성능 최적화.

screener_lib/indicators/ 의 각 calculate() 가 .iloc[-1] 로 마지막 값만 반환하는
것과 달리, 전체 기간을 한 번에 계산해 date-indexed DataFrame 으로 반환한다.
engine.py 에서 특정 날짜의 행을 row_to_ind() 로 변환하면 check_all() 에 그대로 사용
가능하다.
"""

import pandas as pd
import pandas_ta as ta

_FIB_RATIOS   = [0.236, 0.382, 0.5, 0.618, 0.786]
_FIB_LOOKBACK = 60

_FLOAT_KEYS = [
    "close",
    "ma5", "ma20", "ma60", "ma120", "ma240",
    "rsi",
    "macd_hist", "macd_hist_prev",
    "bb_upper", "bb_width", "bb_width_prev",
    "stoch_k",
    "vol_today", "vol_ma20",
]
_BOOL_KEYS = ["obv_rising", "vol_above_ma"]
_FIB_COLS  = [f"fib_{r}" for r in _FIB_RATIOS]


def precompute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """전체 기간 지표를 벡터화 방식으로 한 번에 계산.

    Args:
        df: columns = date, open, high, low, close, volume (RangeIndex)

    Returns:
        date-indexed DataFrame with all indicator columns.
    """
    close  = df["close"]
    high   = df["high"]
    low    = df["low"]
    volume = df["volume"]

    r = pd.DataFrame(index=df.index)
    r["date"]  = df["date"].values
    r["close"] = close.values

    # ── MA ──────────────────────────────────────────────────────────────
    for p in [5, 20, 60, 120, 240]:
        r[f"ma{p}"] = close.rolling(p).mean().values

    # ── RSI ─────────────────────────────────────────────────────────────
    rsi_s = ta.rsi(close, length=14)
    r["rsi"] = rsi_s.values if rsi_s is not None else float("nan")

    # ── MACD histogram ──────────────────────────────────────────────────
    macd_df = ta.macd(close, fast=12, slow=26, signal=9)
    if macd_df is not None and not macd_df.empty:
        col_h = next((c for c in macd_df.columns if c.startswith("MACDh")), None)
        if col_h:
            r["macd_hist"]      = macd_df[col_h].values
            r["macd_hist_prev"] = macd_df[col_h].shift(1).values
        else:
            r["macd_hist"] = r["macd_hist_prev"] = float("nan")
    else:
        r["macd_hist"] = r["macd_hist_prev"] = float("nan")

    # ── Bollinger Bands ──────────────────────────────────────────────────
    bb = ta.bbands(close, length=20, std=2)
    if bb is not None and not bb.empty:
        col_u = next((c for c in bb.columns if c.startswith("BBU")), None)
        col_b = next((c for c in bb.columns if c.startswith("BBB")), None)
        r["bb_upper"]      = bb[col_u].values if col_u else float("nan")
        r["bb_width"]      = bb[col_b].values if col_b else float("nan")
        r["bb_width_prev"] = bb[col_b].shift(5).values if col_b else float("nan")
    else:
        r["bb_upper"] = r["bb_width"] = r["bb_width_prev"] = float("nan")

    # ── OBV ─────────────────────────────────────────────────────────────
    obv = ta.obv(close, volume)
    if obv is not None and not obv.empty:
        obv_ma5  = obv.rolling(5).mean()
        obv_ma20 = obv.rolling(20).mean()
        r["obv_rising"] = (obv_ma5 > obv_ma20).values
    else:
        r["obv_rising"] = False

    # ── Stochastic ───────────────────────────────────────────────────────
    stoch = ta.stoch(high, low, close, k=14, d=3, smooth_k=3)
    if stoch is not None and not stoch.empty:
        col_k = next((c for c in stoch.columns if "STOCHk" in c), None)
        r["stoch_k"] = stoch[col_k].values if col_k else float("nan")
    else:
        r["stoch_k"] = float("nan")

    # ── Volume ──────────────────────────────────────────────────────────
    vol_ma20 = volume.rolling(20).mean()
    r["vol_today"]    = volume.values
    r["vol_ma20"]     = vol_ma20.values
    r["vol_above_ma"] = (volume > vol_ma20).values

    # ── Fibonacci (rolling 60-day high/low) ─────────────────────────────
    hi60 = high.rolling(_FIB_LOOKBACK, min_periods=_FIB_LOOKBACK).max()
    lo60 = low.rolling(_FIB_LOOKBACK,  min_periods=_FIB_LOOKBACK).min()
    rng  = hi60 - lo60
    for ratio in _FIB_RATIOS:
        r[f"fib_{ratio}"] = (lo60 + rng * (1 - ratio)).values

    return r.set_index("date")


def row_to_ind(row: pd.Series) -> dict:
    """Pre-computed row → ind dict (screener_lib check_all 호환)."""
    ind: dict = {}

    for key in _FLOAT_KEYS:
        val = row.get(key)
        ind[key] = None if (val is None or pd.isna(val)) else float(val)

    for key in _BOOL_KEYS:
        val = row.get(key, False)
        try:
            ind[key] = bool(val)
        except (ValueError, TypeError):
            ind[key] = False

    fib_levels = []
    for col in _FIB_COLS:
        val = row.get(col)
        if val is not None and not pd.isna(val):
            fib_levels.append(float(val))
    ind["fib_levels"] = fib_levels

    return ind
