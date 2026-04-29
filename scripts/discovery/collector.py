"""급등/급락 사례 수집 및 IC 분석용 특징값 계산.

## 특징 추가 방법
    1. 아래 _FEATURES 리스트에 Feature(...) 항목 하나 추가
    2. 끝. analyzer.py와 report.py는 수정 불필요.

    예시:
        Feature(
            name="new_feat",
            description="새 특징 설명",
            direction=+1,            # +1=클수록 상승, -1=작을수록 상승, 0=무방향
            compute=lambda ind: ind.get("some_key"),
            screener_arg="--some-flag",   # screener.py CLI 옵션 (없으면 None)
        ),

## ind 딕셔너리 키 참조 (compute 함수 작성 시)
    calc_all(df) 반환값:
        ma5/20/60/120/240, macd_hist, macd_hist_prev,
        bb_upper, bb_width, bb_width_prev, rsi, stoch_k,
        obv_rising, vol_today, vol_ma20, vol_above_ma, fib_levels
    _compute_features() 가 추가 주입:
        close  — 해당 시점 종가 (항상 존재)
"""

import asyncio
import sys
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Callable

import pandas as pd

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from screener_lib.indicators import calc_all           # noqa: E402
from screener_lib.universe import get_stock_universe   # noqa: E402
from .data_loader import _API_DELAY, get_ohlcv_range   # noqa: E402

_MIN_ROWS = 60   # 지표 계산에 필요한 최소 데이터 행 수


@dataclass
class Feature:
    """IC 분석용 특징 하나를 표현하는 단위.

    compute(ind) -> float | None
        None = 필요한 데이터가 ind에 없음 (해당 특징은 분석에서 제외)
    """
    name:         str
    description:  str
    direction:    int            # +1 / -1 / 0
    compute:      Callable[[dict], float | None]
    screener_arg: str | None = None   # report.py 조건 후보 생성에 사용


# ── 복잡한 compute 함수 (람다로 표현하기 어려운 것들) ─────────────────────────

def _ma_align_short(ind: dict) -> float | None:
    ma5, ma20, ma60 = ind.get("ma5"), ind.get("ma20"), ind.get("ma60")
    if not (ma5 and ma20 and ma60):
        return None
    return float(ma5 > ma20 > ma60)


def _ma_align_long(ind: dict) -> float | None:
    ma60, ma120, ma240 = ind.get("ma60"), ind.get("ma120"), ind.get("ma240")
    if not (ma60 and ma120 and ma240):
        return None
    return float(ma60 > ma120 > ma240)


def _macd_cross_up(ind: dict) -> float | None:
    mh, mhp = ind.get("macd_hist"), ind.get("macd_hist_prev")
    if mh is None or mhp is None:
        return None
    return float(mh > 0 and mhp <= 0)


def _macd_rising(ind: dict) -> float | None:
    mh, mhp = ind.get("macd_hist"), ind.get("macd_hist_prev")
    if mh is None or mhp is None:
        return None
    return float(mh > mhp)


def _fib_support(ind: dict) -> float | None:
    close = ind.get("close")
    fibs  = ind.get("fib_levels") or []
    if close is None or not fibs:
        return None
    return float(any(abs(close - lv) / close <= 0.02 for lv in fibs))


# ── 특징 목록 ────────────────────────────────────────────────────────────────
# 새 특징 추가 시 여기에만 항목을 추가하면 된다.
# ─────────────────────────────────────────────────────────────────────────────
_FEATURES: list[Feature] = [

    # ── MA 기반 ───────────────────────────────────────────────────────────────
    Feature("price_vs_ma5",   "종가 vs MA5 비율",               +1,
            lambda ind: (ind["close"] / ind["ma5"]   - 1) if ind.get("ma5")   and ind["ma5"]   > 0 else None,
            "--ma-align 5,20"),
    Feature("price_vs_ma20",  "종가 vs MA20 비율",              +1,
            lambda ind: (ind["close"] / ind["ma20"]  - 1) if ind.get("ma20")  and ind["ma20"]  > 0 else None,
            "--ma-align 5,20"),
    Feature("price_vs_ma60",  "종가 vs MA60 비율",              +1,
            lambda ind: (ind["close"] / ind["ma60"]  - 1) if ind.get("ma60")  and ind["ma60"]  > 0 else None,
            "--ma-align 5,20,60"),
    Feature("price_vs_ma120", "종가 vs MA120 비율",             +1,
            lambda ind: (ind["close"] / ind["ma120"] - 1) if ind.get("ma120") and ind["ma120"] > 0 else None,
            "--ma-align 60,120"),
    Feature("price_vs_ma240", "종가 vs MA240 비율",             +1,
            lambda ind: (ind["close"] / ind["ma240"] - 1) if ind.get("ma240") and ind["ma240"] > 0 else None,
            "--ma-align 60,120,240"),
    Feature("ma_align_short", "단기 정배열 (MA5>MA20>MA60)",    +1,
            _ma_align_short, "--ma-align 5,20,60"),
    Feature("ma_align_long",  "장기 정배열 (MA60>MA120>MA240)", +1,
            _ma_align_long,  "--ma-align 60,120,240"),

    # ── MACD ──────────────────────────────────────────────────────────────────
    Feature("macd_hist_norm", "MACD 히스토그램 (종가 정규화)",  +1,
            lambda ind: (ind["macd_hist"] / ind["close"]) if ind.get("macd_hist") is not None else None,
            "--macd-positive"),
    Feature("macd_cross_up",  "MACD 골든크로스",                +1,
            _macd_cross_up, "--macd-cross-up"),
    Feature("macd_positive",  "MACD 히스토그램 > 0",           +1,
            lambda ind: float(ind["macd_hist"] > 0) if ind.get("macd_hist") is not None else None,
            "--macd-positive"),
    Feature("macd_rising",    "MACD 히스토그램 증가",          +1,
            _macd_rising, None),

    # ── 모멘텀 ────────────────────────────────────────────────────────────────
    Feature("rsi",     "RSI(14)",          -1,
            lambda ind: ind.get("rsi"),    "--rsi-max 50"),
    Feature("stoch_k", "스토캐스틱 %K",   -1,
            lambda ind: ind.get("stoch_k"), "--stoch-max 50"),

    # ── 수급 ──────────────────────────────────────────────────────────────────
    Feature("obv_rising", "OBV 상승 추세", +1,
            lambda ind: float(ind["obv_rising"]) if "obv_rising" in ind else None,
            "--obv-rising"),
    Feature("vol_ratio",  "거래량 / 20일 평균", +1,
            lambda ind: (
                ind["vol_today"] / ind["vol_ma20"]
                if ind.get("vol_today") and ind.get("vol_ma20") and ind["vol_ma20"] > 0
                else None
            ),
            "--vol-above-ma"),

    # ── 볼린저 ────────────────────────────────────────────────────────────────
    Feature("bb_position", "볼린저밴드 위치 (종가/상단)", +1,
            lambda ind: (
                ind["close"] / ind["bb_upper"]
                if ind.get("bb_upper") and ind["bb_upper"] > 0
                else None
            ),
            "--bb-breakout"),
    Feature("bb_squeeze",  "볼린저밴드 수축",             0,
            lambda ind: (
                float(ind["bb_width"] < ind["bb_width_prev"])
                if ind.get("bb_width") and ind.get("bb_width_prev")
                else None
            ),
            "--bb-squeeze"),
    Feature("bb_breakout", "볼린저 상단 돌파", +1,
            lambda ind: (
                float(ind["close"] > ind["bb_upper"])
                if ind.get("close") is not None and ind.get("bb_upper") is not None
                else None
            ),
            "--bb-breakout"),
    Feature("bb_above_mid", "종가 > MA20", +1,
            lambda ind: (
                float(ind["close"] > ind["ma20"])
                if ind.get("close") is not None and ind.get("ma20") is not None
                else None
            ),
            None),

    # ── 피보나치 ──────────────────────────────────────────────────────────────
    Feature("fib_support", "피보나치 지지선 근처", +1,
            _fib_support, "--fib-support"),
]

# ── 파생 상수 (직접 수정 불필요) ─────────────────────────────────────────────
FEATURE_META: dict[str, tuple[str, int]] = {f.name: (f.description, f.direction) for f in _FEATURES}
FEATURE_COLS: list[str]                  = [f.name for f in _FEATURES]


def _compute_features(df: pd.DataFrame) -> dict | None:
    """df 마지막 행 기준으로 IC 분석용 특징값 계산.

    df는 look-ahead bias 방지를 위해 T일까지만 슬라이스된 상태여야 함.
    """
    if len(df) < _MIN_ROWS:
        return None
    try:
        ind          = calc_all(df)
        ind["close"] = float(df["close"].iloc[-1])
        if ind["close"] <= 0:
            return None
        return {f.name: f.compute(ind) for f in _FEATURES}
    except Exception:
        return None


async def collect_samples(
    start: str,
    end: str,
    hold_days: int = 20,
    universe_by: str = "marcap",
    universe_to: int = 300,
    force_refresh: bool = False,
    step: int = 5,
) -> pd.DataFrame:
    """분석 기간 내 전체 종목의 (날짜, 특징값, 미래수익률) 레코드 수집.

    Returns:
        DataFrame columns: ticker, date, future_return, <FEATURE_COLS...>
    """
    buf_start = (pd.Timestamp(start) - timedelta(days=365)).strftime("%Y-%m-%d")
    start_ts  = pd.Timestamp(start)
    end_ts    = pd.Timestamp(end)

    print(f"[collector] 종목 유니버스 조회 중 ({universe_by} 상위 {universe_to}개)...")
    stocks = (await get_stock_universe(universe_by))[:universe_to]
    print(f"[collector] {len(stocks)}개 종목 수집 시작 | 기간: {start} ~ {end} | 보유일: {hold_days}일")

    all_records: list[dict] = []

    for i, stock in enumerate(stocks, 1):
        ticker = stock["ticker"]
        name   = stock.get("name", ticker)

        df = await get_ohlcv_range(ticker, buf_start, end, force_refresh=force_refresh)
        await asyncio.sleep(_API_DELAY)

        if df.empty or "date" not in df.columns:
            continue

        df = df.reset_index(drop=True)

        valid_idx = [
            idx for idx, row in df.iterrows()
            if start_ts <= row["date"] <= end_ts
            and idx + hold_days < len(df)
            and idx >= _MIN_ROWS - 1
        ][::step]

        for idx in valid_idx:
            feat = _compute_features(df.iloc[: idx + 1])
            if feat is None:
                continue

            close_t = df["close"].iloc[idx]
            record  = {
                "ticker":        ticker,
                "date":          df["date"].iloc[idx],
                "future_return": float((df["close"].iloc[idx + hold_days] - close_t) / close_t),
            }
            record.update(feat)
            all_records.append(record)

        if i % 20 == 0 or i == len(stocks):
            print(f"  [{i}/{len(stocks)}] {ticker} ({name}) - 누적 레코드: {len(all_records):,}")

    if not all_records:
        return pd.DataFrame()

    result = pd.DataFrame(all_records)
    print(f"[collector] 완료 - 총 {len(result):,}개 레코드")
    return result
