"""스코어 임계값 탐색.

Train 구간에서 스코어 구간별 수익률 통계를 계산해
조건을 만족하는 가장 낮은 스코어를 권장 임계값으로 제안한다.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pandas as pd

ROOT    = Path(__file__).parent.parent.parent
SCRIPTS = Path(__file__).parent.parent
for p in (str(ROOT), str(SCRIPTS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from discovery.data_loader import get_ohlcv_range      # noqa: E402
from screener_lib.indicators import calc_all            # noqa: E402
from screener_lib.universe import get_stock_universe    # noqa: E402
from .scorer import DEFAULT_WEIGHTS, compute_score      # noqa: E402

_MA240_BUFFER_DAYS = 365
_API_DELAY         = 0.35

DEFAULT_BINS: list[float] = [
    0.90, 0.85, 0.80, 0.75, 0.70, 0.675, 0.65,
    0.625, 0.60, 0.55, 0.50, 0.45, 0.40,
]


def _scan_ticker(
    ticker: str,
    df: pd.DataFrame,
    analysis_start: pd.Timestamp,
    analysis_end: pd.Timestamp,
    hold_days: int,
    step: int,
    weights: dict[str, float],
) -> list[dict]:
    """단일 종목의 (날짜, 스코어, 미래수익률) 레코드 목록."""
    # 분석 기간 내 유효 날짜 인덱스 목록
    valid_indices = [
        i for i, ts in enumerate(df["date"])
        if analysis_start <= ts <= analysis_end
    ]

    rows = []
    for pos, i in enumerate(valid_indices):
        if pos % step != 0:
            continue

        fut_idx = i + hold_days
        if fut_idx >= len(df):
            break

        close_t   = float(df["close"].iloc[i])
        close_fut = float(df["close"].iloc[fut_idx])
        if close_t <= 0:
            continue

        future_return = (close_fut - close_t) / close_t

        # look-ahead bias 방지: T일까지의 데이터만 사용
        slice_df = df.iloc[: i + 1]
        ind = calc_all(slice_df)
        ind["close"] = close_t
        score, _ = compute_score(ind, weights)

        rows.append({
            "ticker":        ticker,
            "date":          df["date"].iloc[i],
            "score":         score,
            "future_return": future_return,
        })

    return rows


async def compute_threshold_stats(
    start: str,
    end: str,
    hold_days: int = 20,
    universe_by: str = "marcap",
    universe_to: int = 300,
    force_refresh: bool = False,
    step: int = 5,
    weights: dict[str, float] | None = None,
    bins: list[float] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Train 구간 스코어 구간별 수익률 통계를 계산한다.

    Returns:
        raw_df:   (ticker, date, score, future_return)
        stats_df: 스코어 구간별 통계 테이블
    """
    if weights is None:
        weights = DEFAULT_WEIGHTS
    if bins is None:
        bins = DEFAULT_BINS

    print("[threshold] 유니버스 조회 중...")
    universe = await get_stock_universe(by=universe_by)
    tickers  = [s["ticker"] for s in universe[:universe_to]]
    print(f"[threshold] {len(tickers)}개 종목 분석 시작 ({start} ~ {end})")

    buf_start = (pd.Timestamp(start) - pd.Timedelta(days=_MA240_BUFFER_DAYS)).strftime("%Y-%m-%d")
    analysis_start = pd.Timestamp(start)
    analysis_end   = pd.Timestamp(end)

    all_records: list[dict] = []
    for idx, ticker in enumerate(tickers, 1):
        if idx % 50 == 0 or idx == len(tickers):
            print(f"[threshold] {idx}/{len(tickers)}: {ticker}  누적={len(all_records):,}")

        df = await get_ohlcv_range(ticker, buf_start, end, force_refresh=force_refresh)
        if df.empty or len(df) < hold_days + 50:
            await asyncio.sleep(_API_DELAY)
            continue

        rows = _scan_ticker(
            ticker, df, analysis_start, analysis_end,
            hold_days=hold_days, step=step, weights=weights,
        )
        all_records.extend(rows)
        await asyncio.sleep(_API_DELAY)

    if not all_records:
        return pd.DataFrame(), pd.DataFrame()

    raw_df   = pd.DataFrame(all_records)
    stats_df = compute_bin_stats(raw_df, bins)
    return raw_df, stats_df


def compute_bin_stats(
    raw_df: pd.DataFrame,
    bins: list[float] | None = None,
) -> pd.DataFrame:
    """스코어 구간별 수익률 통계 계산."""
    if bins is None:
        bins = DEFAULT_BINS

    rows = []
    for threshold in sorted(bins, reverse=True):
        sub = raw_df[raw_df["score"] >= threshold]
        n   = len(sub)
        if n == 0:
            continue

        avg_ret  = float(sub["future_return"].mean())
        win_rate = float((sub["future_return"] > 0).mean())
        gains    = sub.loc[sub["future_return"] > 0, "future_return"]
        losses   = sub.loc[sub["future_return"] < 0, "future_return"].abs()
        pf       = float(gains.mean() / losses.mean()) if len(gains) > 0 and len(losses) > 0 else None

        rows.append({
            "score_bin":      f">= {threshold:.0%}",
            "threshold":      threshold,
            "count":          n,
            "avg_return_pct": avg_ret * 100,
            "win_rate_pct":   win_rate * 100,
            "profit_factor":  pf,
        })

    return pd.DataFrame(rows)


def suggest_threshold(
    stats_df: pd.DataFrame,
    min_samples: int = 30,
    min_win_rate_pct: float = 50.0,
    min_return_pct: float = 0.23,   # 왕복 거래비용
    min_profit_factor: float = 1.5,
) -> float | None:
    """통계표에서 조건을 모두 만족하는 가장 낮은 임계값을 반환."""
    if stats_df.empty:
        return None

    mask = (
        (stats_df["count"]           >= min_samples) &
        (stats_df["win_rate_pct"]    >= min_win_rate_pct) &
        (stats_df["avg_return_pct"]  >= min_return_pct) &
        (stats_df["profit_factor"]   >= min_profit_factor)
    )
    ok = stats_df[mask]
    if ok.empty:
        return None
    return float(ok["threshold"].min())


def print_stats(stats_df: pd.DataFrame, hold_days: int) -> None:
    """스코어 구간별 통계 테이블을 콘솔에 출력한다."""
    print()
    print("=" * 65)
    print(f"[ 스코어 구간별 수익률 통계 ]  보유기간: {hold_days}거래일")
    print("=" * 65)
    print(f"{'스코어구간':>10}  {'샘플수':>6}  {'평균수익률':>10}  {'승률':>8}  {'손익비':>8}")
    print("-" * 65)

    for _, row in stats_df.iterrows():
        pf_str = f"{row['profit_factor']:.2f}" if row["profit_factor"] is not None else "  N/A"
        print(
            f"{row['score_bin']:>10}  {int(row['count']):>6}  "
            f"{row['avg_return_pct']:>+9.2f}%  {row['win_rate_pct']:>7.1f}%  {pf_str:>8}"
        )

    print()
    print("  임계값 기준: 평균수익률>0.23%, 승률>50%, 손익비>1.5, 샘플>30")
