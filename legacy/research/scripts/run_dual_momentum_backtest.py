"""듀얼 모멘텀 백테스트 (Gary Antonacci 방식).

매월 두 ETF의 12개월 수익률을 비교해 더 높은 쪽을 100% 보유.
둘 다 마이너스면 현금(안전자산) 보유.

기본 유니버스:
  069500  KODEX 200          (국내 주식)
  360750  TIGER 미국S&P500   (해외 주식)
  안전자산: 현금 (수익률 0%)

기본 기간: 2022-06-01 ~ 2024-12-31
  (360750 상장 2021-04 + 12개월 룩백 안정화)

Usage:
    python legacy/research/scripts/run_dual_momentum_backtest.py
    python legacy/research/scripts/run_dual_momentum_backtest.py --capital 20000000
    python legacy/research/scripts/run_dual_momentum_backtest.py --lookback 6
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date
from pathlib import Path

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv
load_dotenv()

import pandas as pd

OUT_DIR = Path(__file__).parent / "dca" / "results"

RISKY_ASSETS = ["069500", "360750"]
DEFAULT_START = "2022-06-01"
DEFAULT_END   = "2024-12-31"
DEFAULT_CAPITAL = 10_000_000  # 1,000만원


# ── 헬퍼 ─────────────────────────────────────────────────────────────────────

def _monthly_starts(trading_days: list[date]) -> list[date]:
    """각 월의 첫 거래일."""
    seen: set[tuple[int, int]] = set()
    result = []
    for d in sorted(trading_days):
        k = (d.year, d.month)
        if k not in seen:
            seen.add(k)
            result.append(d)
    return result


def _momentum_return(price_series: pd.Series, as_of: date, lookback_months: int) -> float | None:
    """as_of 기준 lookback_months 전 대비 수익률."""
    price_series = price_series.sort_index()
    if as_of not in price_series.index:
        # 가장 가까운 이전 거래일 사용
        prior = price_series[price_series.index <= as_of]
        if prior.empty:
            return None
        as_of_price = float(prior.iloc[-1])
        as_of_real  = prior.index[-1]
    else:
        as_of_price = float(price_series[as_of])
        as_of_real  = as_of

    # lookback_months 전 날짜 근사
    target_year  = as_of_real.year - (lookback_months // 12)
    target_month = as_of_real.month - (lookback_months % 12)
    if target_month <= 0:
        target_month += 12
        target_year  -= 1
    # 해당 월의 가장 가까운 거래일
    candidates = price_series[
        (price_series.index.year  == target_year) &
        (price_series.index.month == target_month)
    ]
    if candidates.empty:
        return None
    past_price = float(candidates.iloc[-1])
    if past_price == 0:
        return None
    return (as_of_price - past_price) / past_price


def _calc_metrics(
    daily_values: list[tuple[date, float]],
    total_invested: int,
    label: str,
) -> dict:
    if not daily_values:
        return {}
    values = pd.Series([v for _, v in daily_values])
    final  = float(values.iloc[-1])
    first_d, last_d = daily_values[0][0], daily_values[-1][0]
    years  = (last_d - first_d).days / 365.25

    total_return = (final - total_invested) / total_invested * 100
    cagr = ((final / total_invested) ** (1 / years) - 1) * 100 if years > 0 else 0.0

    cummax   = values.cummax()
    mdd      = float(((values - cummax) / cummax * 100).min())

    ret    = values.pct_change().dropna()
    rf     = 0.03 / 252
    excess = ret - rf
    sharpe = (excess.mean() / excess.std() * 252 ** 0.5) if excess.std() > 0 else 0.0

    return {
        "strategy":         label,
        "total_invested":   total_invested,
        "final_value":      round(final, 0),
        "total_return_pct": round(total_return, 2),
        "cagr_pct":         round(cagr, 2),
        "mdd_pct":          round(mdd, 2),
        "sharpe":           round(sharpe, 3),
    }


# ── 시뮬레이션 ────────────────────────────────────────────────────────────────

def simulate_dual_momentum(
    ohlcv_map: dict[str, pd.DataFrame],
    start: str,
    end: str,
    capital: int,
    lookback: int,
) -> tuple[pd.DataFrame, dict]:
    """듀얼 모멘텀 시뮬레이션.

    매월 초 모멘텀을 체크해 포지션을 결정하고,
    해당 월 첫 거래일 시가에 매매를 집행한다.
    """
    # 공통 거래일 구성
    all_dates: set[date] = set()
    price_maps: dict[str, dict[date, float]] = {}
    close_maps: dict[str, dict[date, float]] = {}
    momentum_series: dict[str, pd.Series] = {}

    for ticker, df in ohlcv_map.items():
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"]).dt.date
        all_dates |= set(df["date"])
        price_maps[ticker]     = dict(zip(df["date"], df["open"]))
        close_maps[ticker]     = dict(zip(df["date"], df["close"]))
        momentum_series[ticker] = pd.Series(
            dict(zip(df["date"], df["close"]))
        )
        momentum_series[ticker].index = pd.to_datetime(momentum_series[ticker].index)

    trading_days  = sorted(d for d in all_dates if start <= str(d) <= end)
    monthly_starts = _monthly_starts(trading_days)

    portfolio_cash   = float(capital)
    portfolio_shares = 0.0
    portfolio_ticker: str | None = None  # 현재 보유 티커 (None = 현금)

    rebalance_log: list[dict] = []
    daily_values:  list[tuple[date, float]] = []

    for i, buy_date in enumerate(monthly_starts):
        # ── 모멘텀 체크 (전월 마지막 거래일 종가 기준) ──
        prev_close_date = buy_date
        m_scores: dict[str, float | None] = {}
        for ticker in RISKY_ASSETS:
            m = _momentum_return(momentum_series[ticker], prev_close_date, lookback)
            m_scores[ticker] = m

        # 절대 모멘텀: 최고 수익률이 0 미만이면 현금
        valid = {t: m for t, m in m_scores.items() if m is not None}
        if not valid or max(valid.values()) < 0:
            target_ticker = None  # 현금
        else:
            target_ticker = max(valid, key=lambda t: valid[t])

        # ── 포지션 변경 필요 여부 ──
        if target_ticker == portfolio_ticker:
            pass  # 유지
        else:
            # 기존 포지션 청산 (시가)
            if portfolio_ticker is not None and portfolio_shares > 0:
                sell_price = price_maps[portfolio_ticker].get(buy_date)
                if sell_price:
                    portfolio_cash   = portfolio_shares * sell_price
                    portfolio_shares = 0.0

            # 신규 포지션 진입 (시가)
            if target_ticker is not None:
                buy_price = price_maps[target_ticker].get(buy_date)
                if buy_price and buy_price > 0:
                    portfolio_shares = portfolio_cash / buy_price
                    portfolio_cash   = 0.0
                    portfolio_ticker = target_ticker
                else:
                    portfolio_ticker = None  # 가격 없으면 현금 유지
            else:
                portfolio_ticker = None

            rebalance_log.append({
                "date":          buy_date,
                "target":        target_ticker or "cash",
                "momentum_069500": round(m_scores.get("069500") or 0, 4),
                "momentum_360750": round(m_scores.get("360750") or 0, 4),
                "portfolio_cash":   round(portfolio_cash, 0),
                "portfolio_shares": round(portfolio_shares, 6),
            })

        # ── 이 달 일별 평가금 추적 ──
        next_month_start = monthly_starts[i + 1] if i + 1 < len(monthly_starts) else None
        month_days = [
            d for d in trading_days
            if buy_date <= d < (next_month_start or date(9999, 1, 1))
        ]
        for d in month_days:
            if portfolio_ticker and portfolio_shares > 0:
                close = close_maps[portfolio_ticker].get(d, 0)
                val   = portfolio_shares * close
            else:
                val = portfolio_cash
            daily_values.append((d, val))

    if not daily_values:
        return pd.DataFrame(), {}

    log_df  = pd.DataFrame(rebalance_log)
    metrics = _calc_metrics(daily_values, capital, "dual_momentum")
    return log_df, metrics


def simulate_bah(
    ohlcv: pd.DataFrame,
    capital: int,
    ticker: str,
    start: str,
    end: str,
) -> dict:
    """단순 BaH 기준."""
    df = ohlcv.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[(df["date"].astype(str) >= start) & (df["date"].astype(str) <= end)]
    if df.empty:
        return {}
    first_open = float(df["open"].iloc[0])
    if first_open <= 0:
        return {}
    shares = capital / first_open
    daily_values = [(row["date"], shares * row["close"]) for _, row in df.iterrows()]
    metrics = _calc_metrics(daily_values, capital, f"bah_{ticker}")
    return metrics


# ── 출력 ─────────────────────────────────────────────────────────────────────

def _print_results(all_metrics: list[dict]) -> None:
    from tabulate import tabulate
    headers = ["전략", "총투자(만원)", "최종평가(만원)", "수익률%", "CAGR%", "MDD%", "샤프"]
    table = [
        [
            r["strategy"],
            f"{r['total_invested'] // 10000:,}",
            f"{int(r['final_value']) // 10000:,}",
            f"{r['total_return_pct']:+.2f}",
            f"{r['cagr_pct']:+.2f}",
            f"{r['mdd_pct']:.2f}",
            f"{r['sharpe']:.3f}",
        ]
        for r in all_metrics
    ]
    print("\n[ 듀얼 모멘텀 vs Buy-and-Hold 비교 ]")
    print(tabulate(table, headers=headers, tablefmt="simple"))


# ── 메인 ─────────────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(description="듀얼 모멘텀 ETF 백테스트")
    parser.add_argument("--start",    default=DEFAULT_START)
    parser.add_argument("--end",      default=DEFAULT_END)
    parser.add_argument("--capital",  type=int, default=DEFAULT_CAPITAL,
                        help="초기 투자금 (원, 기본: 10,000,000)")
    parser.add_argument("--lookback", type=int, default=12,
                        help="모멘텀 룩백 기간 (개월, 기본: 12)")
    args = parser.parse_args()

    from discovery.data_loader import get_ohlcv_range

    # 룩백 기간만큼 앞당겨 로드
    load_start = f"{int(args.start[:4]) - 1}-{args.start[5:]}"

    print(f"[dm] OHLCV 로드 중... ({args.start} ~ {args.end}, lookback={args.lookback}개월)")
    ohlcv_map: dict[str, pd.DataFrame] = {}
    for ticker in RISKY_ASSETS:
        df = await get_ohlcv_range(ticker, load_start, args.end)
        if not df.empty:
            ohlcv_map[ticker] = df
            print(f"[dm] {ticker} {len(df)}거래일 확보")
        else:
            print(f"[dm] {ticker} 데이터 없음")

    if len(ohlcv_map) < 2:
        print("[dm] 유니버스 데이터 부족. 종료.")
        return

    log_df, dm_metrics = simulate_dual_momentum(
        ohlcv_map, args.start, args.end, args.capital, args.lookback
    )

    all_metrics = []
    if dm_metrics:
        dm_metrics["period"] = f"{args.start} ~ {args.end}"
        all_metrics.append(dm_metrics)

    for ticker in RISKY_ASSETS:
        if ticker in ohlcv_map:
            bah = simulate_bah(ohlcv_map[ticker], args.capital, ticker, args.start, args.end)
            if bah:
                bah["period"] = f"{args.start} ~ {args.end}"
                all_metrics.append(bah)

    _print_results(all_metrics)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    period_label = f"{args.start[:4]}_{args.end[:4]}"

    if not log_df.empty:
        log_path = OUT_DIR / f"dual_momentum_rebalance_{period_label}.csv"
        log_df.to_csv(log_path, index=False, encoding="utf-8-sig")
        print(f"[dm] 리밸런싱 이력 저장: {log_path.name}")

    if all_metrics:
        summary = pd.DataFrame(all_metrics)
        summary_path = OUT_DIR / f"dual_momentum_summary_{period_label}.csv"
        summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
        print(f"[dm] 요약 저장: {summary_path.name}")


if __name__ == "__main__":
    asyncio.run(main())
