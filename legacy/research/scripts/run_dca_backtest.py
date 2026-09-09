"""ETF 시간 기반 DCA 백테스트.

매주(월요일 기준) vs 매월(첫 거래일) 고정 금액 분할 매수 전략을 시뮬레이션하고
Buy-and-Hold 대비 성과를 비교한다.

Usage:
    python scripts/run_dca_backtest.py --ticker 069500 --start 2019-01-01 --end 2024-12-31
    python scripts/run_dca_backtest.py --ticker 360750 --start 2021-06-01 --end 2024-12-31
    python scripts/run_dca_backtest.py --ticker 069500 360750
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date, timedelta
from pathlib import Path

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv
load_dotenv()

import pandas as pd

# 기본 백테스트 파라미터
WEEKLY_AMOUNT  = 250_000   # 주간 매수 금액 (원)
MONTHLY_AMOUNT = 1_000_000 # 월간 매수 금액 (원)

DEFAULT_PERIODS = {
    "069500": ("2019-01-01", "2024-12-31"),  # KODEX 200 — 코로나 폭락 포함
    "360750": ("2021-06-01", "2024-12-31"),  # TIGER 미국S&P500 — 상장 2달 후
}

OUT_DIR = Path(__file__).parent / "dca" / "results"


# ── 매수일 생성 ───────────────────────────────────────────────────────────────

def _weekly_dates(trading_days: list[date]) -> list[date]:
    """각 주의 첫 거래일 (월요일 우선, 휴장이면 그 주 첫 거래일)."""
    td_set = set(trading_days)
    result: list[date] = []
    seen_weeks: set[tuple[int, int]] = set()

    for d in sorted(trading_days):
        week_key = (d.isocalendar()[0], d.isocalendar()[1])  # (year, week)
        if week_key not in seen_weeks:
            seen_weeks.add(week_key)
            # 해당 주 월요일부터 금요일까지 중 첫 거래일
            monday = d - timedelta(days=d.weekday())
            for offset in range(5):
                candidate = monday + timedelta(days=offset)
                if candidate in td_set:
                    result.append(candidate)
                    break
    return result


def _monthly_dates(trading_days: list[date]) -> list[date]:
    """각 월의 첫 거래일."""
    result: list[date] = []
    seen_months: set[tuple[int, int]] = set()

    for d in sorted(trading_days):
        month_key = (d.year, d.month)
        if month_key not in seen_months:
            seen_months.add(month_key)
            result.append(d)
    return result


# ── 시뮬레이션 ────────────────────────────────────────────────────────────────

def _simulate(
    ohlcv: pd.DataFrame,
    buy_dates: list[date],
    amount_per_buy: int,
    label: str,
) -> tuple[pd.DataFrame, dict]:
    """DCA 시뮬레이션.

    Returns:
        (buy_log_df, metrics_dict)
    """
    ohlcv = ohlcv.copy()
    ohlcv["date"] = pd.to_datetime(ohlcv["date"]).dt.date

    price_map: dict[date, float] = dict(zip(ohlcv["date"], ohlcv["open"]))
    close_map: dict[date, float] = dict(zip(ohlcv["date"], ohlcv["close"]))

    # 마지막 거래일 종가 (평가 기준)
    last_date  = ohlcv["date"].iloc[-1]
    last_close = float(ohlcv["close"].iloc[-1])

    buy_log: list[dict] = []
    total_shares = 0.0
    total_invested = 0

    for d in buy_dates:
        if d not in price_map:
            continue
        open_price = price_map[d]
        if open_price <= 0:
            continue
        shares = amount_per_buy / open_price
        total_shares += shares
        total_invested += amount_per_buy
        buy_log.append({
            "buy_date":       d,
            "strategy":       label,
            "open_price":     round(open_price, 2),
            "shares_bought":  round(shares, 6),
            "amount":         amount_per_buy,
            "cum_invested":   total_invested,
            "cum_shares":     round(total_shares, 6),
            "cum_value":      round(total_shares * open_price, 0),
        })

    if not buy_log or total_shares == 0:
        return pd.DataFrame(), {}

    final_value    = total_shares * last_close
    total_return   = (final_value - total_invested) / total_invested * 100

    # CAGR
    first_buy = buy_log[0]["buy_date"]
    years      = (last_date - first_buy).days / 365.25
    cagr       = ((final_value / total_invested) ** (1 / years) - 1) * 100 if years > 0 else 0.0

    # MDD: 누적 평가금 기준
    log_df = pd.DataFrame(buy_log)
    # 매수일 사이 모든 거래일의 평가금 계산
    all_dates = [d for d in sorted(ohlcv["date"]) if d >= first_buy]
    values = [total_shares * close_map.get(d, last_close) for d in all_dates]
    # buy_log 기준 누적 수량으로 날짜별 평가금 재계산
    cum_shares_by_date: dict[date, float] = {}
    cs = 0.0
    buy_iter = iter(buy_log)
    next_buy = next(buy_iter, None)
    for d in all_dates:
        while next_buy and next_buy["buy_date"] <= d:
            cs = next_buy["cum_shares"]
            next_buy = next(buy_iter, None)
        if cs > 0:
            cum_shares_by_date[d] = cs

    eval_values = [cum_shares_by_date.get(d, 0) * close_map.get(d, 0) for d in all_dates]
    ev_series = pd.Series(eval_values)
    ev_series = ev_series[ev_series > 0]
    if not ev_series.empty:
        rolling_max = ev_series.cummax()
        drawdown    = (ev_series - rolling_max) / rolling_max * 100
        mdd         = float(drawdown.min())
    else:
        mdd = 0.0

    # 샤프 비율 (일간 수익률 기준, 무위험이자율 3%)
    eval_df = pd.DataFrame({"date": all_dates, "value": eval_values})
    eval_df = eval_df[eval_df["value"] > 0].copy()
    if len(eval_df) > 1:
        eval_df["ret"] = eval_df["value"].pct_change()
        daily_rf = 0.03 / 252
        excess   = eval_df["ret"].dropna() - daily_rf
        sharpe   = (excess.mean() / excess.std() * (252 ** 0.5)) if excess.std() > 0 else 0.0
    else:
        sharpe = 0.0

    avg_price = total_invested / total_shares

    metrics = {
        "strategy":        label,
        "buy_count":       len(buy_log),
        "total_invested":  total_invested,
        "final_value":     round(final_value, 0),
        "total_return_pct": round(total_return, 2),
        "cagr_pct":        round(cagr, 2),
        "mdd_pct":         round(mdd, 2),
        "sharpe":          round(sharpe, 3),
        "avg_price":       round(avg_price, 2),
    }
    return log_df, metrics


def _simulate_va(
    ohlcv: pd.DataFrame,
    monthly_dates: list[date],
    target_monthly: int,
    label: str = "value_averaging",
) -> tuple[pd.DataFrame, dict]:
    """Value Averaging: 매월 목표 평가금에 미달하면 그 차이만큼만 매수."""
    ohlcv = ohlcv.copy()
    ohlcv["date"] = pd.to_datetime(ohlcv["date"]).dt.date

    price_map: dict[date, float] = dict(zip(ohlcv["date"], ohlcv["open"]))
    close_map: dict[date, float] = dict(zip(ohlcv["date"], ohlcv["close"]))
    last_close = float(ohlcv["close"].iloc[-1])
    last_date  = ohlcv["date"].iloc[-1]

    total_shares   = 0.0
    total_invested = 0
    buy_log: list[dict] = []

    for month_num, d in enumerate(monthly_dates, 1):
        if d not in price_map:
            continue
        open_price = price_map[d]
        if open_price <= 0:
            continue

        target_value  = month_num * target_monthly
        current_value = total_shares * open_price
        buy_amount    = target_value - current_value

        if buy_amount <= 0:
            continue  # 목표 초과 시 매수 안 함 (매도 제외)

        shares = buy_amount / open_price
        total_shares   += shares
        total_invested += buy_amount
        buy_log.append({
            "buy_date":      d,
            "strategy":      label,
            "open_price":    round(open_price, 2),
            "target_value":  target_value,
            "current_value": round(current_value, 0),
            "buy_amount":    round(buy_amount, 0),
            "shares_bought": round(shares, 6),
            "cum_invested":  round(total_invested, 0),
            "cum_shares":    round(total_shares, 6),
        })

    if not buy_log or total_shares == 0:
        return pd.DataFrame(), {}

    final_value  = total_shares * last_close
    total_return = (final_value - total_invested) / total_invested * 100
    first_buy    = buy_log[0]["buy_date"]
    years        = (last_date - first_buy).days / 365.25
    cagr         = ((final_value / total_invested) ** (1 / years) - 1) * 100 if years > 0 else 0.0

    # MDD
    buy_dates_set = {r["buy_date"] for r in buy_log}
    all_dates = [d for d in sorted(ohlcv["date"]) if d >= first_buy]
    cs = 0.0
    buy_iter = iter(buy_log)
    next_buy = next(buy_iter, None)
    cum_by_date: dict[date, float] = {}
    for d in all_dates:
        while next_buy and next_buy["buy_date"] <= d:
            cs = next_buy["cum_shares"]
            next_buy = next(buy_iter, None)
        if cs > 0:
            cum_by_date[d] = cs

    ev = pd.Series([cum_by_date.get(d, 0) * close_map.get(d, 0) for d in all_dates])
    ev = ev[ev > 0]
    mdd = float(((ev - ev.cummax()) / ev.cummax() * 100).min()) if not ev.empty else 0.0

    ev_df = pd.DataFrame({"value": ev.values})
    ret   = ev_df["value"].pct_change().dropna()
    rf    = 0.03 / 252
    sharpe = ((ret - rf).mean() / (ret - rf).std() * 252 ** 0.5) if (ret - rf).std() > 0 else 0.0

    metrics = {
        "strategy":         label,
        "buy_count":        len(buy_log),
        "total_invested":   round(total_invested, 0),
        "final_value":      round(final_value, 0),
        "total_return_pct": round(total_return, 2),
        "cagr_pct":         round(cagr, 2),
        "mdd_pct":          round(mdd, 2),
        "sharpe":           round(sharpe, 3),
        "avg_price":        round(total_invested / total_shares, 2),
    }
    return pd.DataFrame(buy_log), metrics


def _bah_metrics(ohlcv: pd.DataFrame, total_invested: int, label: str = "buy_and_hold") -> dict:
    """Buy-and-Hold: 첫 거래일 시가에 total_invested 일시 투자."""
    ohlcv = ohlcv.copy()
    ohlcv["date"] = pd.to_datetime(ohlcv["date"]).dt.date

    first_open  = float(ohlcv["open"].iloc[0])
    last_close  = float(ohlcv["close"].iloc[-1])
    first_date  = ohlcv["date"].iloc[0]
    last_date   = ohlcv["date"].iloc[-1]

    if first_open <= 0:
        return {}

    shares        = total_invested / first_open
    final_value   = shares * last_close
    total_return  = (final_value - total_invested) / total_invested * 100
    years         = (last_date - first_date).days / 365.25
    cagr          = ((final_value / total_invested) ** (1 / years) - 1) * 100 if years > 0 else 0.0

    closes = ohlcv["close"].values
    cummax = pd.Series(closes).cummax()
    dd     = (pd.Series(closes) - cummax) / cummax * 100
    mdd    = float(dd.min())

    daily_ret = pd.Series(closes).pct_change().dropna()
    daily_rf  = 0.03 / 252
    excess    = daily_ret - daily_rf
    sharpe    = (excess.mean() / excess.std() * (252 ** 0.5)) if excess.std() > 0 else 0.0

    return {
        "strategy":        label,
        "buy_count":       1,
        "total_invested":  total_invested,
        "final_value":     round(final_value, 0),
        "total_return_pct": round(total_return, 2),
        "cagr_pct":        round(cagr, 2),
        "mdd_pct":         round(mdd, 2),
        "sharpe":          round(sharpe, 3),
        "avg_price":       round(first_open, 2),
    }


# ── 출력 ─────────────────────────────────────────────────────────────────────

def _print_metrics(ticker: str, rows: list[dict]) -> None:
    from tabulate import tabulate
    headers = ["전략", "매수횟수", "총투자(만원)", "최종평가(만원)", "수익률%", "CAGR%", "MDD%", "샤프", "평균단가"]
    table = []
    for r in rows:
        table.append([
            r["strategy"],
            r["buy_count"],
            f"{r['total_invested'] // 10000:,}",
            f"{int(r['final_value']) // 10000:,}",
            f"{r['total_return_pct']:+.2f}",
            f"{r['cagr_pct']:+.2f}",
            f"{r['mdd_pct']:.2f}",
            f"{r['sharpe']:.3f}",
            f"{r['avg_price']:,.0f}",
        ])
    print(f"\n[ {ticker} 성과 비교 ]")
    print(tabulate(table, headers=headers, tablefmt="simple"))


# ── 메인 ─────────────────────────────────────────────────────────────────────

async def run_ticker(ticker: str, start: str, end: str) -> list[dict]:
    from discovery.data_loader import get_ohlcv_range

    print(f"\n[dca] {ticker} OHLCV 로드 중... ({start} ~ {end})")
    ohlcv = await get_ohlcv_range(ticker, start, end)
    if ohlcv.empty:
        print(f"[dca] {ticker} 데이터 없음. 건너뜀.")
        return []

    ohlcv["date"] = pd.to_datetime(ohlcv["date"]).dt.date
    trading_days  = sorted(ohlcv["date"].tolist())
    print(f"[dca] {ticker} 거래일 {len(trading_days)}개 확보")

    weekly_days  = _weekly_dates(trading_days)
    monthly_days = _monthly_dates(trading_days)
    print(f"[dca] 매수일 — 주간: {len(weekly_days)}회 / 월간: {len(monthly_days)}회")

    all_metrics: list[dict] = []
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    period_label = f"{start[:4]}_{end[:4]}"

    for strategy, buy_dates, amount in [
        ("weekly",  weekly_days,  WEEKLY_AMOUNT),
        ("monthly", monthly_days, MONTHLY_AMOUNT),
    ]:
        log_df, metrics = _simulate(ohlcv, buy_dates, amount, strategy)
        if not metrics:
            continue
        metrics["ticker"] = ticker
        metrics["period"] = f"{start} ~ {end}"
        all_metrics.append(metrics)

        csv_path = OUT_DIR / f"dca_{ticker}_{strategy}_{period_label}.csv"
        log_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"[dca] {strategy} 매수 이력 저장: {csv_path.name}")

    # VA (Value Averaging) — 월간과 같은 목표 금액 기준
    va_log, va_metrics = _simulate_va(ohlcv, monthly_days, MONTHLY_AMOUNT, "value_averaging")
    if va_metrics:
        va_metrics["ticker"] = ticker
        va_metrics["period"] = f"{start} ~ {end}"
        all_metrics.append(va_metrics)
        va_csv = OUT_DIR / f"dca_{ticker}_va_{period_label}.csv"
        va_log.to_csv(va_csv, index=False, encoding="utf-8-sig")
        print(f"[dca] value_averaging 매수 이력 저장: {va_csv.name}")

    # Buy-and-Hold는 월간과 같은 총투자금 기준
    monthly_metrics = next((m for m in all_metrics if m["strategy"] == "monthly"), None)
    if monthly_metrics:
        bah = _bah_metrics(ohlcv, monthly_metrics["total_invested"])
        if bah:
            bah["ticker"] = ticker
            bah["period"] = f"{start} ~ {end}"
            all_metrics.append(bah)

    _print_metrics(ticker, all_metrics)
    return all_metrics


async def main() -> None:
    parser = argparse.ArgumentParser(description="ETF DCA 백테스트 (주간 vs 월간)")
    parser.add_argument("--ticker", nargs="+", default=list(DEFAULT_PERIODS.keys()),
                        help="ETF 티커 (기본: 069500 360750)")
    parser.add_argument("--start", default=None, help="시작일 YYYY-MM-DD (기본: 티커별 자동)")
    parser.add_argument("--end",   default=None, help="종료일 YYYY-MM-DD (기본: 티커별 자동)")
    args = parser.parse_args()

    all_results: list[dict] = []

    for ticker in args.ticker:
        default_start, default_end = DEFAULT_PERIODS.get(ticker, ("2020-01-01", "2024-12-31"))
        start = args.start or default_start
        end   = args.end   or default_end
        results = await run_ticker(ticker, start, end)
        all_results.extend(results)

    if not all_results:
        print("[dca] 결과 없음.")
        return

    summary_cols = ["ticker", "strategy", "period", "buy_count",
                    "total_invested", "final_value", "total_return_pct",
                    "cagr_pct", "mdd_pct", "sharpe", "avg_price"]
    summary_df = pd.DataFrame(all_results, columns=summary_cols)
    summary_path = OUT_DIR / "dca_summary.csv"
    summary_df.to_csv(summary_path, index=False, encoding="utf-8-sig")
    print(f"\n[dca] 전체 요약 저장: {summary_path}")


if __name__ == "__main__":
    asyncio.run(main())
