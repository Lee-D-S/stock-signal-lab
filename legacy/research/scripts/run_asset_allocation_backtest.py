"""정적 자산배분 백테스트 (Static Asset Allocation).

설정한 목표 비중으로 초기 매수 후, 분기마다 비중을 원복(리밸런싱)한다.
리밸런싱 없이 보유했을 경우(BaH)와 성과를 비교한다.

기본 유니버스:
  069500  KODEX 200          (국내 주식) — 목표 비중 50%
  360750  TIGER 미국S&P500   (해외 주식) — 목표 비중 50%

기본 기간: 2021-06-01 ~ 2024-12-31
  (360750 상장 2021-04 + 데이터 안정화)

Usage:
    python scripts/run_asset_allocation_backtest.py
    python scripts/run_asset_allocation_backtest.py --capital 20000000
    python scripts/run_asset_allocation_backtest.py --rebalance-freq quarterly
    python scripts/run_asset_allocation_backtest.py --weights 069500=0.6,360750=0.4
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

DEFAULT_ASSETS   = {"069500": 0.5, "360750": 0.5}
DEFAULT_START    = "2021-06-01"
DEFAULT_END      = "2024-12-31"
DEFAULT_CAPITAL  = 10_000_000   # 1,000만원
REBALANCE_FREQS  = {"quarterly": 3, "monthly": 1, "semiannual": 6, "annual": 12}


# ── 헬퍼 ─────────────────────────────────────────────────────────────────────

def _trading_days_in_range(ohlcv_map: dict[str, pd.DataFrame], start: str, end: str) -> list[date]:
    """모든 티커의 거래일 합집합. 누락 종가는 직전 값으로 전진 보정."""
    all_dates: set[date] = set()
    for df in ohlcv_map.values():
        df2 = df.copy()
        df2["date"] = pd.to_datetime(df2["date"]).dt.date
        all_dates |= set(df2["date"])
    return sorted(d for d in all_dates if start <= str(d) <= end)


def _build_close_ffill(
    ohlcv_map: dict[str, pd.DataFrame],
    trading_days: list[date],
) -> dict[str, dict[date, float]]:
    """종가를 날짜 순으로 forward-fill해 반환."""
    result: dict[str, dict[date, float]] = {}
    for ticker, df in ohlcv_map.items():
        df2 = df.copy()
        df2["date"] = pd.to_datetime(df2["date"]).dt.date
        raw = dict(zip(df2["date"], df2["close"]))
        filled: dict[date, float] = {}
        last = None
        for d in trading_days:
            v = raw.get(d)
            if v is not None and v > 0:
                last = v
            if last is not None:
                filled[d] = last
        result[ticker] = filled
    return result


def _rebalance_dates(trading_days: list[date], freq_months: int) -> list[date]:
    """각 리밸런싱 주기의 첫 거래일."""
    seen: set[tuple[int, int]] = set()
    result = []
    for d in trading_days:
        # freq_months 단위로 버킷 할당 (1월 기준 정렬)
        bucket = ((d.year * 12 + d.month - 1) // freq_months)
        if bucket not in seen:
            seen.add(bucket)
            result.append(d)
    return result


def _calc_metrics(
    daily_values: list[tuple[date, float]],
    total_invested: int,
    label: str,
) -> dict:
    if not daily_values:
        return {}
    values  = pd.Series([v for _, v in daily_values])
    final   = float(values.iloc[-1])
    first_d = daily_values[0][0]
    last_d  = daily_values[-1][0]
    years   = (last_d - first_d).days / 365.25

    total_return = (final - total_invested) / total_invested * 100
    cagr = ((final / total_invested) ** (1 / years) - 1) * 100 if years > 0 else 0.0

    cummax = values.cummax()
    mdd    = float(((values - cummax) / cummax * 100).min())

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

def simulate_asset_allocation(
    ohlcv_map: dict[str, pd.DataFrame],
    weights: dict[str, float],
    start: str,
    end: str,
    capital: int,
    rebalance_freq_months: int,
) -> tuple[pd.DataFrame, dict]:
    """정적 자산배분 시뮬레이션.

    초기: 목표 비중대로 시가 매수.
    이후: 주기마다 현재 비중 → 목표 비중으로 시가 리밸런싱.
    """
    price_maps: dict[str, dict[date, float]] = {}

    for ticker, df in ohlcv_map.items():
        df2 = df.copy()
        df2["date"] = pd.to_datetime(df2["date"]).dt.date
        price_maps[ticker] = dict(zip(df2["date"], df2["open"]))

    trading_days  = _trading_days_in_range(ohlcv_map, start, end)
    close_maps    = _build_close_ffill(ohlcv_map, trading_days)
    rebal_dates   = set(_rebalance_dates(trading_days, rebalance_freq_months))

    shares: dict[str, float] = {t: 0.0 for t in weights}
    rebalance_log: list[dict] = []
    daily_values:  list[tuple[date, float]] = []

    for i, d in enumerate(trading_days):
        is_rebal = d in rebal_dates

        if is_rebal:
            # 현재 평가금 (시가 기준)
            open_prices = {t: price_maps[t].get(d) for t in weights}
            if any(p is None or p == 0 for p in open_prices.values()):
                is_rebal = False  # 가격 없으면 스킵

        if is_rebal:
            open_prices = {t: price_maps[t][d] for t in weights}

            # 현재 총 포트폴리오 가치 (시가 기준) — 첫 번째면 초기 자본
            if i == 0:
                total_value = float(capital)
            else:
                total_value = sum(shares[t] * open_prices[t] for t in weights)

            if total_value <= 0:
                total_value = float(capital)

            # 목표 비중으로 주식 수 재계산
            log_row: dict = {"date": d, "total_value": round(total_value, 0)}
            for t, w in weights.items():
                target_value  = total_value * w
                new_shares    = target_value / open_prices[t]
                log_row[f"shares_{t}"]  = round(new_shares, 6)
                log_row[f"open_{t}"]    = open_prices[t]
                log_row[f"weight_{t}"]  = round(
                    (new_shares * open_prices[t]) / total_value * 100, 2
                )
                shares[t] = new_shares

            rebalance_log.append(log_row)

        # 일별 종가 평가 (forward-fill이므로 누락 없음)
        close_val = sum(
            shares[t] * close_maps[t].get(d, 0.0) for t in weights
        )
        if close_val > 0:
            daily_values.append((d, close_val))

    if not daily_values:
        return pd.DataFrame(), {}

    log_df  = pd.DataFrame(rebalance_log)
    label   = f"asset_alloc_{rebalance_freq_months}mo"
    metrics = _calc_metrics(daily_values, capital, label)
    return log_df, metrics


def simulate_bah_multi(
    ohlcv_map: dict[str, pd.DataFrame],
    weights: dict[str, float],
    capital: int,
    start: str,
    end: str,
) -> dict:
    """리밸런싱 없이 초기 비중대로 보유 (BaH 멀티 자산)."""
    price_maps: dict[str, dict[date, float]] = {}

    for ticker, df in ohlcv_map.items():
        df2 = df.copy()
        df2["date"] = pd.to_datetime(df2["date"]).dt.date
        price_maps[ticker] = dict(zip(df2["date"], df2["open"]))

    trading_days = _trading_days_in_range(ohlcv_map, start, end)
    if not trading_days:
        return {}

    # 모든 티커에 시가가 있는 첫 거래일을 초기 매수일로
    first_d = None
    for d in trading_days:
        if all(price_maps[t].get(d, 0) > 0 for t in weights):
            first_d = d
            break
    if first_d is None:
        return {}

    shares: dict[str, float] = {}
    for t, w in weights.items():
        shares[t] = (capital * w) / price_maps[t][first_d]

    close_maps = _build_close_ffill(ohlcv_map, trading_days)

    daily_values: list[tuple[date, float]] = []
    for d in trading_days:
        if d < first_d:
            continue
        val = sum(shares[t] * close_maps[t].get(d, 0.0) for t in weights)
        if val > 0:
            daily_values.append((d, val))

    return _calc_metrics(daily_values, capital, "bah_multi")


# ── 출력 ─────────────────────────────────────────────────────────────────────

def _print_results(all_metrics: list[dict], weights: dict[str, float]) -> None:
    from tabulate import tabulate
    weight_str = " / ".join(f"{t} {w*100:.0f}%" for t, w in weights.items())
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
    print(f"\n[ 정적 자산배분 vs Buy-and-Hold 비교 | {weight_str} ]")
    print(tabulate(table, headers=headers, tablefmt="simple"))


# ── 메인 ─────────────────────────────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(description="정적 자산배분 ETF 백테스트")
    parser.add_argument("--start",           default=DEFAULT_START)
    parser.add_argument("--end",             default=DEFAULT_END)
    parser.add_argument("--capital",         type=int, default=DEFAULT_CAPITAL)
    parser.add_argument("--rebalance-freq",  default="quarterly",
                        choices=list(REBALANCE_FREQS),
                        help="리밸런싱 주기 (기본: quarterly)")
    parser.add_argument("--weights",         default=None,
                        help="비중 설정 (예: 069500=0.6,360750=0.4)")
    args = parser.parse_args()

    weights = DEFAULT_ASSETS.copy()
    if args.weights:
        weights = {}
        for part in args.weights.split(","):
            ticker, w = part.split("=")
            weights[ticker.strip()] = float(w)
        total_w = sum(weights.values())
        if abs(total_w - 1.0) > 0.01:
            print(f"[aa] 경고: 비중 합계 {total_w:.2f} ≠ 1.0. 정규화합니다.")
            weights = {t: w / total_w for t, w in weights.items()}

    freq_months = REBALANCE_FREQS[args.rebalance_freq]

    from discovery.data_loader import get_ohlcv_range

    print(f"[aa] OHLCV 로드 중... ({args.start} ~ {args.end})")
    ohlcv_map: dict[str, pd.DataFrame] = {}
    for ticker in weights:
        df = await get_ohlcv_range(ticker, args.start, args.end)
        if not df.empty:
            ohlcv_map[ticker] = df
            print(f"[aa] {ticker} {len(df)}거래일 확보")
        else:
            print(f"[aa] {ticker} 데이터 없음")

    missing = [t for t in weights if t not in ohlcv_map]
    if missing:
        print(f"[aa] 데이터 부족 ({missing}). 종료.")
        return

    log_df, aa_metrics = simulate_asset_allocation(
        ohlcv_map, weights, args.start, args.end, args.capital, freq_months
    )

    bah_metrics = simulate_bah_multi(ohlcv_map, weights, args.capital, args.start, args.end)

    all_metrics = []
    for m in [aa_metrics, bah_metrics]:
        if m:
            m["period"] = f"{args.start} ~ {args.end}"
            all_metrics.append(m)

    _print_results(all_metrics, weights)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    period_label = f"{args.start[:4]}_{args.end[:4]}"

    if not log_df.empty:
        log_path = OUT_DIR / f"asset_allocation_rebalance_{period_label}.csv"
        log_df.to_csv(log_path, index=False, encoding="utf-8-sig")
        print(f"[aa] 리밸런싱 이력 저장: {log_path.name}")

    if all_metrics:
        summary = pd.DataFrame(all_metrics)
        summary_path = OUT_DIR / f"asset_allocation_summary_{period_label}.csv"
        summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
        print(f"[aa] 요약 저장: {summary_path.name}")


if __name__ == "__main__":
    asyncio.run(main())
