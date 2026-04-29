"""백테스트 엔진 — 날짜 순회, 신호 탐지, 체결 시뮬레이션.

Look-ahead bias 방지 원칙:
  - T일 조건 계산: precompute_indicators() 로 사전 계산된 T일까지의 지표 사용
  - T+1일 시가 매수 체결
  - 청산: T일 시가에 손절/익절/보유기간 만료 확인
"""

import argparse

import pandas as pd

from .portfolio import Portfolio
from .precompute import precompute_indicators, row_to_ind


def _collect_trading_dates(
    universe_data: dict[str, pd.DataFrame],
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
) -> list[pd.Timestamp]:
    all_dates: set = set()
    for df in universe_data.values():
        mask = (df["date"] >= start_ts) & (df["date"] <= end_ts)
        all_dates.update(df.loc[mask, "date"].tolist())
    return sorted(all_dates)


def run_backtest(
    universe_data: dict[str, pd.DataFrame],
    conditions: argparse.Namespace,
    start: str,
    end: str,
    hold_days: int,
    max_positions: int,
    initial_capital: float,
    stop_loss_pct: float,
    take_profit_pct: float,
) -> Portfolio:
    """백테스트 실행.

    Args:
        universe_data: {ticker → OHLCV DataFrame (RangeIndex, date 컬럼 포함)}
        conditions: screener_lib 호환 argparse.Namespace
        start / end: 백테스트 구간 "YYYY-MM-DD"
        hold_days: 최대 보유 거래일 수
        max_positions: 최대 동시 보유 포지션 수
        initial_capital: 초기 자본금 (원)
        stop_loss_pct: 손절 기준 수익률 (예: -0.05)
        take_profit_pct: 익절 기준 수익률 (예: 0.10)

    Returns:
        Portfolio (trades, daily_values 포함)
    """
    from screener_lib.indicators import check_all

    start_ts = pd.Timestamp(start)
    end_ts   = pd.Timestamp(end)

    # ── 1. 지표 사전 계산 ──────────────────────────────────────────────
    print("[engine] 지표 사전 계산 중...")
    ticker_ind: dict[str, pd.DataFrame] = {}
    for ticker, df in universe_data.items():
        try:
            ticker_ind[ticker] = precompute_indicators(df)
        except Exception:
            pass
    print(f"[engine] {len(ticker_ind)}개 종목 지표 계산 완료")

    # ── 2. OHLCV date-indexed 캐시 ────────────────────────────────────
    # open/close 가격 O(1) 조회용
    ticker_ohlcv: dict[str, pd.DataFrame] = {}
    for ticker, df in universe_data.items():
        ticker_ohlcv[ticker] = df.set_index("date")

    # ── 3. 거래일 목록 ────────────────────────────────────────────────
    trading_dates = _collect_trading_dates(universe_data, start_ts, end_ts)
    if not trading_dates:
        print("[engine] 거래일 없음. 종료.")
        return Portfolio(initial_capital, max_positions)

    print(f"[engine] 백테스트 시작: {len(trading_dates)}거래일, {len(ticker_ind)}개 종목")

    portfolio    = Portfolio(initial_capital, max_positions)
    pending_buys: list[str] = []  # T일 스크리닝 → T+1일 시가 매수 대기

    for day_idx, date in enumerate(trading_dates):

        # ── A. 기존 포지션 청산 확인 (T일 시가 기준) ─────────────────
        to_sell: list[tuple[str, str]] = []

        for ticker, pos in list(portfolio.positions.items()):
            ohlcv = ticker_ohlcv.get(ticker)
            if ohlcv is None or date not in ohlcv.index:
                continue
            open_price = float(ohlcv.at[date, "open"])
            if open_price <= 0:
                continue

            unrealized = (open_price / pos.entry_price) - 1

            if unrealized <= stop_loss_pct:
                to_sell.append((ticker, "stop_loss"))
            elif unrealized >= take_profit_pct:
                to_sell.append((ticker, "take_profit"))
            elif pos.hold_days_count >= hold_days:
                to_sell.append((ticker, "hold_days"))

        for ticker, reason in to_sell:
            ohlcv = ticker_ohlcv[ticker]
            if date in ohlcv.index:
                portfolio.sell(ticker, float(ohlcv.at[date, "open"]), date, reason)

        # ── B. 전일 신호 매수 (T일 시가) ─────────────────────────────
        for ticker in pending_buys:
            if ticker in portfolio.positions:
                continue
            ohlcv = ticker_ohlcv.get(ticker)
            if ohlcv is None or date not in ohlcv.index:
                continue
            open_price = float(ohlcv.at[date, "open"])
            portfolio.buy(ticker, open_price, date)
        pending_buys = []

        # ── C. T일 종가 기준 스크리닝 → 다음날 매수 예약 ────────────
        new_signals: list[str] = []
        for ticker, ind_df in ticker_ind.items():
            if ticker in portfolio.positions:
                continue
            if date not in ind_df.index:
                continue

            row = ind_df.loc[date]

            # MA240 NaN → 데이터 부족 → 건너뜀
            if pd.isna(row.get("ma240")):
                continue

            try:
                ind = row_to_ind(row)
                if check_all(ind, conditions):
                    new_signals.append(ticker)
            except Exception:
                continue

        pending_buys = new_signals

        # ── D. 포트폴리오 가치 기록 (T일 종가) ───────────────────────
        close_prices = {}
        for ticker, ohlcv in ticker_ohlcv.items():
            if date in ohlcv.index:
                close_prices[ticker] = float(ohlcv.at[date, "close"])

        portfolio.record_daily_value(date, close_prices)
        portfolio.increment_hold_days()

        if (day_idx + 1) % 60 == 0:
            print(
                f"[engine] {date.date()} | "
                f"포지션: {len(portfolio.positions)}개 | "
                f"체결: {len(portfolio.trades)}건 | "
                f"신호: {len(pending_buys)}개"
            )

    # ── 4. 기간 종료 후 잔여 포지션 강제 청산 (마지막일 종가) ─────────
    last_date = trading_dates[-1]
    for ticker in list(portfolio.positions.keys()):
        ohlcv = ticker_ohlcv.get(ticker)
        if ohlcv is None or last_date not in ohlcv.index:
            continue
        close_price = float(ohlcv.at[last_date, "close"])
        portfolio.sell(ticker, close_price, last_date, "end_of_period")

    print(
        f"[engine] 완료: 총 {len(portfolio.trades)}건 체결 | "
        f"최종 자산: {portfolio.cash:,.0f}원"
    )
    return portfolio
