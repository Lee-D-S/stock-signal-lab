"""포지션 관리, 거래비용, 수익률 추적."""

from dataclasses import dataclass, field

import pandas as pd

BUY_COMMISSION  = 0.00015   # 매수 수수료 0.015%
SELL_COMMISSION = 0.00015   # 매도 수수료 0.015%
SLIPPAGE        = 0.001     # 슬리피지 ±0.1% (불리한 방향)


@dataclass
class Trade:
    ticker: str
    entry_date: pd.Timestamp
    exit_date: pd.Timestamp
    entry_price: float          # 슬리피지 포함 매수 체결가
    exit_price: float           # 슬리피지 포함 매도 체결가
    qty: int
    reason: str                 # hold_days | stop_loss | take_profit | end_of_period
    gross_return: float         # (exit / entry) - 1
    net_return: float           # 수수료 + 슬리피지 차감 후


@dataclass
class Position:
    ticker: str
    qty: int
    entry_price: float          # 슬리피지 포함 매수 체결가
    entry_date: pd.Timestamp
    hold_days_count: int = 0    # 보유 거래일 수


class Portfolio:
    def __init__(self, initial_capital: float, max_positions: int) -> None:
        self.initial_capital = initial_capital
        self.cash            = initial_capital
        self.max_positions   = max_positions
        self.positions: dict[str, Position] = {}
        self.trades: list[Trade]            = []
        self.daily_values: list[dict]       = []

    @property
    def position_budget(self) -> float:
        return self.initial_capital / self.max_positions

    def can_buy(self, ticker: str) -> bool:
        return (
            ticker not in self.positions
            and len(self.positions) < self.max_positions
            and self.cash >= self.position_budget * 0.5  # 예산 절반 이상 남아있을 때
        )

    def buy(self, ticker: str, open_price: float, date: pd.Timestamp) -> bool:
        """T+1 시가 매수 (슬리피지: 시가보다 0.1% 높게 체결 가정).

        Returns True if executed.
        """
        if not self.can_buy(ticker) or open_price <= 0:
            return False

        exec_price   = open_price * (1 + SLIPPAGE)
        cost_per_shr = exec_price * (1 + BUY_COMMISSION)
        qty          = int(self.position_budget / cost_per_shr)
        if qty <= 0:
            return False

        total_paid = cost_per_shr * qty
        if total_paid > self.cash:
            qty = int(self.cash / cost_per_shr)
            if qty <= 0:
                return False
            total_paid = cost_per_shr * qty

        self.cash -= total_paid
        self.positions[ticker] = Position(
            ticker=ticker,
            qty=qty,
            entry_price=exec_price,
            entry_date=date,
        )
        return True

    def sell(
        self,
        ticker: str,
        open_price: float,
        date: pd.Timestamp,
        reason: str,
    ) -> Trade | None:
        """T 시가 매도 (슬리피지: 시가보다 0.1% 낮게 체결 가정).

        Returns Trade record or None if position not found.
        """
        pos = self.positions.pop(ticker, None)
        if pos is None or open_price <= 0:
            return None

        exec_price = open_price * (1 - SLIPPAGE)
        net_per_shr = exec_price * (1 - SELL_COMMISSION)
        self.cash  += net_per_shr * pos.qty

        gross_return = (exec_price / pos.entry_price) - 1
        buy_cost_per_shr = pos.entry_price * (1 + BUY_COMMISSION)
        net_return = (net_per_shr / buy_cost_per_shr) - 1

        trade = Trade(
            ticker=ticker,
            entry_date=pos.entry_date,
            exit_date=date,
            entry_price=pos.entry_price,
            exit_price=exec_price,
            qty=pos.qty,
            reason=reason,
            gross_return=gross_return,
            net_return=net_return,
        )
        self.trades.append(trade)
        return trade

    def record_daily_value(self, date: pd.Timestamp, close_prices: dict[str, float]) -> float:
        stock_value = sum(
            pos.qty * close_prices.get(pos.ticker, pos.entry_price)
            for pos in self.positions.values()
        )
        total = self.cash + stock_value
        self.daily_values.append({"date": date, "value": total})
        return total

    def increment_hold_days(self) -> None:
        for pos in self.positions.values():
            pos.hold_days_count += 1
