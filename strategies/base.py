from abc import ABC, abstractmethod

import pandas as pd


class BaseStrategy(ABC):
    name: str = "base"
    tickers: list[str] = []
    enabled: bool = True

    @abstractmethod
    async def should_buy(self, ticker: str, df: pd.DataFrame) -> tuple[bool, str]:
        """매수 여부 판단.

        Returns:
            (True/False, reason)
        """
        ...

    @abstractmethod
    async def should_sell(self, ticker: str, df: pd.DataFrame) -> tuple[bool, str]:
        """매도 여부 판단.

        Returns:
            (True/False, reason)
        """
        ...

    def get_order_quantity(self, price: int, max_amount: int) -> int:
        """주문 수량 계산 (최대 금액 기준)"""
        if price <= 0:
            return 0
        return max(1, max_amount // price)
