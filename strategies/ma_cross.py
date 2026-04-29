import pandas as pd
import pandas_ta as ta

from .base import BaseStrategy


class MACrossStrategy(BaseStrategy):
    """이동평균 골든크로스/데드크로스 전략

    - 단기 MA가 장기 MA를 상향 돌파 → 매수 (골든크로스)
    - 단기 MA가 장기 MA를 하향 돌파 → 매도 (데드크로스)
    """

    name = "ma_cross"

    def __init__(
        self,
        tickers: list[str],
        short_period: int = 5,
        long_period: int = 20,
        enabled: bool = True,
    ):
        self.tickers = tickers
        self.short_period = short_period
        self.long_period = long_period
        self.enabled = enabled

    async def should_buy(self, ticker: str, df: pd.DataFrame) -> tuple[bool, str]:
        if len(df) < self.long_period + 2:
            return False, "데이터 부족"

        df = df.copy()
        df[f"ma{self.short_period}"] = ta.sma(df["close"], length=self.short_period)
        df[f"ma{self.long_period}"] = ta.sma(df["close"], length=self.long_period)
        df = df.dropna()

        if len(df) < 2:
            return False, "MA 계산 불가"

        prev_short = df[f"ma{self.short_period}"].iloc[-2]
        prev_long = df[f"ma{self.long_period}"].iloc[-2]
        curr_short = df[f"ma{self.short_period}"].iloc[-1]
        curr_long = df[f"ma{self.long_period}"].iloc[-1]

        golden_cross = prev_short <= prev_long and curr_short > curr_long
        if golden_cross:
            return True, f"골든크로스 (MA{self.short_period}={curr_short:.0f} > MA{self.long_period}={curr_long:.0f})"
        return False, ""

    async def should_sell(self, ticker: str, df: pd.DataFrame) -> tuple[bool, str]:
        if len(df) < self.long_period + 2:
            return False, "데이터 부족"

        df = df.copy()
        df[f"ma{self.short_period}"] = ta.sma(df["close"], length=self.short_period)
        df[f"ma{self.long_period}"] = ta.sma(df["close"], length=self.long_period)
        df = df.dropna()

        if len(df) < 2:
            return False, "MA 계산 불가"

        prev_short = df[f"ma{self.short_period}"].iloc[-2]
        prev_long = df[f"ma{self.long_period}"].iloc[-2]
        curr_short = df[f"ma{self.short_period}"].iloc[-1]
        curr_long = df[f"ma{self.long_period}"].iloc[-1]

        dead_cross = prev_short >= prev_long and curr_short < curr_long
        if dead_cross:
            return True, f"데드크로스 (MA{self.short_period}={curr_short:.0f} < MA{self.long_period}={curr_long:.0f})"
        return False, ""
