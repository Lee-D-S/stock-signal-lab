"""성과 지표 계산."""

import math
from collections import Counter

import pandas as pd

from .portfolio import Portfolio, Trade


def compute_metrics(portfolio: Portfolio) -> dict:
    trades = portfolio.trades

    if not trades:
        return {
            "total_trades":  0,
            "win_rate":      0.0,
            "avg_return":    0.0,
            "avg_win":       0.0,
            "avg_loss":      0.0,
            "profit_factor": 0.0,
            "total_return":  0.0,
            "cagr":          0.0,
            "mdd":           0.0,
            "sharpe":        0.0,
        }

    rets   = [t.net_return for t in trades]
    wins   = [r for r in rets if r > 0]
    losses = [r for r in rets if r <= 0]

    win_rate      = len(wins) / len(rets)
    avg_return    = sum(rets) / len(rets)
    avg_win       = sum(wins) / len(wins) if wins else 0.0
    avg_loss      = sum(losses) / len(losses) if losses else 0.0
    gross_wins    = sum(wins)
    gross_losses  = abs(sum(losses))
    profit_factor = gross_wins / gross_losses if gross_losses > 0 else float("inf")

    daily_df     = pd.DataFrame(portfolio.daily_values)
    total_return = cagr = mdd = sharpe = 0.0

    if not daily_df.empty:
        final_value  = daily_df["value"].iloc[-1]
        initial      = portfolio.initial_capital
        total_return = (final_value / initial) - 1

        start = daily_df["date"].iloc[0]
        end   = daily_df["date"].iloc[-1]
        years = (end - start).days / 365.25
        if years > 0:
            cagr = (final_value / initial) ** (1 / years) - 1

        values = daily_df["value"].values
        peak   = values[0]
        max_dd = 0.0
        for v in values:
            if v > peak:
                peak = v
            dd = (v - peak) / peak
            if dd < max_dd:
                max_dd = dd
        mdd = abs(max_dd)

        daily_rets = daily_df["value"].pct_change().dropna()
        if len(daily_rets) > 1 and daily_rets.std() > 0:
            daily_rf = (1.035) ** (1 / 252) - 1
            excess   = daily_rets - daily_rf
            sharpe   = (excess.mean() / excess.std()) * math.sqrt(252)

    return {
        "total_trades":  len(trades),
        "win_rate":      win_rate,
        "avg_return":    avg_return,
        "avg_win":       avg_win,
        "avg_loss":      avg_loss,
        "profit_factor": profit_factor,
        "total_return":  total_return,
        "cagr":          cagr,
        "mdd":           mdd,
        "sharpe":        sharpe,
    }


def compute_reason_breakdown(trades: list[Trade]) -> dict[str, int]:
    return dict(Counter(t.reason for t in trades))
