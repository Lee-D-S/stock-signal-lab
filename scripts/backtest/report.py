"""결과 출력 + CSV 저장."""

from pathlib import Path

import pandas as pd

from .metrics import compute_metrics, compute_reason_breakdown
from .portfolio import Portfolio

_REASON_KO = {
    "hold_days":     "보유기간 만료",
    "stop_loss":     "손절",
    "take_profit":   "익절",
    "end_of_period": "기간 종료",
}

_PASS_THRESHOLD = {
    "win_rate":      (">50%",   lambda v: v > 0.50),
    "avg_return":    (">0.23%", lambda v: v > 0.0023),
    "profit_factor": (">1.5",   lambda v: v > 1.5),
    "cagr":          (">0%",    lambda v: v > 0.0),
    "mdd":           ("<20%",   lambda v: v < 0.20),
    "sharpe":        (">1.0",   lambda v: v > 1.0),
}


def print_report(
    portfolio: Portfolio,
    conditions_desc: str,
    params: dict,
) -> dict:
    metrics = compute_metrics(portfolio)
    reasons = compute_reason_breakdown(portfolio.trades)

    def _flag(key: str, val: float) -> str:
        if key not in _PASS_THRESHOLD:
            return ""
        _, pred = _PASS_THRESHOLD[key]
        return " OK" if pred(val) else " --"

    print()
    print("=" * 64)
    print("[ 백테스트 결과 ]")
    print(f"  조건: {conditions_desc}")
    print(
        f"  기간: {params.get('start')} ~ {params.get('end')}"
        f"  |  보유일: {params.get('hold_days')}일"
        f"  |  포지션: {params.get('max_positions')}개"
    )
    print(
        f"  손절: {params.get('stop_loss', 0):.0%}"
        f"  |  익절: {params.get('take_profit', 0):.0%}"
        f"  |  초기자본: {params.get('initial_capital', 0):,.0f}원"
    )
    print("=" * 64)

    if metrics["total_trades"] == 0:
        print("  거래 없음 — 조건을 완화하거나 기간을 확인하세요.")
        print("=" * 64)
        return metrics

    print(f"  총 거래:    {metrics['total_trades']:>6}회")
    print(f"  승률:       {metrics['win_rate']:>6.1%}  (기준: >50%){_flag('win_rate', metrics['win_rate'])}")
    print(f"  평균수익률: {metrics['avg_return']:>+6.2%}  (기준: >0.23%){_flag('avg_return', metrics['avg_return'])}")
    print(f"  평균수익:   {metrics['avg_win']:>+6.2%}")
    print(f"  평균손실:   {metrics['avg_loss']:>+6.2%}")
    print(f"  손익비:     {metrics['profit_factor']:>6.2f}  (기준: >1.5){_flag('profit_factor', metrics['profit_factor'])}")
    print("-" * 64)
    print(f"  총 수익률:  {metrics['total_return']:>+6.2%}")
    print(f"  CAGR:       {metrics['cagr']:>+6.2%}  (기준: >0%){_flag('cagr', metrics['cagr'])}")
    print(f"  MDD:        {metrics['mdd']:>6.2%}  (기준: <20%){_flag('mdd', metrics['mdd'])}")
    print(f"  Sharpe:     {metrics['sharpe']:>6.2f}  (기준: >1.0){_flag('sharpe', metrics['sharpe'])}")
    print("-" * 64)

    passed = sum(
        1 for key, (_, pred) in _PASS_THRESHOLD.items()
        if metrics.get(key) is not None and pred(metrics[key])
    )
    print(f"  기준 충족:  {passed}/{len(_PASS_THRESHOLD)}개")

    if reasons:
        print("-" * 64)
        print("  청산 사유:")
        for reason, cnt in sorted(reasons.items(), key=lambda x: -x[1]):
            label = _REASON_KO.get(reason, reason)
            print(f"    {label:<16}: {cnt}회")

    print("=" * 64)
    return metrics


def save_trades_csv(portfolio: Portfolio, path: Path) -> None:
    if not portfolio.trades:
        return
    rows = [
        {
            "ticker":           t.ticker,
            "entry_date":       t.entry_date.date(),
            "exit_date":        t.exit_date.date(),
            "entry_price":      round(t.entry_price, 2),
            "exit_price":       round(t.exit_price, 2),
            "qty":              t.qty,
            "reason":           t.reason,
            "gross_return_pct": round(t.gross_return * 100, 3),
            "net_return_pct":   round(t.net_return * 100, 3),
        }
        for t in portfolio.trades
    ]
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")
    print(f"[report] 거래 내역 저장: {path}")


def save_equity_csv(portfolio: Portfolio, path: Path) -> None:
    if not portfolio.daily_values:
        return
    pd.DataFrame(portfolio.daily_values).to_csv(path, index=False, encoding="utf-8-sig")
    print(f"[report] 자산 곡선 저장: {path}")
