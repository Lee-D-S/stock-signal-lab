from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from tmp_realistic_backtest_hypotheses import build_trades, summarize


ROOT = Path(__file__).resolve().parent.parent
BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
BACKTEST_DIR = BASE_DIR / "06_백테스트"

COMBINED_SUMMARY_CSV = BACKTEST_DIR / "가설_실전_백테스트_전체_설정.csv"
COMBINED_TRADES_CSV = BACKTEST_DIR / "가설_실전_백테스트_전체_거래.csv"
COMBINED_MD = BACKTEST_DIR / "가설_실전_백테스트_전체_설정.md"


def fmt_pct(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):+.2f}%"


def fmt_rate(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):.2%}"


def markdown_table(df: pd.DataFrame, max_rows: int = 80) -> str:
    if df.empty:
        return "_데이터 없음_"
    view = df.head(max_rows).copy()
    for col in view.columns:
        if col == "hit_rate":
            view[col] = view[col].map(fmt_rate)
        elif col.endswith("_pct"):
            view[col] = view[col].map(fmt_pct)
        else:
            view[col] = view[col].map(lambda x: "N/A" if pd.isna(x) else x)
    headers = [str(col) for col in view.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for _, row in view.iterrows():
        values = [str(row[col]).replace("|", "\\|") for col in view.columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def build_markdown(summary: pd.DataFrame, trades: pd.DataFrame) -> str:
    best = (
        summary.sort_values(["avg_score_return_pct", "hit_rate"], ascending=[False, False])
        .groupby("hypothesis_id", as_index=False)
        .head(1)
        .sort_values("hypothesis_id")
    )
    coverage = (
        trades.groupby(["entry_mode", "hold_days", "status"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["entry_mode", "hold_days", "status"])
    )
    lines = [
        "# 국면별 후보 실거래형 백테스트 전체 비교",
        "",
        "## 범위",
        "",
        "- 진입 기준: 다음 거래일 시가, 다음 거래일 종가",
        "- 보유 기간: 5/10/20거래일",
        "- 거래비용: 편도 15bp",
        "- 동일 후보/동일 종목 중복 신호는 제거",
        "- 기존 `data/ohlcv_cache`에 있는 일봉만 사용",
        "",
        "## 후보별 최고 조합",
        "",
        markdown_table(
            best[
                [
                    "hypothesis_id",
                    "entry_mode",
                    "hold_days",
                    "action_hint",
                    "tested_trades",
                    "company_count",
                    "avg_score_return_pct",
                    "hit_rate",
                    "worst_score_return_pct",
                    "best_score_return_pct",
                ]
            ]
        ),
        "",
        "## 전체 결과",
        "",
        markdown_table(
            summary[
                [
                    "hypothesis_id",
                    "entry_mode",
                    "hold_days",
                    "action_hint",
                    "tested_trades",
                    "avg_score_return_pct",
                    "hit_rate",
                    "worst_score_return_pct",
                    "best_score_return_pct",
                ]
            ].sort_values(["hypothesis_id", "entry_mode", "hold_days"])
        ),
        "",
        "## 데이터 커버리지",
        "",
        markdown_table(coverage),
        "",
        "## 해석",
        "",
        "- 캐시 보강 후에도 일부 이벤트는 제외되지만, 주요 후보의 검증 표본은 늘었다.",
        "- H01은 진입 후보로 다시 유지한다. 다음날 시가 진입 기준 5/10/20일 모두 평균 점수가 플러스다.",
        "- H02는 반등 관찰 후보 중 가장 강하다. 다음날 시가 진입 20일 보유 기준 평균 점수와 적중률이 가장 좋다.",
        "- H03도 반등 관찰 후보로 유지하되, 20일 기준 적중률은 5~10일보다 낮아 단기 관찰이 더 안정적이다.",
        "- H06은 추격매수 회피 후보로 유지한다. 20일 기준 회피 효과가 안정적이다.",
        "- H04도 추격매수 회피 후보로 격상 가능하다. 다만 2021년 유동성 장세 전용 후보라 현재 장세 적용성은 낮다.",
        "- H05는 보유 기간을 늘릴수록 약해져 단기 반등 후보 이상으로 보기 어렵다.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    summaries = []
    trades_all = []
    for entry_mode in ("next_open", "next_close"):
        for hold_days in (5, 10, 20):
            trades = build_trades(entry_mode=entry_mode, hold_days=hold_days, fee_bps=15.0, no_overlap=True)
            summary = summarize(trades)
            trades["entry_mode"] = entry_mode
            trades["hold_days"] = hold_days
            summary["entry_mode"] = entry_mode
            summary["hold_days"] = hold_days
            trades_all.append(trades)
            summaries.append(summary)

    combined_trades = pd.concat(trades_all, ignore_index=True)
    combined_summary = pd.concat(summaries, ignore_index=True)
    combined_summary = combined_summary.sort_values(
        ["hypothesis_id", "entry_mode", "hold_days"]
    ).reset_index(drop=True)

    combined_trades.to_csv(COMBINED_TRADES_CSV, index=False, encoding="utf-8-sig")
    combined_summary.to_csv(COMBINED_SUMMARY_CSV, index=False, encoding="utf-8-sig")
    COMBINED_MD.write_text(build_markdown(combined_summary, combined_trades), encoding="utf-8")

    print(f"summary_rows={len(combined_summary)}")
    print(f"trade_rows={len(combined_trades)}")
    print(f"summary_csv={COMBINED_SUMMARY_CSV}")
    print(f"trades_csv={COMBINED_TRADES_CSV}")
    print(f"md={COMBINED_MD}")


if __name__ == "__main__":
    main()
