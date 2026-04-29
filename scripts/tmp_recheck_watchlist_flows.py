from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from tmp_quarterly_stock_analysis import fetch_investor_range  # noqa: E402


BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
STRATEGY_DIR = BASE_DIR / "07_전략신호"
CANDIDATES_CSV = STRATEGY_DIR / "관심종목_시그널_후보.csv"
CONFIRMED_CSV = STRATEGY_DIR / "관심종목_시그널_후보_확정.csv"
CONFIRMED_MD = STRATEGY_DIR / "관심종목_시그널_후보_확정.md"


def classify_flow(investor: pd.DataFrame, event_date: pd.Timestamp) -> tuple[str, float | None, float | None, float | None]:
    if investor.empty:
        return "수급정보부족", None, None, None
    win = investor[(investor["date"] >= event_date - pd.Timedelta(days=5)) & (investor["date"] <= event_date)]
    if win.empty:
        return "수급정보부족", None, None, None

    foreign = win["foreign_qty"].sum(min_count=1)
    institution = win["institution_qty"].sum(min_count=1)
    individual = win["individual_qty"].sum(min_count=1)
    if pd.notna(foreign) and pd.notna(institution):
        if foreign > 0 and institution > 0:
            return "외국인기관동반매수", float(foreign), float(institution), float(individual)
        if foreign < 0 and institution < 0:
            return "외국인기관동반매도", float(foreign), float(institution), float(individual)
        return "수급엇갈림", float(foreign), float(institution), float(individual)
    return "수급정보부족", None, None, None


def fmt_int(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{int(round(float(value))):,}"


def fmt_pct(value: Any) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):+.2f}%"


def markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_후보 없음_"
    view = df.copy()
    for col in ("chg_pct", "backtest_avg_score_pct"):
        if col in view.columns:
            view[col] = view[col].map(fmt_pct)
    for col in ("foreign_5d_recheck", "institution_5d_recheck", "individual_5d_recheck"):
        if col in view.columns:
            view[col] = view[col].map(fmt_int)
    if "backtest_hit_rate" in view.columns:
        view["backtest_hit_rate"] = view["backtest_hit_rate"].map(lambda x: "N/A" if pd.isna(x) else f"{float(x):.2%}")
    headers = [str(col) for col in view.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for _, row in view.iterrows():
        values = [str(row[col]).replace("|", "\\|") for col in view.columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def build_markdown(df: pd.DataFrame) -> str:
    confirmed = df[df["flow_recheck_status"] == "confirmed"].copy()
    pending = df[df["flow_recheck_status"] != "confirmed"].copy()
    lines = [
        "# 일별 전략 감시 후보 수급 재조회",
        "",
        f"- 전체 재조회 대상: {len(df):,}건",
        f"- 확정 후보: {len(confirmed):,}건",
        f"- 보류/탈락 후보: {len(pending):,}건",
        "- 확정 후보는 `required_flow_category`와 재조회 수급 분류가 일치한 경우다.",
        "",
        "## 확정 후보",
        "",
        markdown_table(
            confirmed[
                [
                    "priority",
                    "hypothesis_id",
                    "use_type",
                    "ticker",
                    "name",
                    "signal_date",
                    "direction",
                    "chg_pct",
                    "amount_tag",
                    "required_flow_category",
                    "flow_category_recheck",
                    "foreign_5d_recheck",
                    "institution_5d_recheck",
                    "suggested_response",
                    "backtest_avg_score_pct",
                    "backtest_hit_rate",
                ]
            ]
            if not confirmed.empty
            else confirmed
        ),
        "",
        "## 보류/탈락 후보",
        "",
        markdown_table(
            pending[
                [
                    "priority",
                    "hypothesis_id",
                    "use_type",
                    "ticker",
                    "name",
                    "signal_date",
                    "required_flow_category",
                    "flow_category_recheck",
                    "flow_recheck_status",
                    "flow_recheck_error",
                ]
            ]
            if not pending.empty
            else pending
        ),
        "",
    ]
    return "\n".join(lines)


async def recheck_row(row: pd.Series) -> dict[str, Any]:
    event_date = pd.Timestamp(row["signal_date"])
    start = (event_date - pd.Timedelta(days=10)).strftime("%Y-%m-%d")
    end = event_date.strftime("%Y-%m-%d")
    out = row.to_dict()
    try:
        investor = await fetch_investor_range(str(row["ticker"]).zfill(6), start, end)
        flow, foreign, institution, individual = classify_flow(investor, event_date)
        out["flow_category_recheck"] = flow
        out["foreign_5d_recheck"] = foreign
        out["institution_5d_recheck"] = institution
        out["individual_5d_recheck"] = individual
        out["flow_recheck_error"] = ""
        if flow == row["required_flow_category"]:
            out["flow_recheck_status"] = "confirmed"
        elif flow == "수급정보부족":
            out["flow_recheck_status"] = "pending_no_flow_data"
        else:
            out["flow_recheck_status"] = "rejected_flow_mismatch"
    except Exception as exc:
        out["flow_category_recheck"] = "수급정보부족"
        out["foreign_5d_recheck"] = None
        out["institution_5d_recheck"] = None
        out["individual_5d_recheck"] = None
        out["flow_recheck_status"] = "pending_api_error"
        out["flow_recheck_error"] = repr(exc)
    return out


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--delay", type=float, default=0.45)
    args = parser.parse_args()

    candidates = pd.read_csv(CANDIDATES_CSV, encoding="utf-8-sig", dtype={"ticker": str})
    if candidates.empty:
        candidates.to_csv(CONFIRMED_CSV, index=False, encoding="utf-8-sig")
        CONFIRMED_MD.write_text(build_markdown(candidates), encoding="utf-8")
        print("candidates=0")
        return

    rows = []
    for _, row in candidates.iterrows():
        rows.append(await recheck_row(row))
        await asyncio.sleep(args.delay)

    out = pd.DataFrame(rows)
    out = out.sort_values(["flow_recheck_status", "priority", "ticker"]).reset_index(drop=True)
    out.to_csv(CONFIRMED_CSV, index=False, encoding="utf-8-sig")
    CONFIRMED_MD.write_text(build_markdown(out), encoding="utf-8")
    print(f"rechecked={len(out)}")
    print(out["flow_recheck_status"].value_counts().to_string())
    print(f"confirmed_csv={CONFIRMED_CSV}")
    print(f"confirmed_md={CONFIRMED_MD}")


if __name__ == "__main__":
    asyncio.run(main())
