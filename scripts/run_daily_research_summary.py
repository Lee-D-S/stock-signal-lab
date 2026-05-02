from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
PLAN_DIR = BASE_DIR / "01_기획"
STRATEGY_DIR = BASE_DIR / "07_전략신호"
OBS_DIR = BASE_DIR / "08_관찰기록"

UNIVERSE_CSV = STRATEGY_DIR / "거래대금_상위_유니버스.csv"
SCAN_CSV = STRATEGY_DIR / "관심종목_시그널_스캔.csv"
WATCHLIST_CSV = STRATEGY_DIR / "관심종목_시그널_후보.csv"
CONFIRMED_CSV = STRATEGY_DIR / "관심종목_시그널_후보_확정.csv"
ERROR_CSV = STRATEGY_DIR / "관심종목_시그널_오류.csv"
OBS_CSV = OBS_DIR / "관찰_로그(이상).csv"
PERFORMANCE_CSV = OBS_DIR / "관찰_성과_요약.csv"
SUMMARY_DIR = BASE_DIR / "10_일일요약"


def read_csv(path: Path, **kwargs: Any) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, encoding="utf-8-sig", **kwargs)
    except Exception:
        return pd.DataFrame()


def fmt_int(value: Any) -> str:
    if value is None or pd.isna(value):
        return "0"
    return f"{int(value):,}"


def fmt_table_cell(column: str, value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    if column.endswith("_pct"):
        try:
            return f"{float(value):+.2f}%"
        except (TypeError, ValueError):
            return str(value)
    return str(value)


def markdown_table(df: pd.DataFrame, columns: list[str], max_rows: int = 30) -> str:
    if df.empty:
        return "_없음_"
    view = df[[col for col in columns if col in df.columns]].head(max_rows).fillna("")
    if view.empty:
        return "_없음_"
    lines = [
        "| " + " | ".join(view.columns) + " |",
        "| " + " | ".join("---" for _ in view.columns) + " |",
    ]
    for _, row in view.iterrows():
        lines.append("| " + " | ".join(fmt_table_cell(col, row[col]).replace("|", "\\|") for col in view.columns) + " |")
    return "\n".join(lines)


def latest_signal_date(*frames: pd.DataFrame) -> str:
    dates: list[str] = []
    for df in frames:
        if not df.empty and "signal_date" in df.columns:
            dates.extend(str(value) for value in df["signal_date"].dropna().unique())
    return max(dates) if dates else ""


def build_summary(target_date: str) -> str:
    universe = read_csv(UNIVERSE_CSV, dtype={"ticker": str})
    scan = read_csv(SCAN_CSV, dtype={"ticker": str})
    watchlist = read_csv(WATCHLIST_CSV, dtype={"ticker": str})
    confirmed = read_csv(CONFIRMED_CSV, dtype={"ticker": str})
    errors = read_csv(ERROR_CSV, dtype={"ticker": str})
    observations = read_csv(OBS_CSV, dtype={"ticker": str})
    performance = read_csv(PERFORMANCE_CSV)

    signal_date = target_date or latest_signal_date(watchlist, confirmed, observations) or "latest"
    if not observations.empty and "signal_date" in observations.columns and signal_date != "latest":
        obs_today = observations[observations["signal_date"].astype(str) == signal_date].copy()
    else:
        obs_today = pd.DataFrame()

    confirmed_count = 0
    rejected_count = 0
    if not confirmed.empty and "flow_recheck_status" in confirmed.columns:
        confirmed_count = int((confirmed["flow_recheck_status"] == "confirmed").sum())
        rejected_count = int((confirmed["flow_recheck_status"] != "confirmed").sum())
    elif not confirmed.empty:
        confirmed_count = len(confirmed)

    new_universe_count = 0
    report_needed_count = 0
    if not universe.empty:
        if "universe_status" in universe.columns:
            new_universe_count = int(universe["universe_status"].astype(str).str.contains("신규|new", case=False, regex=True).sum())
        if "report_status" in universe.columns:
            report_needed_count = int(universe["report_status"].astype(str).str.contains("필요|needed", case=False, regex=True).sum())

    due_columns = [
        "next_close_return_pct",
        "d_plus_5_return_pct",
        "d_plus_10_return_pct",
        "d_plus_20_return_pct",
    ]
    tracking_filled = 0
    if not observations.empty:
        tracking_filled = int(sum(observations[col].astype(str).str.strip().ne("").sum() for col in due_columns if col in observations.columns))

    lines = [
        f"# 일일 운영 요약 - {signal_date}",
        "",
        "## 1. 거래대금 상위 유니버스",
        "",
        f"- 스캔 종목 수: {fmt_int(len(universe))}",
        f"- 신규/미보유 기업 추정: {fmt_int(new_universe_count)}",
        f"- 보고서 생성 필요 추정: {fmt_int(report_needed_count)}",
        "",
        "### 상위 유니버스",
        "",
        markdown_table(
            universe,
            ["rank", "ticker", "name", "close", "chg_pct", "trade_amount", "universe_status", "report_status"],
            max_rows=30,
        ),
        "",
        "## 2. 전략 신호",
        "",
        f"- 전체 스캔 행: {fmt_int(len(scan))}",
        f"- 초기 후보: {fmt_int(len(watchlist))}",
        f"- 수급 확정 후보: {fmt_int(confirmed_count)}",
        f"- 수급 불일치/보류: {fmt_int(rejected_count)}",
        f"- 오류: {fmt_int(len(errors))}",
        "",
        "### 확정 후보",
        "",
        markdown_table(
            confirmed,
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
                "flow_category_recheck",
                "suggested_response",
            ],
        ),
        "",
        "## 3. 관찰 로그",
        "",
        f"- 누적 관찰 후보: {fmt_int(len(observations))}",
        f"- 기준일 신규 관찰 후보: {fmt_int(len(obs_today))}",
        f"- 누적 D+ 추적값 입력 수: {fmt_int(tracking_filled)}",
        "",
        "### 기준일 관찰 후보",
        "",
        markdown_table(
            obs_today,
            [
                "signal_date",
                "ticker",
                "name",
                "hypothesis_id",
                "use_type",
                "event_close",
                "next_close_return_pct",
                "d_plus_5_return_pct",
                "d_plus_10_return_pct",
                "d_plus_20_return_pct",
                "result_label",
            ],
        ),
        "",
        "### 조건별 관찰 성과",
        "",
        markdown_table(
            performance,
            [
                "hypothesis_id",
                "use_type",
                "sample_count",
                "completed_count",
                "next_close_avg_return_pct",
                "d_plus_5_avg_return_pct",
                "d_plus_10_avg_return_pct",
                "d_plus_20_avg_return_pct",
                "positive_label_count",
                "negative_label_count",
                "result_status",
            ],
        ),
        "",
        "## 4. 다음 확인",
        "",
        "- D+1/D+5/D+10/D+20 도래 항목이 있으면 `run_observation_tracking_update.py` 결과를 확인한다.",
        "- 신규 조건은 active에 자동 승격하지 않는다.",
        "- 오류 CSV가 있으면 KIS/DART 인증, 휴장일, 상장일, 데이터 누락 여부를 확인한다.",
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="일일 운영 요약 생성")
    parser.add_argument("--date", help="YYYY-MM-DD. 생략하면 후보/관찰 로그의 최신 signal_date")
    args = parser.parse_args()

    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    target_date = args.date or ""
    summary = build_summary(target_date)
    output_date = target_date or pd.Timestamp.today().strftime("%Y-%m-%d")
    dated_path = SUMMARY_DIR / f"일일_운영_요약_{output_date}.md"
    latest_path = SUMMARY_DIR / "일일_운영_요약_latest.md"
    dated_path.write_text(summary, encoding="utf-8")
    latest_path.write_text(summary, encoding="utf-8")
    print(f"summary_md={dated_path}")
    print(f"latest_md={latest_path}")


if __name__ == "__main__":
    main()
