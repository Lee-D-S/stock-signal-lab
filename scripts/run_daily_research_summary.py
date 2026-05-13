from __future__ import annotations

import argparse
import os
import re
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent

from analysis_paths import (  # noqa: E402
    FOREIGN_FLOW_WATCHLIST_CSV,
    FOREIGN_SELL_FLOW_WATCHLIST_CSV,
    NEW_CONDITION_CONFIRMED_CSV,
    NEW_CONDITION_WATCHLIST_CSV,
    OBS_COMMON_CSV,
    OBS_COMMON_ERROR_CSV,
    OBS_COMMON_SUMMARY_CSV,
    OBS_FOREIGN_FLOW_ERROR_CSV,
    OBS_FOREIGN_SELL_FLOW_ERROR_CSV,
    OBS_NEW_CONDITION_UTF8_CSV,
    PLAN_DIR,
    SUMMARY_DIR,
    UNIVERSE_MASTER_CSV,
    WATCHLIST_CSV,
    WATCHLIST_CONFIRMED_CSV,
    WATCHLIST_SCAN_CSV,
    WATCHLIST_SCAN_ERROR_CSV,
)

UNIVERSE_CSV = UNIVERSE_MASTER_CSV
SCAN_CSV = WATCHLIST_SCAN_CSV
WATCHLIST_CSV = WATCHLIST_CSV
CONFIRMED_CSV = WATCHLIST_CONFIRMED_CSV
ERROR_CSV = WATCHLIST_SCAN_ERROR_CSV
NEW_WATCHLIST_CSV = NEW_CONDITION_WATCHLIST_CSV
NEW_CONFIRMED_CSV = NEW_CONDITION_CONFIRMED_CSV
FOREIGN_FLOW_WATCHLIST_CSV = FOREIGN_FLOW_WATCHLIST_CSV
FOREIGN_SELL_FLOW_WATCHLIST_CSV = FOREIGN_SELL_FLOW_WATCHLIST_CSV
OBS_CSV = OBS_COMMON_ERROR_CSV
NEW_OBS_CSV = OBS_NEW_CONDITION_UTF8_CSV
FOREIGN_FLOW_OBS_CSV = OBS_FOREIGN_FLOW_ERROR_CSV
FOREIGN_SELL_FLOW_OBS_CSV = OBS_FOREIGN_SELL_FLOW_ERROR_CSV
PERFORMANCE_CSV = OBS_COMMON_SUMMARY_CSV


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
    new_watchlist = read_csv(NEW_WATCHLIST_CSV, dtype={"ticker": str})
    new_confirmed = read_csv(NEW_CONFIRMED_CSV, dtype={"ticker": str})
    foreign_flow_watchlist = read_csv(FOREIGN_FLOW_WATCHLIST_CSV, dtype={"ticker": str})
    foreign_sell_flow_watchlist = read_csv(FOREIGN_SELL_FLOW_WATCHLIST_CSV, dtype={"ticker": str})
    observations = read_csv(OBS_CSV, dtype={"ticker": str})
    new_observations = read_csv(NEW_OBS_CSV, dtype={"ticker": str})
    foreign_flow_observations = read_csv(FOREIGN_FLOW_OBS_CSV, dtype={"ticker": str})
    foreign_sell_flow_observations = read_csv(FOREIGN_SELL_FLOW_OBS_CSV, dtype={"ticker": str})
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
        "## 1. 거래대금 상위 누적 유니버스",
        "",
        f"- 스캔 종목 수: {fmt_int(len(universe))}",
        f"- 신규/미보유 기업 추정: {fmt_int(new_universe_count)}",
        f"- 보고서 생성 필요 추정: {fmt_int(report_needed_count)}",
        "",
        "### 누적 유니버스",
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
                "valuation_class",
                "valuation_profit_trend",
                "valuation_trap_check",
                "suggested_response",
            ],
        ),
        "",
        "### 신규 조건 별도 관찰 후보",
        "",
        f"- 신규 조건 초기 후보: {fmt_int(len(new_watchlist))}",
        f"- 신규 조건 확정 후보: {fmt_int(len(new_confirmed[new_confirmed['flow_recheck_status'] == 'confirmed']) if not new_confirmed.empty and 'flow_recheck_status' in new_confirmed.columns else len(new_confirmed))}",
        f"- 신규 조건 누적 관찰 후보: {fmt_int(len(new_observations))}",
        "",
        markdown_table(
            new_confirmed,
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
        "### 외국인 연속 순매수 관찰",
        "",
        f"- 기준일 후보: {fmt_int(len(foreign_flow_watchlist))}",
        f"- 누적 관찰: {fmt_int(len(foreign_flow_observations))}",
        "",
        markdown_table(
            foreign_flow_watchlist,
            [
                "signal_date",
                "ticker",
                "name",
                "foreign_net_buy_streak",
                "foreign_qty",
                "foreign_net_buy_2d_qty",
                "foreign_net_buy_3d_qty",
                "foreign_volume_ratio_pct",
                "matched_conditions",
                "event_close",
            ],
        ),
        "",
        "### 외국인 연속 순매도 관찰",
        "",
        f"- 기준일 후보: {fmt_int(len(foreign_sell_flow_watchlist))}",
        f"- 누적 관찰: {fmt_int(len(foreign_sell_flow_observations))}",
        "",
        markdown_table(
            foreign_sell_flow_watchlist,
            [
                "signal_date",
                "ticker",
                "name",
                "foreign_net_sell_streak",
                "foreign_qty",
                "foreign_net_sell_2d_qty",
                "foreign_net_sell_3d_qty",
                "foreign_volume_ratio_pct",
                "matched_conditions",
                "event_close",
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


def build_telegram_message(target_date: str) -> str:
    universe = read_csv(UNIVERSE_CSV, dtype={"ticker": str})
    confirmed = read_csv(CONFIRMED_CSV, dtype={"ticker": str})
    new_confirmed = read_csv(NEW_CONFIRMED_CSV, dtype={"ticker": str})
    foreign_flow_watchlist = read_csv(FOREIGN_FLOW_WATCHLIST_CSV, dtype={"ticker": str})
    foreign_sell_flow_watchlist = read_csv(FOREIGN_SELL_FLOW_WATCHLIST_CSV, dtype={"ticker": str})
    observations = read_csv(OBS_CSV, dtype={"ticker": str})
    performance = read_csv(PERFORMANCE_CSV)

    signal_date = target_date or latest_signal_date(confirmed, observations) or pd.Timestamp.today().strftime("%Y-%m-%d")

    confirmed_today = pd.DataFrame()
    if not confirmed.empty and "signal_date" in confirmed.columns:
        confirmed_today = confirmed[confirmed["signal_date"].astype(str) == signal_date]

    confirmed_lines: list[str] = []
    for _, row in confirmed_today.iterrows():
        name = row.get("name", row.get("ticker", "?"))
        hid = row.get("hypothesis_id", "?")
        use_type = row.get("use_type", "")
        valuation = str(row.get("valuation_class", "") or "").strip()
        trap = str(row.get("valuation_trap_check", "") or "").strip()
        valuation_note = f" / {valuation}" if valuation else ""
        if trap and trap != "특이 리스크 제한적":
            valuation_note += f" / {trap}"
        confirmed_lines.append(f"  · {name} ({hid} {use_type}{valuation_note})")

    new_conf_count = 0
    if not new_confirmed.empty:
        if "flow_recheck_status" in new_confirmed.columns:
            new_conf_count = int((new_confirmed["flow_recheck_status"] == "confirmed").sum())
        else:
            new_conf_count = len(new_confirmed)

    obs_today_count = 0
    if not observations.empty and "signal_date" in observations.columns:
        obs_today_count = int((observations["signal_date"].astype(str) == signal_date).sum())

    report_needed = pd.DataFrame()
    if not universe.empty and "report_status" in universe.columns:
        report_needed = universe[
            universe["report_status"].astype(str).str.contains("필요|needed", case=False, regex=True, na=False)
        ].copy()
    new_universe_count = 0
    if not universe.empty and "universe_status" in universe.columns:
        new_universe_count = int(
            universe["universe_status"].astype(str).str.contains("신규|new", case=False, regex=True, na=False).sum()
        )

    perf_lines: list[str] = []
    if not performance.empty:
        for _, row in performance.iterrows():
            hid = row.get("hypothesis_id", "?")
            cnt = int(row.get("sample_count", 0))
            status = str(row.get("result_status", "표본 부족"))
            perf_lines.append(f"  {hid}: {cnt}건 ({status})")

    lines = [
        f"[auto-invest] 일일 요약 {signal_date}",
        "",
        "■ 전략 신호",
        f"확정 후보: {len(confirmed_lines)}건",
        *(confirmed_lines or ["  (없음)"]),
        f"신규 조건 확정: {new_conf_count}건",
        f"외국인 연속 순매수 후보: {len(foreign_flow_watchlist)}건",
        f"외국인 연속 순매도 후보: {len(foreign_sell_flow_watchlist)}건",
        "",
        "■ 관찰 로그",
        f"누적: {len(observations)}건 | 오늘 신규: {obs_today_count}건",
    ]

    if new_universe_count or not report_needed.empty:
        names = []
        if "name" in report_needed.columns:
            names = [str(name) for name in report_needed["name"].dropna().head(5)]
        suffix = "" if len(report_needed) <= 5 else f" 외 {len(report_needed) - 5}개"
        lines += [
            "",
            "■ 신규 기업/보고서",
            f"신규 폴더: {new_universe_count}개 | 보고서 필요: {len(report_needed)}개",
            f"대상: {', '.join(names) + suffix if names else '(목록 없음)'}",
            "보고서 생성 명령:",
            "rtk python -u scripts/run_new_company_reports.py --include-existing-missing",
        ]

    if perf_lines:
        lines += ["", "■ 조건별 성과"]
        lines.extend(perf_lines)

    return "\n".join(lines)


def send_telegram(text: str) -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        print("telegram secrets missing; skip summary notification")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = urllib.parse.urlencode({"chat_id": chat_id, "text": text, "disable_web_page_preview": "true"}).encode("utf-8")
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=payload, method="POST"), timeout=20) as resp:
            resp.read()
        print("telegram summary sent")
    except Exception as exc:
        print(f"telegram summary failed: {exc}")


def update_routine_doc(observations: pd.DataFrame) -> None:
    """운영_루틴.md의 '현재 관찰 중인 종목' 섹션을 관찰 로그 기준으로 갱신."""
    routine_path = PLAN_DIR / "운영_루틴.md"
    if not routine_path.exists():
        return

    today = pd.Timestamp.today().strftime("%Y-%m-%d")

    if not observations.empty and "result_label" in observations.columns:
        active = observations[observations["result_label"].isna() | (observations["result_label"].astype(str).str.strip() == "")]
    else:
        active = observations

    rows: list[str] = []
    for _, row in active.iterrows():
        name = str(row.get("name", ""))
        ticker = str(row.get("ticker", "")).zfill(6)
        hid = str(row.get("hypothesis_id", ""))
        sig_date = str(row.get("signal_date", ""))
        d5 = str(row.get("d_plus_5_return_pct", "")).strip() or "-"
        d10 = str(row.get("d_plus_10_return_pct", "")).strip() or "-"
        d20 = str(row.get("d_plus_20_return_pct", "")).strip() or "-"
        label = str(row.get("result_label", "")).strip() or "-"
        rows.append(f"| {name} | {ticker} | {hid} | {sig_date} | {d5} | {d10} | {d20} | {label} |")

    header = f"## 현재 관찰 중인 종목 ({today} 기준)"
    table_lines = [
        "| 종목 | 코드 | 가설 | 신호일 | D+5 | D+10 | D+20 | result_label |",
        "|------|------|------|--------|-----|------|------|--------------|",
        *(rows or ["| (없음) | | | | | | | |"]),
    ]
    new_section = header + "\n\n" + "\n".join(table_lines)

    content = routine_path.read_text(encoding="utf-8")
    updated = re.sub(
        r"## 현재 관찰 중인 종목 \(.*?기준\).*",
        new_section,
        content,
        flags=re.DOTALL,
    )
    if updated == content:
        updated = content.rstrip() + "\n\n" + new_section + "\n"
    routine_path.write_text(updated, encoding="utf-8")
    print(f"routine doc updated: {routine_path.name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="일일 운영 요약 생성")
    parser.add_argument("--date", help="YYYY-MM-DD. 생략하면 후보/관찰 로그의 최신 signal_date")
    parser.add_argument("--telegram", action="store_true", help="텔레그램으로 핵심 요약 전송")
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

    if args.telegram:
        send_telegram(build_telegram_message(target_date))

    update_routine_doc(read_csv(OBS_CSV, dtype={"ticker": str}))


if __name__ == "__main__":
    main()
