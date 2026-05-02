from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
STRATEGY_DIR = BASE_DIR / "07_전략신호"
OBS_DIR = BASE_DIR / "08_관찰기록"

CONFIRMED_CSV = STRATEGY_DIR / "관심종목_시그널_후보_확정.csv"
OBS_UTF8_CSV = OBS_DIR / "관찰_로그(이상).csv"
OBS_CP949_CSV = OBS_DIR / "관찰_로그.csv"
OBS_MD = OBS_DIR / "관찰_로그.md"

OBS_FIELDNAMES = [
    "signal_date",
    "ticker",
    "name",
    "hypothesis_id",
    "use_type",
    "decision_status",
    "decision_note",
    "event_direction",
    "event_chg_pct",
    "event_close",
    "amount_tag",
    "dart_tag",
    "window_category",
    "flow_category_confirmed",
    "foreign_5d",
    "institution_5d",
    "individual_5d",
    "planned_entry_rule",
    "planned_hold_days",
    "backtest_avg_score_pct",
    "backtest_hit_rate",
    "next_trading_day",
    "next_open",
    "next_close",
    "d_plus_5_close",
    "d_plus_10_close",
    "d_plus_20_close",
    "next_open_return_pct",
    "next_close_return_pct",
    "d_plus_5_return_pct",
    "d_plus_10_return_pct",
    "d_plus_20_return_pct",
    "result_label",
    "review_note",
]


PLANNED_ENTRY_RULES = {
    "H01": "다음 거래일 과도한 갭 상승이 없고 장초반 급락이 아니면 분할 진입 검토. 갭 급등 시 추격 금지",
    "H02": "다음 거래일 장초반 반등 확인 전에는 진입 보류. 전일 종가 회복 또는 장중 VWAP/시가 회복 확인 시 단기 진입 검토",
    "H03": "다음 거래일 하락 진정과 장중 반등 확인 전에는 진입 보류. 반등 실패 시 관찰 종료 검토",
    "H04": "추격 진입 금지. 추가 상승 지속 여부만 리스크 검증 샘플로 관찰",
    "H06": "추격 진입 금지. 과열 이후 되돌림 여부를 리스크 검증 샘플로 관찰",
}


def as_str(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value)


def fmt_int(value: Any) -> str:
    if value is None or pd.isna(value) or value == "":
        return ""
    return str(int(round(float(value))))


def fmt_pct(value: Any) -> str:
    if value is None or pd.isna(value) or value == "":
        return ""
    return f"{float(value):.2f}"


def fmt_hit_rate(value: Any) -> str:
    if value is None or pd.isna(value) or value == "":
        return ""
    number = float(value)
    if abs(number) <= 1:
        number *= 100
    return f"{number:.2f}"


def fmt_signed_int(value: Any) -> str:
    text = fmt_int(value)
    if not text:
        return ""
    number = int(text)
    return f"{number:+,}"


def read_observation_rows(path: Path, encoding: str) -> tuple[list[str], list[dict[str, str]]]:
    if not path.exists():
        return OBS_FIELDNAMES.copy(), []
    with path.open(encoding=encoding, newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        return list(reader.fieldnames or []), rows


def write_observation_rows(path: Path, encoding: str, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding=encoding, newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def confirmed_to_observation(row: pd.Series, fieldnames: list[str]) -> dict[str, str]:
    hypothesis_id = as_str(row.get("hypothesis_id"))
    suggested_response = as_str(row.get("suggested_response"))
    flow_category = as_str(row.get("flow_category_recheck")) or as_str(row.get("flow_category"))

    values = {
        "signal_date": as_str(row.get("signal_date")),
        "ticker": as_str(row.get("ticker")).zfill(6),
        "name": as_str(row.get("name")),
        "hypothesis_id": hypothesis_id,
        "use_type": as_str(row.get("use_type")),
        "decision_status": "관찰대상",
        "decision_note": suggested_response,
        "event_direction": as_str(row.get("direction")),
        "event_chg_pct": fmt_pct(row.get("chg_pct")),
        "event_close": fmt_int(row.get("close")),
        "amount_tag": as_str(row.get("amount_tag")),
        "dart_tag": as_str(row.get("dart_tag")),
        "window_category": as_str(row.get("window_category")),
        "flow_category_confirmed": flow_category,
        "foreign_5d": fmt_int(row.get("foreign_5d_recheck") or row.get("foreign_5d")),
        "institution_5d": fmt_int(row.get("institution_5d_recheck") or row.get("institution_5d")),
        "individual_5d": fmt_int(row.get("individual_5d_recheck") or row.get("individual_5d")),
        "planned_entry_rule": PLANNED_ENTRY_RULES.get(hypothesis_id, suggested_response),
        "planned_hold_days": fmt_int(row.get("preferred_hold_days")),
        "backtest_avg_score_pct": fmt_pct(row.get("backtest_avg_score_pct")),
        "backtest_hit_rate": fmt_hit_rate(row.get("backtest_hit_rate")),
        "next_trading_day": "",
        "next_open": "",
        "next_close": "",
        "d_plus_5_close": "",
        "d_plus_10_close": "",
        "d_plus_20_close": "",
        "next_open_return_pct": "",
        "next_close_return_pct": "",
        "d_plus_5_return_pct": "",
        "d_plus_10_return_pct": "",
        "d_plus_20_return_pct": "",
        "result_label": "",
        "review_note": "",
    }
    return {field: values.get(field, "") for field in fieldnames}


def observation_key(row: dict[str, str]) -> tuple[str, str, str]:
    return (
        as_str(row.get("signal_date")),
        as_str(row.get("ticker")).zfill(6),
        as_str(row.get("hypothesis_id")),
    )


def append_confirmed_to_csv(
    confirmed_csv: Path = CONFIRMED_CSV,
    obs_utf8_csv: Path = OBS_UTF8_CSV,
    obs_cp949_csv: Path = OBS_CP949_CSV,
    dry_run: bool = False,
) -> tuple[list[dict[str, str]], int]:
    confirmed = pd.read_csv(confirmed_csv, encoding="utf-8-sig", dtype={"ticker": str})
    if confirmed.empty:
        return [], 0
    confirmed = confirmed[confirmed["flow_recheck_status"] == "confirmed"].copy()
    if confirmed.empty:
        return [], 0

    fieldnames, rows = read_observation_rows(obs_utf8_csv, "utf-8")
    existing = {observation_key(row) for row in rows}
    new_rows: list[dict[str, str]] = []

    for _, confirmed_row in confirmed.iterrows():
        row = confirmed_to_observation(confirmed_row, fieldnames)
        key = observation_key(row)
        if key in existing:
            continue
        rows.append(row)
        new_rows.append(row)
        existing.add(key)

    if not dry_run and new_rows:
        obs_utf8_csv.parent.mkdir(parents=True, exist_ok=True)
        obs_cp949_csv.parent.mkdir(parents=True, exist_ok=True)
        write_observation_rows(obs_utf8_csv, "utf-8", fieldnames, rows)
        write_observation_rows(obs_cp949_csv, "cp949", fieldnames, rows)

    return new_rows, len(confirmed)


def markdown_candidate_row(row: dict[str, str]) -> str:
    flow = row["flow_category_confirmed"]
    foreign = fmt_signed_int(row["foreign_5d"])
    institution = fmt_signed_int(row["institution_5d"])
    chg = float(row["event_chg_pct"]) if row["event_chg_pct"] else 0.0
    chg_text = f"{chg:+.2f}%"
    return (
        f"| {row['name']} | {row['ticker']} | {row['hypothesis_id']} | {row['use_type']} | "
        f"{chg_text}, {row['amount_tag']} | {flow} (외국인 {foreign} / 기관 {institution}) | "
        f"{row['decision_note']} |"
    )


def default_markdown(title: str) -> str:
    return "\n".join(
        [
            f"# {title}",
            "",
            "## 목적",
            "",
            "확정 후보가 실제로 이후 수익률로 이어지는지 추적하기 위한 관찰 로그다.",
            "",
            "## 추적 항목",
            "",
        ]
    )


def update_markdown(
    new_rows: list[dict[str, str]],
    obs_md: Path = OBS_MD,
    title: str = "일별 후보 관찰 로그",
    dry_run: bool = False,
) -> None:
    if not new_rows:
        return

    text = obs_md.read_text(encoding="utf-8-sig") if obs_md.exists() else default_markdown(title)
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in new_rows:
        grouped[row["signal_date"]].append(row)

    for signal_date in sorted(grouped):
        heading = f"## {signal_date} 확정 후보"
        rows_text = [markdown_candidate_row(row) for row in grouped[signal_date]]
        if heading in text:
            lines = text.splitlines()
            out: list[str] = []
            in_section = False
            inserted = False
            existing_lines = set(lines)
            for line in lines:
                if line == heading:
                    in_section = True
                elif in_section and line.startswith("## "):
                    for row_line in rows_text:
                        if row_line not in existing_lines:
                            out.append(row_line)
                    inserted = True
                    in_section = False
                out.append(line)
            if in_section and not inserted:
                for row_line in rows_text:
                    if row_line not in existing_lines:
                        out.append(row_line)
            text = "\n".join(out) + "\n"
            continue

        section = [
            heading,
            "",
            "| 종목 | 코드 | 가설 | 유형 | 당일 흐름 | 확정 수급 | 관찰 판단 |",
            "| --- | --- | --- | --- | --- | --- | --- |",
            *rows_text,
            "",
        ]
        marker = "## 추적 항목"
        if marker in text:
            text = text.replace(marker, "\n".join(section) + "\n" + marker, 1)
        else:
            text = text.rstrip() + "\n\n" + "\n".join(section)

    if not dry_run:
        obs_md.parent.mkdir(parents=True, exist_ok=True)
        obs_md.write_text(text, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="확정 후보를 관찰 로그에 중복 없이 추가")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--confirmed-csv", type=Path, default=CONFIRMED_CSV)
    parser.add_argument("--obs-utf8-csv", type=Path, default=OBS_UTF8_CSV)
    parser.add_argument("--obs-cp949-csv", type=Path, default=OBS_CP949_CSV)
    parser.add_argument("--obs-md", type=Path, default=OBS_MD)
    parser.add_argument("--title", default="일별 후보 관찰 로그")
    args = parser.parse_args()

    new_rows, confirmed_count = append_confirmed_to_csv(
        confirmed_csv=args.confirmed_csv,
        obs_utf8_csv=args.obs_utf8_csv,
        obs_cp949_csv=args.obs_cp949_csv,
        dry_run=args.dry_run,
    )
    update_markdown(new_rows, obs_md=args.obs_md, title=args.title, dry_run=args.dry_run)

    print(f"confirmed={confirmed_count}")
    print(f"added={len(new_rows)}")
    if new_rows:
        for row in new_rows:
            print(f"- {row['signal_date']} {row['ticker']} {row['name']} {row['hypothesis_id']}")
    print(f"observation_csv={args.obs_utf8_csv}")
    print(f"observation_md={args.obs_md}")


if __name__ == "__main__":
    main()
