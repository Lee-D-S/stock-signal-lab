from __future__ import annotations

import argparse
import asyncio
import csv
import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from tmp_quarterly_stock_analysis import fetch_ohlcv  # noqa: E402


BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
OBS_DIR = BASE_DIR / "08_관찰기록"

OBS_UTF8_CSV = OBS_DIR / "관찰_로그(이상).csv"
OBS_CP949_CSV = OBS_DIR / "관찰_로그.csv"
OBS_MD = OBS_DIR / "관찰_로그.md"

D_PLUS_CLOSE_COLUMNS = {
    5: "d_plus_5_close",
    10: "d_plus_10_close",
    20: "d_plus_20_close",
}
D_PLUS_RETURN_COLUMNS = {
    5: "d_plus_5_return_pct",
    10: "d_plus_10_return_pct",
    20: "d_plus_20_return_pct",
}


def as_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value)


def parse_float(value: Any) -> float | None:
    text = as_text(value).replace(",", "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def fmt_price(value: Any) -> str:
    number = parse_float(value)
    if number is None:
        return ""
    return str(int(round(number)))


def fmt_pct(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.2f}"


def signed_pct(value: Any) -> str:
    number = parse_float(value)
    if number is None:
        return ""
    return f"{number:+.2f}%"


def calc_return(close: Any, event_close: Any) -> str:
    close_number = parse_float(close)
    event_close_number = parse_float(event_close)
    if close_number is None or event_close_number in (None, 0):
        return ""
    return fmt_pct((close_number / float(event_close_number) - 1) * 100)


def read_rows(path: Path, encoding: str = "utf-8") -> tuple[list[str], list[dict[str, str]]]:
    with path.open(encoding=encoding, newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader.fieldnames or []), list(reader)


def write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]], encoding: str) -> None:
    with path.open("w", encoding=encoding, newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def trading_row_after(ohlcv: pd.DataFrame, signal_date: pd.Timestamp, offset: int) -> pd.Series | None:
    future = ohlcv[ohlcv["date"] > signal_date].sort_values("date").reset_index(drop=True)
    index = offset - 1
    if index < 0 or index >= len(future):
        return None
    return future.iloc[index]


def result_label_for(row: dict[str, str]) -> str:
    d20 = parse_float(row.get("d_plus_20_return_pct"))
    d10 = parse_float(row.get("d_plus_10_return_pct"))
    d5 = parse_float(row.get("d_plus_5_return_pct"))
    next_close = parse_float(row.get("next_close_return_pct"))
    direction = row.get("event_direction", "")
    use_type = row.get("use_type", "")

    ref = d20 if d20 is not None else d10 if d10 is not None else d5 if d5 is not None else next_close
    if ref is None:
        return row.get("result_label", "")

    if "회피" in use_type:
        if ref <= -3:
            return "회피 성공"
        if ref >= 3:
            return "회피 실패"
        return "회피 중립"
    if direction == "down" or "반등" in use_type:
        if ref >= 5:
            return "반등 성공"
        if ref > 0:
            return "단기 반등"
        if ref <= -5:
            return "반등 실패"
        return "반등 중립"
    if direction == "up":
        if ref >= 5:
            return "상승 지속"
        if ref > 0:
            return "상승 유지"
        if ref <= -5:
            return "상승 실패"
        return "단기 되돌림"
    return row.get("result_label", "")


def review_note_for(row: dict[str, str]) -> str:
    parts = []
    if row.get("next_trading_day") and row.get("next_open_return_pct") and row.get("next_close_return_pct"):
        parts.append(
            f"D+1({row['next_trading_day']}) 시가 {signed_pct(row['next_open_return_pct'])}, "
            f"종가 {signed_pct(row['next_close_return_pct'])}"
        )
    for day, col in D_PLUS_RETURN_COLUMNS.items():
        if row.get(col):
            parts.append(f"D+{day} {signed_pct(row[col])}")
    if not parts:
        return row.get("review_note", "")
    return "; ".join(parts)


async def update_row(row: dict[str, str], as_of: pd.Timestamp, delay: float) -> bool:
    signal_date = pd.Timestamp(row["signal_date"])
    end_date = as_of + pd.Timedelta(days=45)
    ticker = row["ticker"].zfill(6)
    try:
        ohlcv = await fetch_ohlcv(
            ticker,
            signal_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        )
        await asyncio.sleep(delay)
    except Exception as exc:
        print(f"tracking_skip ticker={ticker} signal_date={row['signal_date']} error={type(exc).__name__}: {exc}")
        return False
    if ohlcv.empty:
        return False
    ohlcv = ohlcv[ohlcv["date"] <= as_of].sort_values("date").reset_index(drop=True)
    if ohlcv.empty:
        return False

    changed = False
    event_close = row.get("event_close")
    next_row = trading_row_after(ohlcv, signal_date, 1)
    if next_row is not None:
        values = {
            "next_trading_day": pd.Timestamp(next_row["date"]).strftime("%Y-%m-%d"),
            "next_open": fmt_price(next_row["open"]),
            "next_close": fmt_price(next_row["close"]),
        }
        values["next_open_return_pct"] = calc_return(values["next_open"], event_close)
        values["next_close_return_pct"] = calc_return(values["next_close"], event_close)
        for key, value in values.items():
            if row.get(key, "") != value:
                row[key] = value
                changed = True

    for day, close_col in D_PLUS_CLOSE_COLUMNS.items():
        drow = trading_row_after(ohlcv, signal_date, day)
        if drow is None:
            continue
        close = fmt_price(drow["close"])
        return_col = D_PLUS_RETURN_COLUMNS[day]
        ret = calc_return(close, event_close)
        if row.get(close_col, "") != close:
            row[close_col] = close
            changed = True
        if row.get(return_col, "") != ret:
            row[return_col] = ret
            changed = True

    label = result_label_for(row)
    note = review_note_for(row)
    if label and row.get("result_label", "") != label:
        row["result_label"] = label
        changed = True
    if note and row.get("review_note", "") != note:
        row["review_note"] = note
        changed = True
    return changed


def format_int(value: Any) -> str:
    number = parse_float(value)
    if number is None:
        return ""
    return f"{int(round(number)):,}"


def build_markdown(rows: list[dict[str, str]]) -> str:
    lines = [
        "# 일별 후보 관찰 로그",
        "",
        "## 목적",
        "",
        "일별 감시 조건을 통과한 후보가 실제로 이후 수익으로 이어지는지 누적 확인한다. 이 문서는 매수 추천 기록이 아니라, 과거 패턴에서 나온 후보를 실전 환경에서 검증하기 위한 관찰 기록이다.",
        "",
    ]

    by_date: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        by_date.setdefault(row["signal_date"], []).append(row)

    for signal_date in sorted(by_date):
        lines.extend(
            [
                f"## {signal_date} 확정 후보",
                "",
                "| 종목 | 코드 | 가설 | 유형 | 당일 흐름 | 확정 수급 | 관찰 판단 |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in by_date[signal_date]:
            chg = signed_pct(row.get("event_chg_pct"))
            flow = row.get("flow_category_confirmed", "")
            foreign = format_int(row.get("foreign_5d"))
            institution = format_int(row.get("institution_5d"))
            lines.append(
                f"| {row['name']} | {row['ticker']} | {row['hypothesis_id']} | {row['use_type']} | "
                f"{chg}, {row.get('amount_tag', '')} | {flow} (외국인 {foreign} / 기관 {institution}) | "
                f"{row.get('decision_note', '')} |"
            )
        lines.append("")

    lines.extend(
        [
            "## 자동 D+ 추적 현황",
            "",
            "| 신호일 | 종목 | 가설 | 신호일 종가 | D+1 시가 | D+1 종가 | D+5 | D+10 | D+20 | 결과 라벨 | 관찰 메모 |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
        ]
    )
    for row in sorted(rows, key=lambda r: (r["signal_date"], r["ticker"], r["hypothesis_id"])):
        lines.append(
            f"| {row['signal_date']} | {row['name']} | {row['hypothesis_id']} | {format_int(row.get('event_close'))} | "
            f"{format_int(row.get('next_open'))} ({signed_pct(row.get('next_open_return_pct'))}) | "
            f"{format_int(row.get('next_close'))} ({signed_pct(row.get('next_close_return_pct'))}) | "
            f"{format_int(row.get('d_plus_5_close'))} ({signed_pct(row.get('d_plus_5_return_pct'))}) | "
            f"{format_int(row.get('d_plus_10_close'))} ({signed_pct(row.get('d_plus_10_return_pct'))}) | "
            f"{format_int(row.get('d_plus_20_close'))} ({signed_pct(row.get('d_plus_20_return_pct'))}) | "
            f"{row.get('result_label', '')} | {row.get('review_note', '')} |"
        )
    lines.extend(
        [
            "",
            "## 운영 원칙",
            "",
            "- 확정 후보만 관찰 로그에 남긴다.",
            "- `flow_check_required` 또는 `rejected_flow_mismatch` 후보는 로그에 남기지 않는다.",
            "- 실제 진입 여부와 무관하게 다음 거래일/D+5/D+10/D+20 결과는 채운다.",
            "- 한 번의 성공 또는 실패로 전략을 확정하지 않는다. 최소 20건 이상 누적 후 후보별 성과를 다시 본다.",
            "",
        ]
    )
    return "\n".join(lines)


async def run(as_of: pd.Timestamp, delay: float, dry_run: bool) -> tuple[int, int]:
    fieldnames, rows = read_rows(OBS_UTF8_CSV)
    changed_count = 0
    for row in rows:
        if not row.get("signal_date") or not row.get("ticker"):
            continue
        changed = await update_row(row, as_of, delay)
        if changed:
            changed_count += 1

    if not dry_run and changed_count:
        write_rows(OBS_UTF8_CSV, fieldnames, rows, "utf-8")
        write_rows(OBS_CP949_CSV, fieldnames, rows, "cp949")
        OBS_MD.write_text(build_markdown(rows), encoding="utf-8")
    return len(rows), changed_count


def main() -> None:
    parser = argparse.ArgumentParser(description="관찰 로그 D+1/D+5/D+10/D+20 추적값 자동 업데이트")
    parser.add_argument("--as-of", help="YYYY-MM-DD. 생략하면 오늘")
    parser.add_argument("--delay", type=float, default=0.35)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    as_of = pd.Timestamp(args.as_of) if args.as_of else pd.Timestamp.today().normalize()
    total, changed = asyncio.run(run(as_of, args.delay, args.dry_run))
    print(f"observations={total}")
    print(f"updated={changed}")
    print(f"as_of={as_of.strftime('%Y-%m-%d')}")
    print(f"observation_csv={OBS_UTF8_CSV}")
    print(f"observation_md={OBS_MD}")


if __name__ == "__main__":
    main()
