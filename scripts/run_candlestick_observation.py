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

from candlestick_patterns import detect_all  # noqa: E402
from tmp_quarterly_stock_analysis import fetch_ohlcv  # noqa: E402


BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
STRATEGY_DIR = BASE_DIR / "07_전략신호"
OBS_DIR = BASE_DIR / "08_관찰기록"

UNIVERSE_CSV = STRATEGY_DIR / "거래대금_상위_유니버스.csv"
SCAN_CSV = STRATEGY_DIR / "캔들_패턴_스캔.csv"
SCAN_MD = STRATEGY_DIR / "캔들_패턴_스캔.md"
OBS_CSV = OBS_DIR / "캔들_관찰_로그.csv"
OBS_MD = OBS_DIR / "캔들_관찰_로그.md"
SUMMARY_CSV = OBS_DIR / "캔들_패턴_성과_요약.csv"
SUMMARY_MD = OBS_DIR / "캔들_패턴_성과_요약.md"

DEFAULT_COMPANIES = [
    ("005930", "삼성전자"),
    ("000660", "SK하이닉스"),
    ("047040", "대우건설"),
    ("006400", "삼성SDI"),
    ("005490", "POSCO홀딩스"),
    ("001440", "대한전선"),
    ("042700", "한미반도체"),
    ("009150", "삼성전기"),
    ("222080", "씨아이에스"),
    ("066570", "LG전자"),
    ("267260", "HD현대일렉트릭"),
    ("034020", "두산에너빌리티"),
    ("012450", "한화에어로스페이스"),
    ("028050", "삼성E&A"),
    ("298040", "효성중공업"),
]

FIELDNAMES = [
    "signal_date",
    "ticker",
    "name",
    "pattern_id",
    "pattern_name",
    "category",
    "prediction_direction",
    "confidence",
    "basis",
    "signal_open",
    "signal_high",
    "signal_low",
    "signal_close",
    "signal_volume",
    "signal_trade_amount",
    "next_trading_day",
    "next_close",
    "d_plus_5_close",
    "d_plus_10_close",
    "d_plus_20_close",
    "next_close_return_pct",
    "d_plus_5_return_pct",
    "d_plus_10_return_pct",
    "d_plus_20_return_pct",
    "match_label",
    "review_note",
]

RETURN_COLUMNS = [
    "next_close_return_pct",
    "d_plus_5_return_pct",
    "d_plus_10_return_pct",
    "d_plus_20_return_pct",
]


def as_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def parse_float(value: Any) -> float | None:
    text = as_text(value).replace(",", "")
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


def load_companies(universe_csv: Path = UNIVERSE_CSV, limit: int | None = None) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []
    if universe_csv.exists():
        df = pd.read_csv(universe_csv, encoding="utf-8-sig", dtype={"ticker": str})
        if {"ticker", "name"}.issubset(df.columns):
            for _, row in df.iterrows():
                ticker = as_text(row.get("ticker")).zfill(6)
                name = as_text(row.get("name"))
                if ticker and name:
                    rows.append((ticker, name))
    if not rows:
        rows = DEFAULT_COMPANIES.copy()
    return rows[:limit] if limit else rows


def read_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not path.exists():
        return FIELDNAMES.copy(), []
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        for field in FIELDNAMES:
            if field not in fieldnames:
                fieldnames.append(field)
        rows = list(reader)
    for row in rows:
        for field in fieldnames:
            row.setdefault(field, "")
    return fieldnames, rows


def write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def observation_key(row: dict[str, str]) -> tuple[str, str, str]:
    return (row["signal_date"], row["ticker"].zfill(6), row["pattern_id"])


def signal_to_observation(row: dict[str, Any], fieldnames: list[str]) -> dict[str, str]:
    values = {
        "signal_date": as_text(row.get("signal_date")),
        "ticker": as_text(row.get("ticker")).zfill(6),
        "name": as_text(row.get("name")),
        "pattern_id": as_text(row.get("pattern_id")),
        "pattern_name": as_text(row.get("pattern_name")),
        "category": as_text(row.get("category")),
        "prediction_direction": as_text(row.get("prediction_direction")),
        "confidence": as_text(row.get("confidence")),
        "basis": as_text(row.get("basis")),
        "signal_open": fmt_price(row.get("open")),
        "signal_high": fmt_price(row.get("high")),
        "signal_low": fmt_price(row.get("low")),
        "signal_close": fmt_price(row.get("close")),
        "signal_volume": fmt_price(row.get("volume")),
        "signal_trade_amount": fmt_price(row.get("trade_amount")),
    }
    return {field: values.get(field, "") for field in fieldnames}


def trading_row_after(ohlcv: pd.DataFrame, signal_date: pd.Timestamp, offset: int) -> pd.Series | None:
    future = ohlcv[ohlcv["date"] > signal_date].sort_values("date").reset_index(drop=True)
    index = offset - 1
    if index < 0 or index >= len(future):
        return None
    return future.iloc[index]


def calc_return(close: Any, base_close: Any) -> str:
    close_number = parse_float(close)
    base_number = parse_float(base_close)
    if close_number is None or base_number in (None, 0):
        return ""
    return fmt_pct((close_number / float(base_number) - 1) * 100)


def match_label(row: dict[str, str]) -> str:
    direction = row.get("prediction_direction", "")
    ref = (
        parse_float(row.get("d_plus_20_return_pct"))
        or parse_float(row.get("d_plus_10_return_pct"))
        or parse_float(row.get("d_plus_5_return_pct"))
        or parse_float(row.get("next_close_return_pct"))
    )
    if ref is None:
        return row.get("match_label", "")
    if direction == "up":
        if ref > 0:
            return "예측 일치"
        if ref < 0:
            return "예측 불일치"
        return "중립"
    if direction == "down":
        if ref < 0:
            return "예측 일치"
        if ref > 0:
            return "예측 불일치"
        return "중립"
    return "방향 없음"


def review_note(row: dict[str, str]) -> str:
    parts = []
    if row.get("next_trading_day") and row.get("next_close_return_pct"):
        parts.append(f"D+1({row['next_trading_day']}) {signed_pct(row['next_close_return_pct'])}")
    if row.get("d_plus_5_return_pct"):
        parts.append(f"D+5 {signed_pct(row['d_plus_5_return_pct'])}")
    if row.get("d_plus_10_return_pct"):
        parts.append(f"D+10 {signed_pct(row['d_plus_10_return_pct'])}")
    if row.get("d_plus_20_return_pct"):
        parts.append(f"D+20 {signed_pct(row['d_plus_20_return_pct'])}")
    return "; ".join(parts)


async def fetch_company_patterns(
    ticker: str,
    name: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    target_date: pd.Timestamp | None,
) -> list[dict[str, Any]]:
    ohlcv = await fetch_ohlcv(ticker, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
    if ohlcv.empty:
        return []
    patterns = detect_all(ohlcv)
    if target_date is not None:
        latest = ohlcv[ohlcv["date"] <= target_date].sort_values("date").tail(1)
        if latest.empty:
            return []
        target_text = pd.Timestamp(latest.iloc[0]["date"]).strftime("%Y-%m-%d")
        patterns = [row for row in patterns if row["signal_date"] == target_text]
    for row in patterns:
        row["ticker"] = ticker
        row["name"] = name
    return patterns


async def scan(args: argparse.Namespace) -> pd.DataFrame:
    end = pd.Timestamp(args.date).normalize() if args.date else pd.Timestamp.today().normalize()
    start = end - pd.Timedelta(days=args.lookback_days)
    target_date = end if not args.backfill else None
    companies = load_companies(args.universe_csv, args.limit)
    rows: list[dict[str, Any]] = []
    errors = 0
    for ticker, name in companies:
        try:
            rows.extend(await fetch_company_patterns(ticker, name, start, end, target_date))
        except Exception as exc:
            errors += 1
            print(f"scan_skip ticker={ticker} name={name} error={type(exc).__name__}: {exc}")
        await asyncio.sleep(args.delay)
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["signal_date", "ticker", "confidence", "pattern_id"], ascending=[True, True, False, True])
    if not args.dry_run:
        args.scan_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(args.scan_csv, index=False, encoding="utf-8-sig")
        args.scan_md.write_text(build_scan_markdown(df, end.strftime("%Y-%m-%d"), len(companies), errors), encoding="utf-8")
    print(f"universe_count={len(companies)}")
    print(f"patterns={len(df)}")
    print(f"errors={errors}")
    print(f"scan_csv={args.scan_csv}")
    print(f"scan_md={args.scan_md}")
    return df


def append_observations(signals: pd.DataFrame, obs_csv: Path, dry_run: bool) -> int:
    fieldnames, rows = read_rows(obs_csv)
    existing = {observation_key(row) for row in rows}
    added = 0
    for _, signal in signals.iterrows():
        row = signal_to_observation(signal.to_dict(), fieldnames)
        key = observation_key(row)
        if key in existing:
            continue
        rows.append(row)
        existing.add(key)
        added += 1
    if added and not dry_run:
        write_rows(obs_csv, fieldnames, rows)
    return added


async def update_observations(args: argparse.Namespace) -> tuple[int, int]:
    as_of = pd.Timestamp(args.as_of).normalize() if args.as_of else pd.Timestamp.today().normalize()
    fieldnames, rows = read_rows(args.obs_csv)
    changed = 0
    for row in rows:
        if not row.get("signal_date") or not row.get("ticker"):
            continue
        signal_date = pd.Timestamp(row["signal_date"])
        end = max(as_of, signal_date + pd.Timedelta(days=45))
        try:
            ohlcv = await fetch_ohlcv(row["ticker"].zfill(6), signal_date.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))
        except Exception as exc:
            print(f"update_skip ticker={row['ticker']} signal_date={row['signal_date']} error={type(exc).__name__}: {exc}")
            continue
        await asyncio.sleep(args.delay)
        if ohlcv.empty:
            continue
        ohlcv = ohlcv[ohlcv["date"] <= as_of].sort_values("date").reset_index(drop=True)
        base_close = row.get("signal_close")
        values: dict[str, str] = {}
        next_row = trading_row_after(ohlcv, signal_date, 1)
        if next_row is not None:
            values["next_trading_day"] = pd.Timestamp(next_row["date"]).strftime("%Y-%m-%d")
            values["next_close"] = fmt_price(next_row["close"])
            values["next_close_return_pct"] = calc_return(values["next_close"], base_close)
        for day in (5, 10, 20):
            future_row = trading_row_after(ohlcv, signal_date, day)
            if future_row is None:
                continue
            close_col = f"d_plus_{day}_close"
            return_col = f"d_plus_{day}_return_pct"
            values[close_col] = fmt_price(future_row["close"])
            values[return_col] = calc_return(values[close_col], base_close)
        for key, value in values.items():
            if row.get(key, "") != value:
                row[key] = value
                changed += 1
        label = match_label(row)
        note = review_note(row)
        if label and row.get("match_label", "") != label:
            row["match_label"] = label
            changed += 1
        if note and row.get("review_note", "") != note:
            row["review_note"] = note
            changed += 1
    if changed and not args.dry_run:
        write_rows(args.obs_csv, fieldnames, rows)
    print(f"observations={len(rows)}")
    print(f"updated_fields={changed}")
    print(f"obs_csv={args.obs_csv}")
    return len(rows), changed


def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "pattern_id",
        "pattern_name",
        "category",
        "prediction_direction",
        "sample_count",
        "completed_count",
        "match_count",
        "mismatch_count",
        "match_rate_pct",
        "next_close_avg_return_pct",
        "d_plus_5_avg_return_pct",
        "d_plus_10_avg_return_pct",
        "d_plus_20_avg_return_pct",
        "result_status",
    ]
    if df.empty:
        return pd.DataFrame(columns=columns)
    rows: list[dict[str, Any]] = []
    for keys, group in df.groupby(["pattern_id", "pattern_name", "category", "prediction_direction"], dropna=False):
        labels = group.get("match_label", pd.Series(dtype=str)).dropna().astype(str).str.strip()
        completed = labels[labels != ""]
        match_count = int((completed == "예측 일치").sum())
        mismatch_count = int((completed == "예측 불일치").sum())
        row: dict[str, Any] = {
            "pattern_id": keys[0],
            "pattern_name": keys[1],
            "category": keys[2],
            "prediction_direction": keys[3],
            "sample_count": len(group),
            "completed_count": len(completed),
            "match_count": match_count,
            "mismatch_count": mismatch_count,
            "match_rate_pct": round(match_count / len(completed) * 100, 1) if len(completed) else "",
            "result_status": "표본 부족" if len(group) < 20 else "검토 가능",
        }
        for column in RETURN_COLUMNS:
            values = pd.to_numeric(group.get(column, pd.Series(dtype=str)).astype(str).str.replace(",", "", regex=False), errors="coerce").dropna()
            row[f"{column.replace('_return_pct', '')}_avg_return_pct"] = round(float(values.mean()), 2) if not values.empty else ""
        rows.append(row)
    return pd.DataFrame(rows, columns=columns).sort_values(["result_status", "pattern_id"]).reset_index(drop=True)


def markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_없음_"
    view = df.fillna("").copy()
    lines = [
        "| " + " | ".join(view.columns) + " |",
        "| " + " | ".join("---" for _ in view.columns) + " |",
    ]
    for _, row in view.iterrows():
        lines.append("| " + " | ".join(str(row[col]).replace("|", "\\|") for col in view.columns) + " |")
    return "\n".join(lines)


def build_scan_markdown(df: pd.DataFrame, target_date: str, universe_count: int, errors: int) -> str:
    view_cols = ["signal_date", "ticker", "name", "pattern_name", "category", "prediction_direction", "confidence", "basis", "close"]
    lines = [
        "# 캔들 패턴 스캔",
        "",
        f"- 기준일: {target_date}",
        f"- 스캔 종목 수: {universe_count:,}",
        f"- 탐지 패턴 수: {len(df):,}",
        f"- 오류 종목 수: {errors:,}",
        "- 이 파일은 OHLC 캔들만으로 만든 방향성 관찰 후보이며 매수 지시가 아니다.",
        "",
        "## 탐지 결과",
        "",
        markdown_table(df[[col for col in view_cols if col in df.columns]] if not df.empty else df),
        "",
    ]
    return "\n".join(lines)


def build_observation_markdown(df: pd.DataFrame) -> str:
    view_cols = [
        "signal_date",
        "ticker",
        "name",
        "pattern_name",
        "prediction_direction",
        "signal_close",
        "next_close_return_pct",
        "d_plus_5_return_pct",
        "d_plus_10_return_pct",
        "d_plus_20_return_pct",
        "match_label",
        "review_note",
    ]
    return "\n".join(
        [
            "# 캔들 관찰 로그",
            "",
            "## 목적",
            "",
            "캔들 패턴이 예측한 방향과 실제 이후 주가 방향이 어느 정도 일치하는지 누적 관찰한다.",
            "",
            "## 관찰 현황",
            "",
            markdown_table(df[[col for col in view_cols if col in df.columns]] if not df.empty else df),
            "",
        ]
    )


def build_summary_markdown(source: pd.DataFrame, summary: pd.DataFrame) -> str:
    completed = 0
    if not source.empty and "match_label" in source.columns:
        completed = int(source["match_label"].dropna().astype(str).str.strip().ne("").sum())
    return "\n".join(
        [
            "# 캔들 패턴 성과 요약",
            "",
            "## 전체 현황",
            "",
            f"- 누적 관찰 건수: {len(source):,}",
            f"- 결과 입력 건수: {completed:,}",
            f"- 패턴 그룹 수: {len(summary):,}",
            "",
            "## 패턴별 성과",
            "",
            markdown_table(summary),
            "",
            "## 해석 기준",
            "",
            "- `match_rate_pct`는 예측 방향과 실제 기준 수익률 방향이 일치한 비율이다.",
            "- 기준 수익률은 D+20, D+10, D+5, D+1 순서로 사용 가능한 가장 긴 값을 쓴다.",
            "- 표본이 20건 미만이면 결론을 내리지 않고 `표본 부족`으로 둔다.",
            "",
        ]
    )


def refresh_markdown_and_summary(args: argparse.Namespace) -> None:
    _, rows = read_rows(args.obs_csv)
    df = pd.DataFrame(rows)
    summary = build_summary(df)
    if not args.dry_run:
        args.obs_md.parent.mkdir(parents=True, exist_ok=True)
        args.summary_csv.parent.mkdir(parents=True, exist_ok=True)
        args.obs_md.write_text(build_observation_markdown(df), encoding="utf-8")
        summary.to_csv(args.summary_csv, index=False, encoding="utf-8-sig")
        args.summary_md.write_text(build_summary_markdown(df, summary), encoding="utf-8")
    print(f"summary_rows={len(summary)}")
    print(f"obs_md={args.obs_md}")
    print(f"summary_csv={args.summary_csv}")
    print(f"summary_md={args.summary_md}")


async def main() -> None:
    parser = argparse.ArgumentParser(description="캔들 패턴 스캔과 예측 일치도 관찰")
    parser.add_argument("--mode", choices=["scan", "update", "summary", "daily"], default="daily")
    parser.add_argument("--date", help="스캔 기준일 YYYY-MM-DD. 생략하면 오늘")
    parser.add_argument("--as-of", help="관찰 업데이트 기준일 YYYY-MM-DD. 생략하면 오늘")
    parser.add_argument("--lookback-days", type=int, default=220)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--delay", type=float, default=0.35)
    parser.add_argument("--backfill", action="store_true", help="기준일 하루가 아니라 조회 기간 전체 패턴을 관찰 로그에 추가")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--universe-csv", type=Path, default=UNIVERSE_CSV)
    parser.add_argument("--scan-csv", type=Path, default=SCAN_CSV)
    parser.add_argument("--scan-md", type=Path, default=SCAN_MD)
    parser.add_argument("--obs-csv", type=Path, default=OBS_CSV)
    parser.add_argument("--obs-md", type=Path, default=OBS_MD)
    parser.add_argument("--summary-csv", type=Path, default=SUMMARY_CSV)
    parser.add_argument("--summary-md", type=Path, default=SUMMARY_MD)
    args = parser.parse_args()

    if args.mode in {"scan", "daily"}:
        signals = await scan(args)
        added = append_observations(signals, args.obs_csv, args.dry_run)
        label = "observations_would_add" if args.dry_run else "observations_added"
        print(f"{label}={added}")
    if args.mode in {"update", "daily"}:
        await update_observations(args)
    if args.mode in {"summary", "update", "daily"}:
        refresh_markdown_and_summary(args)


if __name__ == "__main__":
    asyncio.run(main())
