from __future__ import annotations

import argparse
import asyncio
import csv
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

from discovery.data_loader import _API_DELAY, get_ohlcv_range  # noqa: E402
from discovery.investor_flow_loader import (  # noqa: E402
    add_investor_flow_features,
    get_investor_flow_range,
)
from screener_lib.universe import get_stock_universe  # noqa: E402

from analysis_paths import (  # noqa: E402
    FOREIGN_FLOW_DIR,
    FOREIGN_FLOW_SCAN_CSV,
    FOREIGN_FLOW_WATCHLIST_CSV,
    FOREIGN_FLOW_WATCHLIST_MD,
    FOREIGN_SELL_FLOW_SCAN_CSV,
    FOREIGN_SELL_FLOW_WATCHLIST_CSV,
    FOREIGN_SELL_FLOW_WATCHLIST_MD,
    OBS_FOREIGN_FLOW_CSV,
    OBS_FOREIGN_FLOW_ERROR_CSV,
    OBS_FOREIGN_FLOW_ERROR_MD,
    OBS_FOREIGN_FLOW_MD,
    OBS_FOREIGN_SELL_FLOW_CSV,
    OBS_FOREIGN_SELL_FLOW_ERROR_CSV,
    OBS_FOREIGN_SELL_FLOW_ERROR_MD,
    OBS_FOREIGN_SELL_FLOW_MD,
)

SCAN_CSV = FOREIGN_FLOW_SCAN_CSV
WATCHLIST_CSV = FOREIGN_FLOW_WATCHLIST_CSV
WATCHLIST_MD = FOREIGN_FLOW_WATCHLIST_MD
OBS_UTF8_CSV = OBS_FOREIGN_FLOW_ERROR_CSV
OBS_CP949_CSV = OBS_FOREIGN_FLOW_CSV
OBS_MD = OBS_FOREIGN_FLOW_MD

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
OBS_COLUMNS = [
    "signal_date",
    "ticker",
    "name",
    "condition_id",
    "matched_conditions",
    "foreign_net_buy_streak",
    "foreign_qty",
    "foreign_net_buy_2d_qty",
    "foreign_net_buy_3d_qty",
    "foreign_volume_ratio_pct",
    "event_close",
    "next_trading_day",
    "next_open",
    "next_open_return_pct",
    "next_close",
    "next_close_return_pct",
    "d_plus_5_close",
    "d_plus_5_return_pct",
    "d_plus_10_close",
    "d_plus_10_return_pct",
    "d_plus_20_close",
    "d_plus_20_return_pct",
    "result_label",
    "review_note",
]


@dataclass(frozen=True)
class FlowConfig:
    mode: str
    label: str
    condition_prefix: str
    scan_csv: Path
    watchlist_csv: Path
    watchlist_md: Path
    obs_utf8_csv: Path
    obs_cp949_csv: Path
    obs_md: Path
    streak_col: str
    qty_2d_col: str
    qty_3d_col: str
    all_2d_col: str
    all_3d_col: str
    ratio_col: str


BUY_CONFIG = FlowConfig(
    mode="buy",
    label="순매수",
    condition_prefix="foreign_buy",
    scan_csv=FOREIGN_FLOW_SCAN_CSV,
    watchlist_csv=FOREIGN_FLOW_WATCHLIST_CSV,
    watchlist_md=FOREIGN_FLOW_WATCHLIST_MD,
    obs_utf8_csv=OBS_FOREIGN_FLOW_ERROR_CSV,
    obs_cp949_csv=OBS_FOREIGN_FLOW_CSV,
    obs_md=OBS_FOREIGN_FLOW_MD,
    streak_col="foreign_net_buy_streak",
    qty_2d_col="foreign_net_buy_2d_qty",
    qty_3d_col="foreign_net_buy_3d_qty",
    all_2d_col="foreign_net_buy_2d_all",
    all_3d_col="foreign_net_buy_3d_all",
    ratio_col="foreign_net_buy_today_volume_ratio",
)
SELL_CONFIG = FlowConfig(
    mode="sell",
    label="순매도",
    condition_prefix="foreign_sell",
    scan_csv=FOREIGN_SELL_FLOW_SCAN_CSV,
    watchlist_csv=FOREIGN_SELL_FLOW_WATCHLIST_CSV,
    watchlist_md=FOREIGN_SELL_FLOW_WATCHLIST_MD,
    obs_utf8_csv=OBS_FOREIGN_SELL_FLOW_ERROR_CSV,
    obs_cp949_csv=OBS_FOREIGN_SELL_FLOW_CSV,
    obs_md=OBS_FOREIGN_SELL_FLOW_MD,
    streak_col="foreign_net_sell_streak",
    qty_2d_col="foreign_net_sell_2d_qty",
    qty_3d_col="foreign_net_sell_3d_qty",
    all_2d_col="foreign_net_sell_2d_all",
    all_3d_col="foreign_net_sell_3d_all",
    ratio_col="foreign_net_sell_today_volume_ratio",
)


def observation_columns(config: FlowConfig) -> list[str]:
    columns = OBS_COLUMNS.copy()
    replacements = {
        "foreign_net_buy_streak": config.streak_col,
        "foreign_net_buy_2d_qty": config.qty_2d_col,
        "foreign_net_buy_3d_qty": config.qty_3d_col,
    }
    return [replacements.get(col, col) for col in columns]


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


def format_int(value: Any) -> str:
    number = parse_float(value)
    if number is None:
        return ""
    return f"{int(round(number)):,}"


def format_ratio_pct(value: Any) -> str:
    number = parse_float(value)
    if number is None:
        return ""
    return f"{number:.2f}"


def read_rows(
    path: Path,
    encoding: str = "utf-8-sig",
    default_columns: list[str] | None = None,
) -> tuple[list[str], list[dict[str, str]]]:
    default_columns = default_columns or OBS_COLUMNS
    if not path.exists():
        return default_columns.copy(), []
    with path.open(encoding=encoding, newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = [name.lstrip("\ufeff") for name in (reader.fieldnames or default_columns)]
        rows = []
        for row in reader:
            cleaned = {}
            for key, value in row.items():
                cleaned[(key or "").lstrip("\ufeff")] = value
            rows.append(cleaned)
        return fieldnames, rows


def write_rows(
    path: Path,
    fieldnames: list[str],
    rows: list[dict[str, str]],
    encoding: str,
    errors: str = "strict",
) -> None:
    normalized_fieldnames = [field.lstrip("\ufeff") for field in fieldnames]
    for row in rows:
        for key in list(row.keys()):
            clean_key = str(key).lstrip("\ufeff")
            if clean_key != key:
                row[clean_key] = row.pop(key)
            if clean_key not in normalized_fieldnames:
                normalized_fieldnames.append(clean_key)
        for field in normalized_fieldnames:
            row.setdefault(field, "")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding=encoding, errors=errors, newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=normalized_fieldnames, lineterminator="\n")
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
    ref = d20 if d20 is not None else d10 if d10 is not None else d5 if d5 is not None else next_close
    if ref is None:
        return row.get("result_label", "")
    if ref >= 5:
        return "상승 지속"
    if ref > 0:
        return "상승"
    if ref <= -5:
        return "하락 전환"
    return "보합"


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


def markdown_table(df: pd.DataFrame, columns: list[str], max_rows: int = 50) -> str:
    if df.empty:
        return "_없음_"
    view = df[[col for col in columns if col in df.columns]].head(max_rows).fillna("")
    lines = [
        "| " + " | ".join(view.columns) + " |",
        "| " + " | ".join("---" for _ in view.columns) + " |",
    ]
    for _, row in view.iterrows():
        lines.append("| " + " | ".join(str(row[col]).replace("|", "\\|") for col in view.columns) + " |")
    return "\n".join(lines)


async def latest_ohlcv_row(ticker: str, end: pd.Timestamp) -> pd.Series | None:
    start = (end - pd.Timedelta(days=20)).strftime("%Y-%m-%d")
    df = await get_ohlcv_range(ticker, start, end.strftime("%Y-%m-%d"))
    if df.empty:
        return None
    df = df[df["date"] <= end].sort_values("date").reset_index(drop=True)
    if df.empty:
        return None
    return df.iloc[-1]


def matched_conditions(row: pd.Series, config: FlowConfig) -> list[str]:
    conditions = []
    streak = parse_float(row.get(config.streak_col)) or 0
    ratio = parse_float(row.get(config.ratio_col))
    if streak >= 2:
        conditions.append(f"{config.condition_prefix}_streak_2")
    if streak >= 3:
        conditions.append(f"{config.condition_prefix}_streak_3")
    if parse_float(row.get(config.all_2d_col)) == 1:
        conditions.append(f"{config.condition_prefix}_2d_all")
    if parse_float(row.get(config.all_3d_col)) == 1:
        conditions.append(f"{config.condition_prefix}_3d_all")
    if ratio is not None and ratio >= 0.005:
        conditions.append(f"{config.condition_prefix}_vol_0_5pct")
    if ratio is not None and ratio >= 0.01:
        conditions.append(f"{config.condition_prefix}_vol_1pct")
    if ratio is not None and ratio >= 0.02:
        conditions.append(f"{config.condition_prefix}_vol_2pct")
    return conditions


async def scan_ticker(
    stock: dict[str, Any],
    as_of: pd.Timestamp,
    min_streak: int,
    config: FlowConfig,
) -> dict[str, Any] | None:
    ticker = str(stock.get("ticker", "")).zfill(6)
    name = str(stock.get("name", "")).strip()
    price_row = await latest_ohlcv_row(ticker, as_of)
    await asyncio.sleep(_API_DELAY)
    if price_row is None:
        return None

    signal_date = pd.Timestamp(price_row["date"])
    investor = await get_investor_flow_range(
        ticker,
        start=signal_date - pd.Timedelta(days=20),
        end=signal_date,
    )
    await asyncio.sleep(_API_DELAY)

    records = pd.DataFrame(
        {
            "ticker": [ticker],
            "date": [signal_date],
            "volume": [price_row["volume"]],
        }
    )
    features = add_investor_flow_features(records, investor)
    row = features.iloc[0]
    streak = parse_float(row.get(config.streak_col)) or 0
    conditions = matched_conditions(row, config)
    if streak < min_streak:
        return None

    ratio = parse_float(row.get(config.ratio_col))
    return {
        "signal_date": signal_date.strftime("%Y-%m-%d"),
        "ticker": ticker,
        "name": name,
        "condition_id": f"{config.condition_prefix}_streak_{int(streak)}",
        "matched_conditions": ",".join(conditions),
        config.streak_col: str(int(streak)),
        "foreign_qty": fmt_price(row.get("foreign_qty")),
        config.qty_2d_col: fmt_price(row.get(config.qty_2d_col)),
        config.qty_3d_col: fmt_price(row.get(config.qty_3d_col)),
        "foreign_volume_ratio_pct": format_ratio_pct(ratio * 100 if ratio is not None else None),
        "event_close": fmt_price(price_row["close"]),
    }


async def scan_candidates(as_of: pd.Timestamp, top: int, pool_size: int, min_streak: int, config: FlowConfig) -> pd.DataFrame:
    stocks = (await get_stock_universe("amount"))[:pool_size]
    rows = []
    for idx, stock in enumerate(stocks, 1):
        candidate = await scan_ticker(stock, as_of, min_streak, config)
        if candidate is not None:
            rows.append(candidate)
        if idx % 20 == 0 or idx == len(stocks):
            print(f"[foreign-flow:{config.mode}] scanned={idx}/{len(stocks)} candidates={len(rows)}")
        if len(rows) >= top:
            break
    return pd.DataFrame(rows)


async def update_tracking_row(row: dict[str, str], as_of: pd.Timestamp) -> bool:
    signal_date = pd.Timestamp(row["signal_date"])
    end_date = as_of + pd.Timedelta(days=45)
    ticker = row["ticker"].zfill(6)
    try:
        ohlcv = await get_ohlcv_range(
            ticker,
            signal_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        )
        await asyncio.sleep(_API_DELAY)
    except RuntimeError as exc:
        print(f"tracking_skip ticker={ticker} signal_date={row['signal_date']} error={exc}")
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


def append_new_observations(existing: list[dict[str, str]], candidates: pd.DataFrame, columns: list[str]) -> int:
    seen = {(row.get("signal_date", ""), row.get("ticker", "")) for row in existing}
    added = 0
    for _, candidate in candidates.iterrows():
        key = (str(candidate["signal_date"]), str(candidate["ticker"]).zfill(6))
        if key in seen:
            continue
        row = {col: "" for col in columns}
        for col in candidates.columns:
            row[col] = as_text(candidate[col])
        existing.append(row)
        seen.add(key)
        added += 1
    return added


def build_watchlist_markdown(candidates: pd.DataFrame, as_of: pd.Timestamp, config: FlowConfig) -> str:
    lines = [
        f"# 외국인 연속 {config.label} 후보 - {as_of.strftime('%Y-%m-%d')}",
        "",
        f"- 후보 수: {len(candidates):,}",
        "",
        markdown_table(
            candidates,
            [
                "signal_date",
                "ticker",
                "name",
                config.streak_col,
                "foreign_qty",
                config.qty_2d_col,
                config.qty_3d_col,
                "foreign_volume_ratio_pct",
                "matched_conditions",
                "event_close",
            ],
        ),
        "",
    ]
    return "\n".join(lines)


def build_observation_markdown(rows: list[dict[str, str]], config: FlowConfig) -> str:
    lines = [
        f"# 외국인 연속 {config.label} 관찰 로그",
        "",
        f"외국인 {config.label}가 2거래일 이상 이어진 종목의 다음 거래일 및 이후 수익률을 누적 추적한다.",
        "",
        "| 신호일 | 종목 | 코드 | 연속일 | 조건 | 신호일 종가 | D+1 시가 | D+1 종가 | D+5 | D+10 | D+20 | 결과 | 메모 |",
        "| --- | --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for row in sorted(rows, key=lambda r: (r.get("signal_date", ""), r.get("ticker", ""))):
        lines.append(
            f"| {row.get('signal_date', '')} | {row.get('name', '')} | {row.get('ticker', '')} | "
            f"{row.get(config.streak_col, '')} | {row.get('matched_conditions', '')} | "
            f"{format_int(row.get('event_close'))} | "
            f"{format_int(row.get('next_open'))} ({signed_pct(row.get('next_open_return_pct'))}) | "
            f"{format_int(row.get('next_close'))} ({signed_pct(row.get('next_close_return_pct'))}) | "
            f"{format_int(row.get('d_plus_5_close'))} ({signed_pct(row.get('d_plus_5_return_pct'))}) | "
            f"{format_int(row.get('d_plus_10_close'))} ({signed_pct(row.get('d_plus_10_return_pct'))}) | "
            f"{format_int(row.get('d_plus_20_close'))} ({signed_pct(row.get('d_plus_20_return_pct'))}) | "
            f"{row.get('result_label', '')} | {row.get('review_note', '')} |"
        )
    lines.append("")
    return "\n".join(lines)


async def run_one(args: argparse.Namespace, config: FlowConfig) -> tuple[str, int, int, int]:
    as_of = pd.Timestamp(args.date).normalize() if args.date else pd.Timestamp.today().normalize()
    FOREIGN_FLOW_DIR.mkdir(parents=True, exist_ok=True)
    config.obs_utf8_csv.parent.mkdir(parents=True, exist_ok=True)

    candidates = await scan_candidates(as_of, args.top, args.pool_size, args.min_streak, config)
    candidates.to_csv(config.scan_csv, index=False, encoding="utf-8-sig")
    candidates.to_csv(config.watchlist_csv, index=False, encoding="utf-8-sig")
    config.watchlist_md.write_text(build_watchlist_markdown(candidates, as_of, config), encoding="utf-8")

    columns = observation_columns(config)
    fieldnames, rows = read_rows(config.obs_utf8_csv, default_columns=columns)
    for col in columns:
        if col not in fieldnames:
            fieldnames.append(col)
    added = append_new_observations(rows, candidates, columns)

    updated = 0
    for row in rows:
        if not row.get("signal_date") or not row.get("ticker"):
            continue
        if await update_tracking_row(row, as_of):
            updated += 1

    if not args.dry_run:
        write_rows(config.obs_utf8_csv, fieldnames, rows, "utf-8")
        write_rows(config.obs_cp949_csv, fieldnames, rows, "cp949", errors="replace")
        config.obs_md.write_text(build_observation_markdown(rows, config), encoding="utf-8")

    return config.mode, len(candidates), added, updated


async def run(args: argparse.Namespace) -> list[tuple[str, int, int, int]]:
    configs = []
    if args.mode in {"buy", "both"}:
        configs.append(BUY_CONFIG)
    if args.mode in {"sell", "both"}:
        configs.append(SELL_CONFIG)

    results = []
    for config in configs:
        results.append(await run_one(args, config))
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="외국인 연속 순매수/순매도 후보 자동 기록 및 D+ 추적")
    parser.add_argument("--date", help="YYYY-MM-DD. 생략하면 오늘 기준으로 KIS 최신 일봉 사용")
    parser.add_argument("--top", type=int, default=50, help="기록할 최대 후보 수")
    parser.add_argument("--pool-size", type=int, default=120, help="거래대금 상위 조회 후보 수")
    parser.add_argument("--min-streak", type=int, default=2, help="최소 외국인 연속 순매수/순매도 일수")
    parser.add_argument("--mode", choices=["buy", "sell", "both"], default="both", help="관찰 방향")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    results = asyncio.run(run(args))
    for mode, candidates, added, updated in results:
        config = BUY_CONFIG if mode == "buy" else SELL_CONFIG
        print(f"foreign_flow_{mode}_candidates={candidates}")
        print(f"foreign_flow_{mode}_observations_added={added}")
        print(f"foreign_flow_{mode}_observations_updated={updated}")
        print(f"foreign_flow_{mode}_watchlist_csv={config.watchlist_csv}")
        print(f"foreign_flow_{mode}_observation_csv={config.obs_utf8_csv}")
        print(f"foreign_flow_{mode}_observation_md={config.obs_md}")


if __name__ == "__main__":
    main()
