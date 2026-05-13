"""NTM Forward PER 일별 관찰 기록.

KIS 현재가와 종목추정실적을 이용해 가능한 경우 NTM EPS와 NTM PER을 계산한다.
추정 EPS 필드가 불명확하거나 누락된 종목도 상태를 남겨 데이터 커버리지를 추적한다.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv
load_dotenv()

import pandas as pd

from analysis_paths import (  # noqa: E402
    OBS_NTM_PER_CSV,
    OBS_NTM_PER_MD,
    OBS_NTM_PER_RAW_JSONL,
    OBS_NTM_PER_SUMMARY_CSV,
    OBS_VALUATION_DIR,
    UNIVERSE_CSV,
)

LOG_COLS = [
    "signal_date",
    "ticker",
    "name",
    "current_price",
    "trailing_per",
    "trailing_eps",
    "ntm_eps",
    "ntm_per",
    "eps_year_1",
    "eps_year_2",
    "eps_source",
    "estimate_status",
    "ntm_per_status",
    "peg_growth_5y_pct",
    "peg",
    "peg_status",
    "review_note",
]

SUMMARY_COLS = [
    "run_date",
    "pool_size",
    "rows_written",
    "ntm_per_available",
    "peg_available",
    "estimate_missing",
    "eps_nonpositive",
    "peg_over_1_5",
]


def _to_float(value: Any) -> float | None:
    if value in ("", None):
        return None
    try:
        text = str(value).replace(",", "").replace("%", "").strip()
        if not text or text in {"-", "N/A", "nan", "None"}:
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def _flatten_outputs(raw: dict | None) -> list[dict[str, Any]]:
    if not raw:
        return []
    rows: list[dict[str, Any]] = []
    for key in ("output", "output1", "output2", "output3", "output4"):
        value = raw.get(key)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    rows.append({"_output": key, **item})
        elif isinstance(value, dict):
            rows.append({"_output": key, **value})
    return rows


def _row_text(row: dict[str, Any]) -> str:
    label_keys = [
        "name",
        "nm",
        "title",
        "account",
        "account_nm",
        "item",
        "item_nm",
        "item_kor_nm",
        "output3",
        "output4",
    ]
    return " ".join(str(row.get(k, "")) for k in label_keys if row.get(k)).lower()


def _period_key(row: dict[str, Any], key: str) -> str:
    for candidate in ("dt", "estdate", "year", "yyyy", "stac_yymm", "fs_year", "fiscal_year"):
        if row.get(candidate):
            return str(row[candidate])
    return key


def _extract_eps_points(rows: list[dict[str, Any]]) -> list[tuple[str, float, str]]:
    points: list[tuple[str, float, str]] = []
    direct_keys = {
        "eps",
        "est_eps",
        "forward_eps",
        "fwd_eps",
        "ntm_eps",
        "eps1",
        "eps2",
        "eps_1",
        "eps_2",
    }
    label_tokens = ("eps", "주당순이익", "예상eps", "추정eps")

    for row in rows:
        output = str(row.get("_output", ""))
        for key, value in row.items():
            lower_key = key.lower()
            if lower_key in direct_keys or ("eps" in lower_key and "per" not in lower_key):
                number = _to_float(value)
                if number is not None:
                    points.append((_period_key(row, key), number, f"{output}.{key}"))

        text = _row_text(row)
        if any(token in text for token in label_tokens):
            for key in ("data1", "data2", "data3", "data4", "data5"):
                number = _to_float(row.get(key))
                if number is not None:
                    points.append((_period_key(row, key), number, f"{output}.{key}"))

    deduped: list[tuple[str, float, str]] = []
    seen: set[tuple[str, float]] = set()
    for period, value, source in points:
        marker = (period, value)
        if marker not in seen:
            deduped.append((period, value, source))
            seen.add(marker)
    return deduped


def _extract_growth_5y(rows: list[dict[str, Any]]) -> tuple[float | None, str]:
    for row in rows:
        output = str(row.get("_output", ""))
        for key, value in row.items():
            lower_key = key.lower()
            if "growth" in lower_key or "cagr" in lower_key or "grw" in lower_key:
                number = _to_float(value)
                if number is not None:
                    return number, f"{output}.{key}"
        text = _row_text(row)
        if "5" in text and ("성장" in text or "growth" in text or "cagr" in text):
            for key in ("data1", "data2", "data3", "data4", "data5"):
                number = _to_float(row.get(key))
                if number is not None:
                    return number, f"{output}.{key}"
    return None, ""


def _sort_eps_points(points: list[tuple[str, float, str]]) -> list[tuple[str, float, str]]:
    def _period_year(period: str) -> int:
        digits = "".join(ch for ch in period if ch.isdigit())
        return int(digits[:4]) if len(digits) >= 4 else 9999

    def _sort_key(item: tuple[str, float, str]) -> tuple[int, str]:
        period = item[0]
        return _period_year(period), period

    return sorted(points, key=_sort_key)


def _ntm_eps(points: list[tuple[str, float, str]], as_of: date) -> tuple[float | None, float | None, float | None, str]:
    def _period_year(period: str) -> int:
        digits = "".join(ch for ch in period if ch.isdigit())
        return int(digits[:4]) if len(digits) >= 4 else 9999

    positives = [
        point
        for point in _sort_eps_points(points)
        if point[1] > 0 and _period_year(point[0]) >= as_of.year
    ]
    if not positives:
        return None, None, None, ""
    if len(positives) == 1:
        period, value, source = positives[0]
        return value, value, None, f"{source}:{period}"

    current = positives[0]
    next_year = positives[1]
    current_weight = max(0.0, min(1.0, (date(as_of.year, 12, 31) - as_of).days / 365.0))
    next_weight = 1.0 - current_weight
    ntm = current[1] * current_weight + next_year[1] * next_weight
    source = f"{current[2]}:{current[0]} {current_weight:.2f} + {next_year[2]}:{next_year[0]} {next_weight:.2f}"
    return round(ntm, 4), current[1], next_year[1], source


def _ntm_status(ntm_per: float | None, ntm_eps: float | None, has_estimate: bool) -> str:
    if not has_estimate:
        return "estimate_missing"
    if ntm_eps is None or ntm_eps <= 0:
        return "eps_nonpositive"
    if ntm_per is None:
        return "ntm_per_missing"
    if ntm_per >= 50:
        return "extreme"
    if ntm_per >= 35:
        return "high"
    if ntm_per >= 20:
        return "elevated"
    return "normal"


def _peg_status(peg: float | None) -> str:
    if peg is None:
        return ""
    if peg >= 1.5:
        return "peg_over_1_5"
    if peg >= 1.0:
        return "peg_1_0_to_1_5"
    return "peg_below_1_0"


def _load_universe(limit: int) -> list[dict[str, Any]]:
    if not UNIVERSE_CSV.exists():
        return []
    df = pd.read_csv(UNIVERSE_CSV, encoding="utf-8-sig", dtype={"ticker": str})
    if "rank" in df.columns:
        df = df.sort_values("rank")
    return df.head(limit).to_dict("records")


def _load_log() -> pd.DataFrame:
    if OBS_NTM_PER_CSV.exists():
        return pd.read_csv(OBS_NTM_PER_CSV, encoding="utf-8-sig", dtype={"ticker": str})
    return pd.DataFrame(columns=LOG_COLS)


def _save_log(df: pd.DataFrame) -> None:
    OBS_VALUATION_DIR.mkdir(parents=True, exist_ok=True)
    out = df[[col for col in LOG_COLS if col in df.columns]].copy()
    out.to_csv(OBS_NTM_PER_CSV, index=False, encoding="utf-8-sig")

    lines = ["# NTM PER 관찰 로그", ""]
    if out.empty:
        lines.append("_없음_")
    else:
        view = out.tail(100)
        lines.append(view.to_markdown(index=False))
    OBS_NTM_PER_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _append_summary(run_date: str, pool_size: int, rows: list[dict[str, Any]]) -> None:
    OBS_VALUATION_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(OBS_NTM_PER_SUMMARY_CSV, encoding="utf-8-sig") if OBS_NTM_PER_SUMMARY_CSV.exists() else pd.DataFrame(columns=SUMMARY_COLS)
    df = df[df["run_date"].astype(str) != run_date]
    row = {
        "run_date": run_date,
        "pool_size": pool_size,
        "rows_written": len(rows),
        "ntm_per_available": sum(1 for r in rows if pd.notna(r.get("ntm_per"))),
        "peg_available": sum(1 for r in rows if pd.notna(r.get("peg"))),
        "estimate_missing": sum(1 for r in rows if r.get("estimate_status") == "missing"),
        "eps_nonpositive": sum(1 for r in rows if r.get("ntm_per_status") == "eps_nonpositive"),
        "peg_over_1_5": sum(1 for r in rows if r.get("peg_status") == "peg_over_1_5"),
    }
    pd.concat([df, pd.DataFrame([row])], ignore_index=True).to_csv(OBS_NTM_PER_SUMMARY_CSV, index=False, encoding="utf-8-sig")


async def _build_row(stock: dict[str, Any], as_of: date, write_raw: bool) -> dict[str, Any]:
    from screener_lib.data import get_kis_estimate_performance, get_kis_quote_snapshot

    ticker = str(stock.get("ticker", "")).zfill(6)
    name = str(stock.get("name", ""))
    quote = await get_kis_quote_snapshot(ticker) or {}
    raw = await get_kis_estimate_performance(ticker)

    if write_raw and raw:
        with OBS_NTM_PER_RAW_JSONL.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"signal_date": as_of.isoformat(), "ticker": ticker, "name": name, "raw": raw}, ensure_ascii=False) + "\n")

    rows = _flatten_outputs(raw)
    eps_points = _extract_eps_points(rows)
    ntm_eps, eps1, eps2, eps_source = _ntm_eps(eps_points, as_of)
    growth, growth_source = _extract_growth_5y(rows)

    price = quote.get("price") or _to_float(stock.get("price")) or _to_float(stock.get("close"))
    ntm_per = round(price / ntm_eps, 2) if price and ntm_eps and ntm_eps > 0 else None
    peg = round(ntm_per / growth, 2) if ntm_per and growth and growth > 0 else None
    estimate_status = "ok" if eps_points else "missing"

    return {
        "signal_date": as_of.isoformat(),
        "ticker": ticker,
        "name": name,
        "current_price": price,
        "trailing_per": quote.get("per"),
        "trailing_eps": quote.get("eps"),
        "ntm_eps": ntm_eps,
        "ntm_per": ntm_per,
        "eps_year_1": eps1,
        "eps_year_2": eps2,
        "eps_source": eps_source,
        "estimate_status": estimate_status,
        "ntm_per_status": _ntm_status(ntm_per, ntm_eps, bool(eps_points)),
        "peg_growth_5y_pct": growth,
        "peg": peg,
        "peg_status": _peg_status(peg),
        "review_note": f"growth_source={growth_source}" if growth_source else "",
    }


async def main() -> None:
    parser = argparse.ArgumentParser(description="NTM PER 일별 관찰 기록")
    parser.add_argument("--date", default=None, help="기준일 YYYY-MM-DD (기본: 오늘)")
    parser.add_argument("--pool-size", type=int, default=80, help="거래대금 유니버스 상위 N개")
    parser.add_argument("--delay", type=float, default=0.35)
    parser.add_argument("--write-raw", action="store_true", help="KIS 추정실적 원본 응답을 jsonl로 저장")
    args = parser.parse_args()

    as_of = date.fromisoformat(args.date) if args.date else date.today()
    stocks = _load_universe(args.pool_size)
    if not stocks:
        print("[ntm-per] 유니버스 파일 없음. run_daily_universe_refresh.py를 먼저 실행하세요.")
        return

    if args.write_raw:
        OBS_VALUATION_DIR.mkdir(parents=True, exist_ok=True)
        OBS_NTM_PER_RAW_JSONL.write_text("", encoding="utf-8")

    print(f"[ntm-per] {len(stocks)}개 종목 NTM PER 조회 중...")
    rows: list[dict[str, Any]] = []
    for idx, stock in enumerate(stocks, 1):
        row = await _build_row(stock, as_of, args.write_raw)
        rows.append(row)
        if idx % 20 == 0:
            available = sum(1 for item in rows if item.get("ntm_per") is not None)
            print(f"[ntm-per] {idx}/{len(stocks)} 완료  ntm_per={available}")
        await asyncio.sleep(args.delay)

    log_df = _load_log()
    today_df = pd.DataFrame(rows, columns=LOG_COLS)
    if not log_df.empty:
        log_df = log_df[
            ~(
                (log_df["signal_date"].astype(str) == as_of.isoformat())
                & (log_df["ticker"].astype(str).isin(today_df["ticker"].astype(str)))
            )
        ]
    out = pd.concat([log_df, today_df], ignore_index=True)
    _save_log(out)
    _append_summary(as_of.isoformat(), len(stocks), rows)
    print(f"[ntm-per] 저장: {OBS_NTM_PER_CSV}")
    print(f"[ntm-per] 일별 요약: {OBS_NTM_PER_SUMMARY_CSV}")


if __name__ == "__main__":
    asyncio.run(main())
