from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from discovery.data_loader import (  # noqa: E402
    _fetch_chunk,
    _read_cached_frame,
    _write_cached_frame,
    _cache_paths,
)


BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
GAP_CSV = BASE_DIR / "가설_백테스트_갭_분류.csv"
FILL_SUMMARY_CSV = BASE_DIR / "가설_OHLCV_내부_갭_보정_요약.csv"


async def fetch_range(ticker: str, start: pd.Timestamp, end: pd.Timestamp, chunk_days: int, delay: float) -> pd.DataFrame:
    chunks = []
    cur = start
    while cur <= end:
        chunk_end = min(cur + pd.Timedelta(days=chunk_days), end)
        chunk = await _fetch_chunk(ticker, cur.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d"))
        if not chunk.empty:
            chunks.append(chunk)
        cur = chunk_end + pd.Timedelta(days=1)
        if cur <= end:
            await asyncio.sleep(delay)
    if not chunks:
        return pd.DataFrame()
    return pd.concat(chunks).drop_duplicates("date").sort_values("date").reset_index(drop=True)


def read_existing(ticker: str) -> pd.DataFrame:
    parquet_path, pickle_path = _cache_paths(ticker)
    existing = _read_cached_frame(parquet_path, pickle_path)
    if existing is None:
        return pd.DataFrame()
    existing = existing.copy()
    existing["date"] = pd.to_datetime(existing["date"])
    return existing.sort_values("date").reset_index(drop=True)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--before-days", type=int, default=10)
    parser.add_argument("--after-days", type=int, default=45)
    parser.add_argument("--chunk-days", type=int, default=120)
    parser.add_argument("--delay", type=float, default=0.45)
    args = parser.parse_args()

    gaps = pd.read_csv(GAP_CSV, encoding="utf-8-sig", dtype={"ticker": str})
    gaps = gaps[gaps["gap_type"] == "missing_inside_cache_range"].copy()
    gaps["date"] = pd.to_datetime(gaps["date"])
    ranges = gaps.groupby(["ticker", "name"], as_index=False).agg(
        first_missing=("date", "min"),
        last_missing=("date", "max"),
        missing_count=("date", "count"),
    )
    rows = []
    for _, row in ranges.sort_values("ticker").iterrows():
        ticker = row["ticker"]
        name = row["name"]
        fetch_start = pd.Timestamp(row["first_missing"]) - pd.Timedelta(days=args.before_days)
        fetch_end = pd.Timestamp(row["last_missing"]) + pd.Timedelta(days=args.after_days)
        existing = read_existing(ticker)
        before_rows = len(existing)
        before_has = int(existing["date"].isin(gaps[gaps["ticker"] == ticker]["date"]).sum()) if not existing.empty else 0

        print(f"[fill] {name}({ticker}) {fetch_start.date()} ~ {fetch_end.date()}")
        fetched = await fetch_range(ticker, fetch_start, fetch_end, args.chunk_days, args.delay)
        if fetched.empty:
            status = "no_data"
            merged = existing
        else:
            merged = (
                pd.concat([existing, fetched])
                .drop_duplicates("date")
                .sort_values("date")
                .reset_index(drop=True)
            )
            parquet_path, pickle_path = _cache_paths(ticker)
            _write_cached_frame(merged, parquet_path, pickle_path)
            status = "ok"

        ticker_gaps = gaps[gaps["ticker"] == ticker]["date"]
        after_has = int(merged["date"].isin(ticker_gaps).sum()) if not merged.empty else 0
        rows.append(
            {
                "ticker": ticker,
                "name": name,
                "missing_count": int(row["missing_count"]),
                "fetch_start": fetch_start.strftime("%Y-%m-%d"),
                "fetch_end": fetch_end.strftime("%Y-%m-%d"),
                "before_rows": before_rows,
                "fetched_rows": len(fetched),
                "after_rows": len(merged),
                "missing_dates_present_before": before_has,
                "missing_dates_present_after": after_has,
                "status": status,
            }
        )
        await asyncio.sleep(args.delay)

    out = pd.DataFrame(rows)
    out.to_csv(FILL_SUMMARY_CSV, index=False, encoding="utf-8-sig")
    print(f"summary={FILL_SUMMARY_CSV}")
    print(out["status"].value_counts().to_string())
    print(f"missing_dates_present_after={out['missing_dates_present_after'].sum()} / {out['missing_count'].sum()}")


if __name__ == "__main__":
    asyncio.run(main())
