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

from discovery.data_loader import CACHE_DIR, _cache_paths, _read_cached_frame, get_ohlcv_range  # noqa: E402


BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
REVIEW_CSV = BASE_DIR / "가설_이벤트_검토.csv"
REFRESH_SUMMARY_CSV = BASE_DIR / "가설_OHLCV_캐시_갱신_요약.csv"


def required_ranges(buffer_days: int) -> pd.DataFrame:
    review = pd.read_csv(REVIEW_CSV, encoding="utf-8-sig", dtype={"ticker": str})
    review["date"] = pd.to_datetime(review["date"])
    grouped = review.groupby(["ticker", "name"], as_index=False).agg(
        first_event=("date", "min"),
        last_event=("date", "max"),
        event_count=("event_id", "count"),
    )
    grouped["required_end"] = grouped["last_event"] + pd.Timedelta(days=buffer_days)
    return grouped.sort_values(["ticker"]).reset_index(drop=True)


def cached_range(ticker: str) -> tuple[pd.Timestamp | None, pd.Timestamp | None, int]:
    parquet_path, pickle_path = _cache_paths(ticker)
    df = _read_cached_frame(parquet_path, pickle_path)
    if df is None or df.empty:
        return None, None, 0
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    return df["date"].min(), df["date"].max(), len(df)


async def refresh_one(ticker: str, start: pd.Timestamp, end: pd.Timestamp) -> tuple[pd.Timestamp | None, pd.Timestamp | None, int]:
    df = await get_ohlcv_range(ticker, start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"), force_refresh=False)
    if df.empty:
        return None, None, 0
    return df["date"].min(), df["date"].max(), len(df)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--buffer-days", type=int, default=45)
    parser.add_argument("--delay", type=float, default=0.45)
    args = parser.parse_args()

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    ranges = required_ranges(args.buffer_days)
    rows = []

    for idx, row in ranges.iterrows():
        ticker = row["ticker"]
        name = row["name"]
        required_start = pd.Timestamp(row["first_event"]) - pd.Timedelta(days=5)
        required_end = pd.Timestamp(row["required_end"])
        before_min, before_max, before_rows = cached_range(ticker)

        if before_max is None:
            fetch_start = required_start
        else:
            fetch_start = min(required_start, before_max + pd.Timedelta(days=1)) if before_min and before_min > required_start else before_max + pd.Timedelta(days=1)

        if before_min is not None and before_min <= required_start and before_max is not None and before_max >= required_end:
            status = "skip_already_covered"
            fetched_min = fetched_max = None
            fetched_rows = 0
        elif fetch_start > required_end:
            status = "skip_no_gap"
            fetched_min = fetched_max = None
            fetched_rows = 0
        else:
            print(f"[refresh] {name}({ticker}) {fetch_start.date()} ~ {required_end.date()}")
            try:
                fetched_min, fetched_max, fetched_rows = await refresh_one(ticker, fetch_start, required_end)
                status = "ok" if fetched_rows else "no_data"
            except Exception as exc:
                fetched_min = fetched_max = None
                fetched_rows = 0
                status = f"error:{exc!r}"
            await asyncio.sleep(args.delay)

        after_min, after_max, after_rows = cached_range(ticker)
        rows.append(
            {
                "ticker": ticker,
                "name": name,
                "event_count": int(row["event_count"]),
                "required_start": required_start.strftime("%Y-%m-%d"),
                "required_end": required_end.strftime("%Y-%m-%d"),
                "before_min": "" if before_min is None else before_min.strftime("%Y-%m-%d"),
                "before_max": "" if before_max is None else before_max.strftime("%Y-%m-%d"),
                "before_rows": before_rows,
                "fetch_start": fetch_start.strftime("%Y-%m-%d"),
                "fetch_end": required_end.strftime("%Y-%m-%d"),
                "fetched_min": "" if fetched_min is None else pd.Timestamp(fetched_min).strftime("%Y-%m-%d"),
                "fetched_max": "" if fetched_max is None else pd.Timestamp(fetched_max).strftime("%Y-%m-%d"),
                "fetched_rows": fetched_rows,
                "after_min": "" if after_min is None else after_min.strftime("%Y-%m-%d"),
                "after_max": "" if after_max is None else after_max.strftime("%Y-%m-%d"),
                "after_rows": after_rows,
                "status": status,
            }
        )

    out = pd.DataFrame(rows)
    out.to_csv(REFRESH_SUMMARY_CSV, index=False, encoding="utf-8-sig")
    print(f"summary={REFRESH_SUMMARY_CSV}")
    print(out["status"].value_counts().to_string())


if __name__ == "__main__":
    asyncio.run(main())
