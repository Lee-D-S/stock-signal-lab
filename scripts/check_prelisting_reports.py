from __future__ import annotations

import argparse
import asyncio
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from tmp_generate_watchlist_signals import COMPANIES  # noqa: E402
from tmp_quarterly_stock_analysis import fetch_stock_info, listing_date_from_stock_info  # noqa: E402


BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
COMPANY_DIR = BASE_DIR / "00_기업별분석"
PLAN_DIR = BASE_DIR / "01_기획"
UNIVERSE_CSV = BASE_DIR / "07_전략신호" / "거래대금_상위_유니버스.csv"
OUT_CSV = PLAN_DIR / "상장전_보고서_점검.csv"

PERIOD_RE = re.compile(r"_(\d{4})_Q([1-4])_원인후보_실제분석\.md$")


def quarter_end(year: int, quarter: int) -> pd.Timestamp:
    month = quarter * 3
    return pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)


def infer_ticker_map() -> dict[str, str]:
    mapping: dict[str, str] = {}
    for ticker, name, _corp_ticker in COMPANIES:
        mapping[str(name).strip()] = str(ticker).zfill(6)

    if UNIVERSE_CSV.exists():
        universe = pd.read_csv(UNIVERSE_CSV, encoding="utf-8-sig", dtype={"ticker": str})
        for _, row in universe.iterrows():
            mapping[str(row["name"]).strip()] = str(row["ticker"]).zfill(6)

    for events_path in COMPANY_DIR.rglob("*_events.jsonl"):
        parts = events_path.stem.split("_")
        if len(parts) >= 3 and parts[-2].isdigit() and parts[-1] == "events":
            ticker = parts[-2].zfill(6)
            if len(ticker) == 6:
                mapping.setdefault(events_path.parent.name, ticker)
    return mapping


async def listing_dates_for(tickers: set[str], delay: float) -> dict[str, pd.Timestamp | None]:
    out: dict[str, pd.Timestamp | None] = {}
    for ticker in sorted(tickers):
        try:
            stock_info = await fetch_stock_info(ticker)
            out[ticker] = listing_date_from_stock_info(stock_info)
        except Exception:
            out[ticker] = None
        await asyncio.sleep(delay)
    return out


async def main() -> None:
    parser = argparse.ArgumentParser(description="상장일 기준 상장 전 보고서 점검")
    parser.add_argument("--delay", type=float, default=0.25)
    args = parser.parse_args()

    name_to_ticker = infer_ticker_map()
    tickers = {ticker for ticker in name_to_ticker.values() if ticker}
    listing_dates = await listing_dates_for(tickers, args.delay)

    rows: list[dict[str, Any]] = []
    for report_path in COMPANY_DIR.rglob("*_원인후보_실제분석.md"):
        match = PERIOD_RE.search(report_path.name)
        if not match:
            continue
        year = int(match.group(1))
        quarter = int(match.group(2))
        end_date = quarter_end(year, quarter)
        company = report_path.parent.name
        ticker = name_to_ticker.get(company, "")
        listing_date = listing_dates.get(ticker)
        if listing_date is None:
            status = "listing_unverified"
        elif end_date < listing_date:
            status = "pre_listing"
        else:
            status = "listed_period"
        if status != "listed_period":
            events_path = report_path.with_name(report_path.name.replace("_원인후보_실제분석.md", "_events.jsonl"))
            rows.append(
                {
                    "company": company,
                    "ticker": ticker,
                    "period": f"{year}_Q{quarter}",
                    "period_end": end_date.strftime("%Y-%m-%d"),
                    "listing_date": "" if listing_date is None else listing_date.strftime("%Y-%m-%d"),
                    "status": status,
                    "report_file": str(report_path),
                    "events_file": str(events_path),
                    "events_exists": events_path.is_file(),
                    "events_size": events_path.stat().st_size if events_path.is_file() else "",
                }
            )

    out = pd.DataFrame(rows).sort_values(["status", "company", "period"]) if rows else pd.DataFrame()
    out.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"out_csv={OUT_CSV}")
    if out.empty:
        print("pre_listing_or_unverified=0")
    else:
        print(out["status"].value_counts().to_string())
        pre = out[out["status"] == "pre_listing"]
        if not pre.empty:
            print(pre.groupby("company")["period"].apply(lambda s: ", ".join(sorted(s))).to_string())


def run_async_entrypoint() -> None:
    if sys.platform != "win32":
        asyncio.run(main())
        return
    policies = [
        asyncio.WindowsProactorEventLoopPolicy,
        asyncio.WindowsSelectorEventLoopPolicy,
    ]
    last_error: BaseException | None = None
    for policy_factory in policies:
        try:
            asyncio.set_event_loop_policy(policy_factory())
            loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(loop)
                loop.run_until_complete(main())
                return
            finally:
                loop.close()
        except BaseException as exc:
            last_error = exc
            continue
    if last_error:
        raise last_error


if __name__ == "__main__":
    run_async_entrypoint()
