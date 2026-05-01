from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from tmp_quarterly_stock_analysis import (  # noqa: E402
    OUT_DIR,
    PERIODS,
    build_period,
    fetch_financials,
    fetch_price_snapshot,
    fetch_stock_info,
    get_corp_code_map,
    listing_date_from_stock_info,
)


BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
STRATEGY_DIR = BASE_DIR / "07_전략신호"
PLAN_DIR = BASE_DIR / "01_기획"
UNIVERSE_CSV = STRATEGY_DIR / "거래대금_상위_유니버스.csv"
SUMMARY_CSV = PLAN_DIR / "신규_기업_보고서_생성_상태.csv"


def report_path(name: str, period_code: str) -> Path:
    return OUT_DIR / name / f"{name}_{period_code}_원인후보_실제분석.md"


def events_path(name: str, ticker: str) -> Path:
    return OUT_DIR / name / f"{name}_{ticker}_events.jsonl"


def is_pre_listing_period(end: str, listing_date: pd.Timestamp | None) -> bool:
    return listing_date is not None and pd.Timestamp(end) < listing_date


def missing_periods(name: str, listing_date: pd.Timestamp | None = None) -> list[tuple[str, str, str, str]]:
    return [
        (code, title, start, end)
        for code, title, start, end in PERIODS
        if not is_pre_listing_period(end, listing_date)
        if not report_path(name, code).is_file()
    ]


def load_targets(include_existing_missing: bool) -> pd.DataFrame:
    universe = pd.read_csv(UNIVERSE_CSV, encoding="utf-8-sig", dtype={"ticker": str})
    if include_existing_missing:
        return universe[universe["report_status"] == "report_needed"].copy()
    return universe[universe["universe_status"] == "new"].copy()


async def generate_one(ticker: str, name: str, corp_map: dict[str, str], delay: float) -> dict[str, Any]:
    company_dir = OUT_DIR / name
    company_dir.mkdir(parents=True, exist_ok=True)
    stock_info = await fetch_stock_info(ticker)
    listing_date = listing_date_from_stock_info(stock_info)
    skipped_pre_listing = sum(1 for _, _, _, end in PERIODS if is_pre_listing_period(end, listing_date))
    missing = missing_periods(name, listing_date)
    if not missing:
        return {
            "ticker": ticker,
            "name": name,
            "status": "skip_existing",
            "created_reports": 0,
            "missing_reports_after": 0,
            "listing_date": "" if listing_date is None else listing_date.strftime("%Y-%m-%d"),
            "skipped_pre_listing": skipped_pre_listing,
            "events_path": str(events_path(name, ticker)),
            "error": "",
        }

    try:
        corp_code = corp_map[ticker]
        financials, snapshot = await asyncio.gather(fetch_financials(corp_code), fetch_price_snapshot(ticker))
        created = 0
        for code, title, start, end in missing:
            path = await build_period(ticker, name, code, title, start, end, corp_code, financials, snapshot, listing_date)
            if path is not None:
                created += 1
            await asyncio.sleep(delay)
        missing_after = len(missing_periods(name, listing_date))
        return {
            "ticker": ticker,
            "name": name,
            "status": "ok" if missing_after == 0 else "partial",
            "created_reports": created,
            "missing_reports_after": missing_after,
            "listing_date": "" if listing_date is None else listing_date.strftime("%Y-%m-%d"),
            "skipped_pre_listing": skipped_pre_listing,
            "events_path": str(events_path(name, ticker)),
            "error": "",
        }
    except Exception as exc:
        return {
            "ticker": ticker,
            "name": name,
            "status": "error",
            "created_reports": 0,
            "missing_reports_after": len(missing_periods(name, listing_date if "listing_date" in locals() else None)),
            "listing_date": "" if "listing_date" not in locals() or listing_date is None else listing_date.strftime("%Y-%m-%d"),
            "skipped_pre_listing": skipped_pre_listing if "skipped_pre_listing" in locals() else 0,
            "events_path": str(events_path(name, ticker)),
            "error": repr(exc),
        }


async def main() -> None:
    parser = argparse.ArgumentParser(description="거래대금 상위 유니버스의 신규/보고서 필요 기업 보고서 생성")
    parser.add_argument("--limit", type=int, help="처리할 최대 기업 수")
    parser.add_argument("--delay", type=float, default=0.8)
    parser.add_argument(
        "--include-existing-missing",
        action="store_true",
        help="이번 실행에서 new가 아니어도 report_needed 상태인 기업까지 처리",
    )
    args = parser.parse_args()

    targets = load_targets(include_existing_missing=args.include_existing_missing)
    if args.limit:
        targets = targets.head(args.limit)

    corp_map = await get_corp_code_map()
    rows: list[dict[str, Any]] = []
    for _, row in targets.iterrows():
        ticker = str(row["ticker"]).zfill(6)
        name = str(row["name"]).strip()
        print(f"[start] {name}({ticker})")
        result = await generate_one(ticker, name, corp_map, args.delay)
        rows.append(result)
        print(
            f"[{result['status']}] {name} created={result['created_reports']} "
            f"missing_after={result['missing_reports_after']} listing_date={result.get('listing_date') or 'N/A'} "
            f"pre_listing_skipped={result.get('skipped_pre_listing', 0)}"
        )
        if result["error"]:
            print(f"  error={result['error']}")
        await asyncio.sleep(args.delay)

    out = pd.DataFrame(rows)
    out.to_csv(SUMMARY_CSV, index=False, encoding="utf-8-sig")
    print(f"summary_csv={SUMMARY_CSV}")
    if not out.empty:
        print(out["status"].value_counts().to_string())
    else:
        print("targets=0")


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
