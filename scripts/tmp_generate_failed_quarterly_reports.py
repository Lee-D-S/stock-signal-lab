from __future__ import annotations

import asyncio

from tmp_quarterly_stock_analysis import (
    OUT_DIR,
    PERIODS,
    build_period,
    fetch_financials,
    fetch_price_snapshot,
    get_corp_code_map,
)

COMPANIES = [
    ("005935", "삼성전자우", "005930"),
    ("012450", "한화에어로스페이스", None),
    ("490470", "세미파이브", None),
]


async def main() -> None:
    corp_map = await get_corp_code_map()
    failed: list[tuple[str, str]] = []

    for ticker, name, corp_ticker in COMPANIES:
        try:
            company_dir = OUT_DIR / name
            company_dir.mkdir(parents=True, exist_ok=True)
            lookup_ticker = corp_ticker or ticker
            corp_code = corp_map[lookup_ticker]
            print(f"[start] {name}({ticker}) corp_code={corp_code}")
            financials, snapshot = await asyncio.gather(fetch_financials(corp_code), fetch_price_snapshot(ticker))
            for code, title, start, end in PERIODS:
                target = company_dir / f"{name}_{code}_원인후보_실제분석.md"
                if target.is_file():
                    print(f"[skip] {target.name}")
                    continue
                await build_period(ticker, name, code, title, start, end, corp_code, financials, snapshot)
                await asyncio.sleep(0.8)
            print(f"[done] {name}")
        except Exception as exc:
            failed.append((name, repr(exc)))
            print(f"[fail] {name}: {exc!r}", file=sys.stderr)
        await asyncio.sleep(1.5)

    print(f"[summary] failed={failed}")


if __name__ == "__main__":
    asyncio.run(main())
