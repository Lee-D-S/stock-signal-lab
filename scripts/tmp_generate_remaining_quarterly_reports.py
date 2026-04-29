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
    ("000660", "SK하이닉스"),
    ("047040", "대우건설"),
    ("006400", "삼성SDI"),
    ("005490", "POSCO홀딩스"),
    ("001440", "대한전선"),
    ("001510", "SK증권"),
    ("005935", "삼성전자우"),
    ("042700", "한미반도체"),
    ("009150", "삼성전기"),
    ("222080", "씨아이에스"),
    ("402340", "SK스퀘어"),
    ("267260", "HD현대일렉트릭"),
    ("298380", "에이비엘바이오"),
    ("322000", "HD현대에너지솔루션"),
    ("000720", "현대건설"),
    ("010170", "대한광통신"),
    ("012450", "한화에어로스페이스"),
    ("028050", "삼성E&A"),
    ("298040", "효성중공업"),
    ("086520", "에코프로"),
    ("329180", "HD현대중공업"),
    ("006360", "GS건설"),
    ("241520", "DSC인베스트먼트"),
    ("490470", "세미파이브"),
]


def has_all_reports(name: str) -> bool:
    company_dir = OUT_DIR / name
    return all((company_dir / f"{name}_{code}_원인후보_실제분석.md").is_file() for code, *_ in PERIODS)


async def main() -> None:
    corp_map = await get_corp_code_map()
    completed: list[str] = []
    skipped: list[str] = []
    failed: list[tuple[str, str]] = []

    for ticker, name in COMPANIES:
        if has_all_reports(name):
            print(f"[skip] {name}: existing 5 reports")
            skipped.append(name)
            continue

        try:
            corp_code = corp_map[ticker]
            print(f"[start] {name}({ticker}) corp_code={corp_code}")
            financials, snapshot = await asyncio.gather(fetch_financials(corp_code), fetch_price_snapshot(ticker))
            for code, title, start, end in PERIODS:
                await build_period(ticker, name, code, title, start, end, corp_code, financials, snapshot)
                await asyncio.sleep(0.5)
            completed.append(name)
            print(f"[done] {name}")
        except Exception as exc:
            failed.append((name, repr(exc)))
            print(f"[fail] {name}: {exc!r}", file=sys.stderr)
        await asyncio.sleep(1.0)

    print("[summary]")
    print(f"completed={len(completed)}: {', '.join(completed)}")
    print(f"skipped={len(skipped)}: {', '.join(skipped)}")
    print(f"failed={len(failed)}: {failed}")


if __name__ == "__main__":
    asyncio.run(main())
