from __future__ import annotations

import asyncio
import sys

from tmp_quarterly_stock_analysis import (
    OUT_DIR,
    PERIODS,
    build_period,
    fetch_financials,
    fetch_price_snapshot,
    get_corp_code_map,
)

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

COMPANIES = [
    ("005930", "삼성전자", None),
    ("000660", "SK하이닉스", None),
    ("047040", "대우건설", None),
    ("006400", "삼성SDI", None),
    ("005490", "POSCO홀딩스", None),
    ("001440", "대한전선", None),
    ("001510", "SK증권", None),
    ("005935", "삼성전자우", "005930"),
    ("042700", "한미반도체", None),
    ("009150", "삼성전기", None),
    ("222080", "씨아이에스", None),
    ("066570", "LG전자", None),
    ("402340", "SK스퀘어", None),
    ("267260", "HD현대일렉트릭", None),
    ("034020", "두산에너빌리티", None),
    ("298380", "에이비엘바이오", None),
    ("322000", "HD현대에너지솔루션", None),
    ("000720", "현대건설", None),
    ("010170", "대한광통신", None),
    ("012450", "한화에어로스페이스", None),
    ("028050", "삼성E&A", None),
    ("298040", "효성중공업", None),
    ("086520", "에코프로", None),
    ("329180", "HD현대중공업", None),
    ("006360", "GS건설", None),
    ("241520", "DSC인베스트먼트", None),
    ("490470", "세미파이브", None),
]


def has_new_format(name: str, code: str) -> bool:
    path = OUT_DIR / name / f"{name}_{code}_원인후보_실제분석.md"
    events_path = OUT_DIR / name / f"{name}_{code}_events.jsonl"
    if not path.is_file():
        return False
    if not events_path.is_file():
        return False
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
        events_text = events_path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return False
    return (
        "## 이벤트별 상승 원인 후보 판정" in text
        and "## 이벤트별 하락 원인 후보 판정" in text
        and "멀티 윈도우 요약" in text
        and "원인 창 분류" in text
        and ("event_id" in events_text or len(events_text) == 0)
        and "Gemini 호출 실패" not in text
        and "google-genai 미설치" not in text
        and "GEMINI_API_KEY 미설정" not in text
        and "외부 요인 후보 (Gemini 검색)" not in text
    )


async def main() -> None:
    corp_map = await get_corp_code_map()
    failed: list[tuple[str, str]] = []

    for ticker, name, corp_ticker in COMPANIES:
        try:
            missing_periods = [(code, title, start, end) for code, title, start, end in PERIODS if not has_new_format(name, code)]
            if not missing_periods:
                print(f"[skip] {name}: new format complete")
                continue
            lookup_ticker = corp_ticker or ticker
            corp_code = corp_map[lookup_ticker]
            print(f"[start] {name}({ticker}) corp_code={corp_code}")
            financials, snapshot = await asyncio.gather(fetch_financials(corp_code), fetch_price_snapshot(ticker))
            for code, title, start, end in missing_periods:
                await build_period(ticker, name, code, title, start, end, corp_code, financials, snapshot)
                await asyncio.sleep(0.5)
            print(f"[done] {name}")
        except Exception as exc:
            failed.append((name, repr(exc)))
            print(f"[fail] {name}: {exc!r}", file=sys.stderr)
        await asyncio.sleep(1.0)

    print(f"[summary] failed={failed}")


if __name__ == "__main__":
    asyncio.run(main())
