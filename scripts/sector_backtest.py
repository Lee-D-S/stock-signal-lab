#!/usr/bin/env python3
"""섹터 감성 분석 vs 실제 등락률 비교

실행: python scripts/sector_backtest.py

흐름:
  1. 최신 뉴스 크롤링 → Gemini 섹터 판단 → DB 저장
  2. KIS API로 오늘 섹터별 등락률 조회
  3. 감성 점수(-100~+100) vs 실제 등락률 비교 출력
"""
import asyncio
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

from sqlalchemy import select

from core.market_data import get_sector_changes
from models.database import AsyncSessionLocal, init_db
from models.sector_signal import SectorSignal
from strategies.news_sector import NewsSectorAnalyzer

# 분석 대상 기간 (오늘 포함 N일치 뉴스)
LOOKBACK_DAYS = 2


def _score(sentiment: str, confidence: float) -> float:
    """positive/negative + confidence → -100~+100"""
    return confidence * 100 if sentiment == "positive" else -confidence * 100


_SECTOR_ALIAS = {
    "서비스업": "일반서비스",
    "의약품": "제약",
    "운수장비": "운송장비",
    "섬유의복": "섬유",
    "음식료품": "음식료",
    "철강금속": "금속",
}


def _normalize(name: str) -> str:
    return name.replace("/", "").replace(" ", "").replace("·", "").replace("·", "")


def _match(sector: str, actual: dict[str, float]) -> float | None:
    """섹터명 퍼지 매칭 → 실제 등락률"""
    sector = _SECTOR_ALIAS.get(sector, sector)
    a = _normalize(sector)
    for name, rate in actual.items():
        b = _normalize(name)
        if a in b or b in a:
            return rate
    return None


async def step1_crawl(analyzer: NewsSectorAnalyzer) -> list:
    print(f"\n[1/3] 뉴스 크롤링 + Gemini 섹터 분석 중... (최근 {LOOKBACK_DAYS}일)")
    await analyzer.run_crawl_and_analyze()

    since = datetime.combine(date.today() - timedelta(days=LOOKBACK_DAYS - 1), datetime.min.time())
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(SectorSignal).where(SectorSignal.predicted_at >= since)
        )
        signals = result.scalars().all()

    print(f"  → 오늘 저장된 신호: {len(signals)}건")

    if signals:
        from collections import Counter
        counts = Counter(s.sector_name for s in signals)
        for sector, cnt in counts.most_common():
            subset = [s for s in signals if s.sector_name == sector]
            avg_score = sum(_score(s.sentiment, s.confidence) for s in subset) / len(subset)
            print(f"     {sector:<12} {avg_score:>+6.1f}점  ({cnt}건)")

    return signals


async def step2_sector_changes() -> dict[str, float]:
    print("\n[2/3] KIS 섹터 등락률 조회 중...")
    try:
        kospi = await get_sector_changes("K")
        kosdaq = await get_sector_changes("Q")
    except Exception as e:
        print(f"  ✗ 조회 실패: {e}")
        return {}

    _NOISE_PREFIXES = ("KRX", "WISE", "KSQ", "KTOP", "K200", "KEBI", "KRX",
                       "Nikkei", "DJCI", "DJSI", "HSCEI", "NASDAQ", "S&P",
                       "FnGuide", "Solactive", "코스피 TR", "코스피200",
                       "고배당", "배당성장", "대형주", "중형주", "소형주", "우선주",
                       "종합", "제조", "코스피 200", "미니 F", "주식골드")

    actual: dict[str, float] = {}
    for row in kospi:  # 코스피 섹터만 사용 (코스닥은 지수 위주)
        name = row["name"]
        if name and not any(name.startswith(p) for p in _NOISE_PREFIXES):
            actual[name] = row["change_rate"]

    print(f"  → 코스피 업종 {len(actual)}개 (지수·테마 제외)")
    return actual


def step3_analyze(signals: list, actual: dict[str, float]) -> None:
    print("\n[3/3] 비교 분석")
    print("=" * 65)

    # 섹터별 점수 집계
    sector_scores: dict[str, list[float]] = {}
    for sig in signals:
        sector_scores.setdefault(sig.sector_name, []).append(
            _score(sig.sentiment, sig.confidence)
        )

    avg_scores = {k: sum(v) / len(v) for k, v in sector_scores.items()}

    print(f"{'섹터':<12} {'감성점수':>8} {'기사수':>5} {'실제등락률':>10} {'방향일치'}")
    print("-" * 55)

    matched, hit = 0, 0
    for sector, score in sorted(avg_scores.items(), key=lambda x: -abs(x[1])):
        count = len(sector_scores[sector])
        rate = _match(sector, actual)

        if rate is not None:
            matched += 1
            ok = (score > 0 and rate > 0) or (score < 0 and rate < 0)
            if ok:
                hit += 1
            mark = "✅" if ok else "❌"
            rate_str = f"{rate:+.2f}%"
        else:
            mark = "？ (미매칭)"
            rate_str = "  N/A"

        print(f"{sector:<12} {score:>+8.1f} {count:>5} {rate_str:>10}  {mark}")

    print("=" * 65)

    if matched:
        print(f"\n매칭 섹터: {matched}개 | 방향 일치: {hit}개 | 적중률: {hit/matched*100:.1f}%")
    else:
        print("\n매칭된 섹터 없음")
        # 디버그: KIS 실제 업종명 출력
        print(f"\n[디버그] KIS 반환 업종명 목록:")
        for name in sorted(actual.keys()):
            print(f"  - {name}")


class BacktestAnalyzer(NewsSectorAnalyzer):
    """무료 티어용 백테스트 분석기 — 1배치(10건)만 호출"""

    async def _fetch_articles(self) -> list[dict]:
        articles = await super()._fetch_articles()
        # 무료 티어 5 RPM 대응: 최대 10건(1배치)만 분석
        limited = articles[:10]
        print(f"  (수집된 새 기사 {len(articles)}건 중 {len(limited)}건만 분석 — 무료 티어 제한)")
        return limited


async def main() -> None:
    # --compare-only: 크롤링 없이 기존 DB 신호로 비교만 실행
    compare_only = "--compare-only" in sys.argv

    print("=" * 65)
    print("  섹터 감성 분석 vs 실제 등락률 비교")
    print(f"  기준일: {date.today()} (최근 {LOOKBACK_DAYS}일 뉴스)")
    if compare_only:
        print("  모드: 비교만 (크롤링 생략)")
    print("=" * 65)

    await init_db()

    if compare_only:
        since = datetime.combine(date.today() - timedelta(days=LOOKBACK_DAYS - 1), datetime.min.time())
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(SectorSignal).where(SectorSignal.predicted_at >= since)
            )
            signals = result.scalars().all()
        print(f"\n[1/3 skip] DB에서 신호 {len(signals)}건 로드")
    else:
        analyzer = BacktestAnalyzer(min_confidence=0.5)
        signals = await step1_crawl(analyzer)

    if not signals:
        print("\n수집된 신호 없음. Gemini API 키 및 네트워크 확인 필요.")
        return

    actual = await step2_sector_changes()
    if not actual:
        print("\n섹터 등락률 조회 실패. 장 마감 후(15:30 이후) 실행 권장.")
        return

    step3_analyze(signals, actual)


if __name__ == "__main__":
    asyncio.run(main())
