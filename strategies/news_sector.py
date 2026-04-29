import asyncio
import hashlib
import json
import logging

import feedparser
import google.generativeai as genai
import httpx
from bs4 import BeautifulSoup
from sqlalchemy import select

from config import settings
from models.database import AsyncSessionLocal
from models.sector_signal import SectorSignal

logger = logging.getLogger(__name__)

NEWS_SOURCES = [
    "https://www.hankyung.com/feed/finance",
    "https://www.mk.co.kr/rss/30000001/",
]

# Gemini에게 강제로 선택하게 할 업종 목록 (KIS 업종명 기준)
SECTOR_LIST = [
    "음식료품", "섬유의복", "화학", "의약품", "철강금속",
    "기계", "전기전자", "운수장비", "건설업", "통신업",
    "금융업", "은행", "증권", "보험", "서비스업",
    "유통업", "전기가스업", "비금속광물", "종이목재",
]


class NewsSectorAnalyzer:
    """뉴스 기사 → 업종 감성 분석 + 장 마감 후 정확도 검증"""

    def __init__(self, min_confidence: float = 0.7):
        self.min_confidence = min_confidence
        genai.configure(api_key=settings.gemini_api_key)
        self.model = genai.GenerativeModel("gemini-2.5-flash-lite")

    BATCH_SIZE = 10   # 한 번에 Gemini에 보낼 기사 수
    BATCH_DELAY = 13  # 배치 간 딜레이(초) — 무료 티어 5 RPM 대응

    async def run_crawl_and_analyze(self) -> None:
        articles = await self._fetch_articles()
        logger.info(f"[SectorAnalyzer] 새 기사 {len(articles)}건 수집")

        for i in range(0, len(articles), self.BATCH_SIZE):
            batch = articles[i:i + self.BATCH_SIZE]
            results = await self._analyze_batch_with_retry(batch)
            for article, result in zip(batch, results):
                if not result:
                    continue
                if result["confidence"] < self.min_confidence:
                    continue
                url_hash = hashlib.sha256(article["url"].encode()).hexdigest()
                async with AsyncSessionLocal() as session:
                    session.add(SectorSignal(
                        url_hash=url_hash,
                        title=article["title"],
                        sector_name=result["sector"],
                        sentiment=result["sentiment"],
                        confidence=result["confidence"],
                        reason=result.get("reason", ""),
                    ))
                    await session.commit()
                logger.info(f"[SectorAnalyzer] {result['sector']} {result['sentiment']} ({result['confidence']:.2f}) — {article['title'][:40]}")

            if i + self.BATCH_SIZE < len(articles):
                await asyncio.sleep(self.BATCH_DELAY)

    async def _analyze_batch_with_retry(self, articles: list[dict], max_retries: int = 3) -> list[dict | None]:
        """429 에러 시 retry_delay만큼 대기 후 재시도"""
        import re
        for attempt in range(max_retries):
            try:
                return await self._analyze_batch(articles)
            except Exception as e:
                msg = str(e)
                if "429" in msg:
                    # retry_delay 파싱
                    m = re.search(r"seconds:\s*(\d+)", msg)
                    wait = int(m.group(1)) + 5 if m else 60
                    logger.warning(f"[SectorAnalyzer] 속도 제한 — {wait}초 대기 후 재시도 ({attempt+1}/{max_retries})")
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"[SectorAnalyzer] 배치 분석 실패: {e}")
                    return [None] * len(articles)
        logger.error("[SectorAnalyzer] 최대 재시도 초과")
        return [None] * len(articles)

    def match_sector(self, gemini_name: str, available: list[str]) -> str | None:
        """Gemini 출력 업종명을 실제 KIS 업종명에 퍼지 매칭"""
        gemini_clean = gemini_name.replace("/", "").replace(" ", "")
        for name in available:
            clean = name.replace("/", "").replace(" ", "")
            if gemini_clean in clean or clean in gemini_clean:
                return name
        return None

    async def _fetch_articles(self) -> list[dict]:
        articles = []

        for url in NEWS_SOURCES:
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:10]:
                    article = {
                        "title": entry.get("title", ""),
                        "url": entry.get("link", ""),
                        "summary": entry.get("summary", ""),
                    }
                    if article["title"] and article["url"] and await self._is_new(article["url"]):
                        articles.append(article)
            except Exception as e:
                logger.warning(f"RSS 수집 실패 ({url}): {e}")

        try:
            naver = await self._crawl_naver_finance()
            articles.extend(naver)
        except Exception as e:
            logger.warning(f"네이버 증권 수집 실패: {e}")

        return articles

    async def _crawl_naver_finance(self) -> list[dict]:
        url = "https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258"
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        articles = []
        for item in soup.select(".newslist li")[:15]:
            a_tag = item.select_one("a")
            if not a_tag:
                continue
            title = a_tag.get_text(strip=True)
            href = a_tag.get("href", "")
            full_url = f"https://finance.naver.com{href}" if href.startswith("/") else href
            if title and full_url and await self._is_new(full_url):
                articles.append({"title": title, "url": full_url, "summary": ""})
        return articles

    async def _is_new(self, url: str) -> bool:
        url_hash = hashlib.sha256(url.encode()).hexdigest()
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(SectorSignal).where(SectorSignal.url_hash == url_hash)
            )
            return result.scalar_one_or_none() is None

    async def _analyze_batch(self, articles: list[dict]) -> list[dict | None]:
        """여러 기사를 한 번의 Gemini 호출로 분석"""
        sector_list_str = ", ".join(SECTOR_LIST)

        articles_text = "\n".join(
            f"{i+1}. 제목: {a['title']}\n   요약: {a.get('summary', '없음') or '없음'}"
            for i, a in enumerate(articles)
        )

        prompt = f"""다음 뉴스 기사들을 각각 분석해서 관련 업종과 감성을 판단해주세요.

{articles_text}

업종 목록 (반드시 아래 목록 중 하나만 선택):
{sector_list_str}

답변 규칙:
1. 기사 번호 순서대로 JSON 배열로만 답변 (설명 없이)
2. 관련 업종이 없거나 판단 불가능하면 해당 항목을 null로
3. sentiment: "positive"(호재) 또는 "negative"(악재)
4. confidence: 0.0~1.0

[
  {{"sector": "업종명", "sentiment": "positive", "confidence": 0.8, "reason": "근거"}},
  null,
  ...
]"""

        response = await self.model.generate_content_async(prompt)
        text = response.text.strip()

        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        text = text.strip()

        try:
            results = json.loads(text)
            if not isinstance(results, list):
                return [None] * len(articles)

            output = []
            for r in results[:len(articles)]:
                if r is None or not isinstance(r, dict):
                    output.append(None)
                elif r.get("sector") not in SECTOR_LIST:
                    output.append(None)
                else:
                    output.append(r)

            # 반환 개수가 부족하면 None으로 채움
            while len(output) < len(articles):
                output.append(None)
            return output

        except json.JSONDecodeError:
            logger.warning(f"[SectorAnalyzer] 배치 파싱 실패: {text[:120]}")
            return [None] * len(articles)
