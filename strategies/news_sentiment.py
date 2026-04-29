import hashlib
import json
import logging
import re

import feedparser
import google.generativeai as genai
import httpx
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import select

from config import settings
from models.database import AsyncSessionLocal
from models.news_cache import NewsCache
from .base import BaseStrategy

logger = logging.getLogger(__name__)

# 뉴스 RSS 소스
NEWS_SOURCES = [
    "https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258",  # 네이버 증권 뉴스
    "https://www.hankyung.com/feed/finance",  # 한국경제
    "https://www.mk.co.kr/rss/30000001/",    # 매일경제
]


class NewsSentimentStrategy(BaseStrategy):
    """뉴스 크롤링 + Gemini AI 감성 분석 전략

    - 뉴스 기사를 크롤링하여 Gemini API로 분석
    - 호재/악재 판단 후 해당 종목 매수/매도
    """

    name = "news_sentiment"
    tickers: list[str] = []  # 동적으로 AI가 판단

    # AI 분석 결과 임시 저장 {ticker: signal}
    _pending_signals: dict[str, dict] = {}

    def __init__(self, min_confidence: float = 0.7, enabled: bool = True):
        self.min_confidence = min_confidence
        self.enabled = enabled
        genai.configure(api_key=settings.gemini_api_key)
        self.model = genai.GenerativeModel("gemini-2.5-flash")

    async def run_crawl_and_analyze(self) -> None:
        """뉴스 크롤링 + AI 분석 실행 (스케줄러에서 호출)"""
        articles = await self._fetch_articles()
        logger.info(f"[NewsSentiment] 새 기사 {len(articles)}건 수집")

        for article in articles:
            try:
                signals = await self._analyze(article)
                for signal in signals:
                    ticker = signal.get("ticker", "")
                    if ticker:
                        self._pending_signals[ticker] = signal
                        logger.info(
                            f"[NewsSentiment] {ticker} {signal['signal']} - {signal['reason']}"
                        )
            except Exception as e:
                logger.error(f"[NewsSentiment] 분석 실패: {e}")

    async def should_buy(self, ticker: str, df: pd.DataFrame) -> tuple[bool, str]:
        signal = self._pending_signals.get(ticker)
        if signal and signal.get("signal") == "buy":
            confidence = signal.get("confidence", 0)
            if confidence >= self.min_confidence:
                self._pending_signals.pop(ticker, None)
                return True, signal.get("reason", "AI 호재 감지")
        return False, ""

    async def should_sell(self, ticker: str, df: pd.DataFrame) -> tuple[bool, str]:
        signal = self._pending_signals.get(ticker)
        if signal and signal.get("signal") == "sell":
            confidence = signal.get("confidence", 0)
            if confidence >= self.min_confidence:
                self._pending_signals.pop(ticker, None)
                return True, signal.get("reason", "AI 악재 감지")
        return False, ""

    def get_pending_tickers(self) -> list[str]:
        """현재 매수/매도 신호가 있는 종목 목록"""
        return list(self._pending_signals.keys())

    async def _fetch_articles(self) -> list[dict]:
        """RSS 피드 및 네이버 증권에서 뉴스 수집 (중복 제거)"""
        articles = []

        # RSS 피드
        for url in NEWS_SOURCES[1:]:  # 한경, MK
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:10]:
                    article = {
                        "title": entry.get("title", ""),
                        "url": entry.get("link", ""),
                        "summary": entry.get("summary", ""),
                    }
                    if await self._is_new(article["url"]):
                        articles.append(article)
            except Exception as e:
                logger.warning(f"RSS 수집 실패 ({url}): {e}")

        # 네이버 증권 뉴스 크롤링
        try:
            naver_articles = await self._crawl_naver_finance()
            articles.extend(naver_articles)
        except Exception as e:
            logger.warning(f"네이버 증권 뉴스 수집 실패: {e}")

        return articles

    async def _crawl_naver_finance(self) -> list[dict]:
        """네이버 금융 뉴스 크롤링"""
        url = "https://finance.naver.com/news/news_list.naver?mode=LSS2D&section_id=101&section_id2=258"
        headers = {"User-Agent": "Mozilla/5.0"}

        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url, headers=headers)
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
        """DB에 없는 새 기사인지 확인 후 저장"""
        url_hash = hashlib.sha256(url.encode()).hexdigest()

        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(NewsCache).where(NewsCache.url_hash == url_hash)
            )
            existing = result.scalar_one_or_none()
            if existing:
                return False

            session.add(NewsCache(url_hash=url_hash, title="", url=url))
            await session.commit()

        return True

    async def _analyze(self, article: dict) -> list[dict]:
        """Gemini API로 기사 감성 분석"""
        prompt = f"""
다음 뉴스 기사 제목을 분석해서 특정 상장 기업에 호재인지 악재인지 판단해주세요.

기사 제목: {article['title']}
요약: {article.get('summary', '없음')}

답변 규칙:
1. 반드시 아래 JSON 형식으로만 답변하세요 (설명 없이 JSON만)
2. 명확하지 않으면 빈 배열 반환
3. ticker는 6자리 종목코드 (예: 005930)
4. signal: "buy"(호재) 또는 "sell"(악재)
5. confidence: 0.0~1.0 (판단 확신도)

형식:
[
  {{
    "name": "회사명",
    "ticker": "종목코드",
    "signal": "buy 또는 sell",
    "confidence": 0.8,
    "reason": "판단 근거 한 줄"
  }}
]
"""
        response = await self.model.generate_content_async(prompt)
        text = response.text.strip()

        # JSON 파싱
        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]

        try:
            result = json.loads(text.strip())
            if not isinstance(result, list):
                return []
            valid = [s for s in result if re.fullmatch(r"\d{6}", s.get("ticker", ""))]
            return valid
        except json.JSONDecodeError:
            logger.warning(f"Gemini 응답 파싱 실패: {text[:100]}")
            return []
