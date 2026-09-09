#!/usr/bin/env python3
"""Gemini API 동작 확인용 — 뉴스 기사 1건 분석

실행: python scripts/test_gemini.py
"""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import google.generativeai as genai

from config import settings
from strategies.news_sector import SECTOR_LIST


async def main() -> None:
    if not settings.gemini_api_key:
        print("❌ GEMINI_API_KEY가 .env에 없음")
        return

    genai.configure(api_key=settings.gemini_api_key)
    model = genai.GenerativeModel("gemini-2.5-flash-lite")

    article = {
        "title": "삼성전자, HBM3E 납품 승인… 엔비디아에 4분기부터 공급",
        "summary": "삼성전자가 엔비디아의 HBM3E 품질 테스트를 통과해 4분기부터 납품을 시작한다고 밝혔다.",
    }

    sector_list_str = ", ".join(SECTOR_LIST)
    prompt = f"""다음 뉴스 기사를 분석해서 관련 업종과 감성을 판단해주세요.

제목: {article['title']}
요약: {article['summary']}

업종 목록 (반드시 아래 목록 중 하나만 선택):
{sector_list_str}

JSON만 답변:
{{"sector": "업종명", "sentiment": "positive 또는 negative", "confidence": 0.0~1.0, "reason": "근거"}}"""

    print(f"모델  : gemini-2.5-flash-lite")
    print(f"기사  : {article['title']}")
    print(f"요약  : {article['summary']}")
    print("-" * 55)
    print("Gemini 호출 중...")

    try:
        response = await model.generate_content_async(prompt)
        text = response.text.strip()

        if "```" in text:
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        text = text.strip()

        result = json.loads(text)
        print(f"✅ 성공")
        print(f"  섹터  : {result.get('sector')}")
        print(f"  감성  : {result.get('sentiment')}")
        print(f"  신뢰도: {result.get('confidence', 0):.0%}")
        print(f"  근거  : {result.get('reason')}")
    except json.JSONDecodeError:
        print(f"✅ API 호출 성공 (JSON 파싱 실패)")
        print(f"  원문 응답: {response.text[:300]}")
    except Exception as e:
        print(f"❌ 실패: {e}")


if __name__ == "__main__":
    asyncio.run(main())
