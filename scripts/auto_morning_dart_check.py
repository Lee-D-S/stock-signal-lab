"""평일 08:50 자동 실행 — 관찰 종목 DART 공시 확인 후 텔레그램 알림."""
from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import aiohttp
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config import settings  # noqa: E402

DART_API_KEY = settings.dart_api_key
DART_BASE_URL = "https://opendart.fss.or.kr/api"
CORP_CODE_CACHE = ROOT / "data" / "dart_corp_codes.json"
OBS_DIR = ROOT / "ai 주가 변동 원인 분석" / "08_관찰기록"
OBS_UTF8_CSV = OBS_DIR / "관찰_로그(이상).csv"
OBS_CP949_CSV = OBS_DIR / "관찰_로그.csv"


def load_watched_tickers() -> list[tuple[str, str]]:
    """관찰 로그에서 아직 결과 라벨이 비어 있는 종목 (ticker, name) 반환."""
    if OBS_UTF8_CSV.exists():
        df = pd.read_csv(OBS_UTF8_CSV, encoding="utf-8-sig", dtype={"ticker": str})
    elif OBS_CP949_CSV.exists():
        df = pd.read_csv(OBS_CP949_CSV, encoding="cp949", dtype={"ticker": str})
    else:
        return []
    if df.empty or not {"ticker", "name", "result_label"}.issubset(df.columns):
        return []
    active = df[df["result_label"].isna() | (df["result_label"] == "")]
    return list(zip(active["ticker"].str.zfill(6), active["name"]))


def load_corp_codes() -> dict[str, str]:
    """data/dart_corp_codes.json → {stock_code: corp_code}"""
    if not CORP_CODE_CACHE.exists():
        return {}
    with open(CORP_CODE_CACHE, encoding="utf-8") as f:
        return json.load(f)


async def fetch_disclosures(session: aiohttp.ClientSession, corp_code: str, bgn_de: str, end_de: str) -> list[dict]:
    url = f"{DART_BASE_URL}/list.json"
    params = {
        "crtfc_key": DART_API_KEY,
        "corp_code": corp_code,
        "bgn_de": bgn_de,
        "end_de": end_de,
        "sort": "date",
        "sort_mth": "desc",
        "page_no": "1",
        "page_count": "10",
    }
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        data = await resp.json(content_type=None)
    if data.get("status") != "000":
        return []
    return data.get("list", [])


async def send_telegram(message: str) -> None:
    token = settings.telegram_bot_token
    chat_id = settings.telegram_chat_id
    if not token or not chat_id:
        print("[telegram] 설정 없음, 출력만 합니다.")
        print(message)
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                print(f"[telegram] 전송 실패: {resp.status}")


async def main() -> None:
    today = datetime.now()
    # 오늘 포함 최근 2 거래일 범위
    bgn = (today - timedelta(days=3)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")

    tickers = load_watched_tickers()
    if not tickers:
        print("관찰 종목 없음")
        return

    corp_codes = load_corp_codes()
    found: list[str] = []

    async with aiohttp.ClientSession() as session:
        for ticker, name in tickers:
            corp_code = corp_codes.get(ticker)
            if not corp_code:
                print(f"[{name}] corp_code 없음 (ticker={ticker})")
                continue
            try:
                disclosures = await fetch_disclosures(session, corp_code, bgn, end)
            except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                print(f"[{name}] DART 조회 실패: {type(exc).__name__}: {exc}")
                continue
            if disclosures:
                lines = [f"📋 <b>{name} ({ticker})</b> 공시 {len(disclosures)}건"]
                for d in disclosures[:3]:
                    lines.append(f"  • [{d.get('rcept_dt','')}] {d.get('report_nm','')}")
                found.append("\n".join(lines))
            else:
                print(f"[{name}] 신규 공시 없음")
            await asyncio.sleep(0.3)

    if found:
        msg = f"🔔 <b>오전 공시 확인 ({today.strftime('%Y-%m-%d')})</b>\n\n" + "\n\n".join(found)
    else:
        msg = f"✅ <b>오전 공시 확인 ({today.strftime('%Y-%m-%d')})</b>\n관찰 종목 신규 공시 없음"

    await send_telegram(msg)
    print(msg)


if __name__ == "__main__":
    asyncio.run(main())
