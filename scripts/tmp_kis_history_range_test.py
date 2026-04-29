"""KIS API 과거 데이터 범위 테스트.

삼성전자(005930)로 3개 엔드포인트의 가장 오래된 데이터 날짜를 확인.
  - OHLCV: inquire-daily-itemchartprice (FHKST03010100)
  - 투자자별 매매동향: investor-trade-by-stock-daily (FHPTJ04160001)
  - 공매도: daily-short-sale (FHPST04830000)
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import httpx  # noqa: E402
import pandas as pd  # noqa: E402

from config import settings  # noqa: E402
from core.api.auth import get_real_access_token  # noqa: E402
from core.api.client import get_marketdata  # noqa: E402

TICKER = "005930"
TEST_RANGES = [
    ("2021-01-01", "2021-03-31", "2021_Q1"),
    ("2022-01-01", "2022-03-31", "2022_Q1"),
    ("2023-01-01", "2023-03-31", "2023_Q1"),
    ("2024-01-01", "2024-03-31", "2024_Q1"),
]


async def kis_get(path: str, params: dict, tr_id: str) -> dict:
    token = await get_real_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "appkey": settings.kis_real_app_key,
        "appsecret": settings.kis_real_app_secret,
        "tr_id": tr_id,
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=20) as http:
        resp = await http.get(
            f"https://openapi.koreainvestment.com:9443{path}",
            headers=headers,
            params=params,
        )
    resp.raise_for_status()
    data = resp.json()
    if data.get("rt_cd", "0") != "0":
        raise RuntimeError(f"KIS API error {tr_id}: {data.get('msg1')}")
    return data


async def test_ohlcv(start: str, end: str, label: str) -> None:
    try:
        data = await get_marketdata(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": TICKER,
                "FID_INPUT_DATE_1": start.replace("-", ""),
                "FID_INPUT_DATE_2": end.replace("-", ""),
                "FID_PERIOD_DIV_CODE": "D",
                "FID_ORG_ADJ_PRC": "0",
            },
            tr_id="FHKST03010100",
        )
        rows = data.get("output2", [])
        dates = [r.get("stck_bsop_date") for r in rows if r.get("stck_bsop_date")]
        if dates:
            print(f"  [OHLCV]  {label}: OK - {len(dates)}일, {min(dates)} ~ {max(dates)}")
        else:
            print(f"  [OHLCV]  {label}: 응답 있으나 데이터 없음 (rt_cd={data.get('rt_cd')}, msg={data.get('msg1')})")
    except Exception as e:
        print(f"  [OHLCV]  {label}: ERROR - {e}")


async def test_investor(end: str, label: str) -> None:
    try:
        data = await kis_get(
            "/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily",
            {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": TICKER,
                "FID_INPUT_DATE_1": end.replace("-", ""),
                "FID_ORG_ADJ_PRC": "",
                "FID_ETC_CLS_CODE": "",
            },
            "FHPTJ04160001",
        )
        rows = data.get("output2") or data.get("output1") or data.get("output") or []
        if isinstance(rows, dict):
            rows = [rows]
        dates = [r.get("stck_bsop_date") or r.get("bsop_date") or r.get("trad_dt") for r in rows]
        dates = [d for d in dates if d]
        if dates:
            print(f"  [투자자] {label}: OK - {len(dates)}일, {min(dates)} ~ {max(dates)}")
        else:
            print(f"  [투자자] {label}: 응답 있으나 데이터 없음 (rt_cd={data.get('rt_cd')}, msg={data.get('msg1')})")
    except Exception as e:
        print(f"  [투자자] {label}: ERROR - {e}")


async def test_short_sale(start: str, end: str, label: str) -> None:
    try:
        data = await get_marketdata(
            "/uapi/domestic-stock/v1/quotations/daily-short-sale",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": TICKER,
                "FID_INPUT_DATE_1": start.replace("-", ""),
                "FID_INPUT_DATE_2": end.replace("-", ""),
            },
            tr_id="FHPST04830000",
        )
        rows = data.get("output2", []) or []
        dates = [r.get("stck_bsop_date") or r.get("bsop_date") for r in rows if r]
        dates = [d for d in dates if d]
        if dates:
            print(f"  [공매도] {label}: OK - {len(dates)}일, {min(dates)} ~ {max(dates)}")
        else:
            print(f"  [공매도] {label}: 응답 있으나 데이터 없음 (rt_cd={data.get('rt_cd')}, msg={data.get('msg1')})")
    except Exception as e:
        print(f"  [공매도] {label}: ERROR - {e}")


async def main() -> None:
    print(f"삼성전자({TICKER}) KIS API 과거 데이터 범위 테스트\n")
    for start, end, label in TEST_RANGES:
        print(f"[{label}] {start} ~ {end}")
        await test_ohlcv(start, end, label)
        await asyncio.sleep(0.5)
        await test_investor(end, label)
        await asyncio.sleep(0.5)
        await test_short_sale(start, end, label)
        await asyncio.sleep(0.5)
        print()


if __name__ == "__main__":
    asyncio.run(main())
