"""날짜별 상장 종목 로더 — 생존 편향 제거.

전략:
    DART 공시목록(list.json) 에서 기간 내 정기공시(사업보고서/분기보고서)를
    제출한 법인을 조회 → 해당 기간에 상장 중이었던 종목으로 판단.

한계:
    - 폐지 기업이 마지막 보고서를 백테스트 시작 전에 제출했다면 누락 가능
    - 역으로 기간 중 일시적으로 공시 누락된 기업이 제외될 수 있음
    - 정확한 상장/폐지일 대신 분기 단위 근사치 사용

캐시:
    data/dart_universe_cache/{start}_{end}.json 에 조회 결과 저장
    → 동일 기간 재실행 시 API 재호출 없음
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import aiohttp

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

DART_API_KEY  = os.getenv("DART_API_KEY", "")
DART_BASE_URL = "https://opendart.fss.or.kr/api"
CACHE_DIR     = ROOT / "data" / "dart_universe_cache"


def _yyyymmdd(date_str: str) -> str:
    return date_str.replace("-", "")


async def _fetch_ticker_page(
    session: aiohttp.ClientSession,
    bgn_de: str,
    end_de: str,
    corp_cls: str,
    page_no: int,
) -> tuple[set[str], int]:
    """단일 페이지 조회 → (종목코드 집합, 전체 페이지 수)."""
    try:
        async with session.get(
            f"{DART_BASE_URL}/list.json",
            params={
                "crtfc_key":  DART_API_KEY,
                "bgn_de":     bgn_de,
                "end_de":     end_de,
                "pblntf_ty":  "A",       # 정기공시 (사업/반기/분기보고서)
                "corp_cls":   corp_cls,
                "page_no":    page_no,
                "page_count": 100,
            },
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            data = await resp.json(content_type=None)
    except Exception:
        return set(), 1

    if data.get("status") != "000":
        return set(), 1

    tickers = set()
    for item in data.get("list", []):
        sc = (item.get("stock_code") or "").strip()
        if sc and len(sc) == 6 and sc.isdigit():
            tickers.add(sc)

    total_page = max(int(data.get("total_page", 1)), 1)
    return tickers, total_page


async def _fetch_tickers_for_market(
    bgn_de: str,
    end_de: str,
    corp_cls: str,
) -> set[str]:
    """특정 시장(KOSPI/KOSDAQ)의 기간 내 공시 종목코드 전체 수집."""
    tickers: set[str] = set()
    page_no = 1

    async with aiohttp.ClientSession() as session:
        while True:
            page_tickers, total_page = await _fetch_ticker_page(
                session, bgn_de, end_de, corp_cls, page_no
            )
            tickers.update(page_tickers)
            if page_no >= total_page:
                break
            page_no += 1
            await asyncio.sleep(0.3)

    return tickers


async def get_historical_tickers(
    start: str,
    end: str,
    force_refresh: bool = False,
) -> list[str]:
    """기간 내 상장 중이었던 종목코드 리스트 반환 (DART 공시 기반).

    Args:
        start: 백테스트 시작일 "YYYY-MM-DD"
        end:   백테스트 종료일 "YYYY-MM-DD"
        force_refresh: True 이면 캐시 무시

    Returns:
        sorted list of ticker strings (e.g. ["005930", "000660", ...])
    """
    if not DART_API_KEY:
        print("[universe_loader] DART_API_KEY 미설정. 현재 시총 기반 유니버스 사용 권장.")
        return []

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_key  = f"{start}_{end}".replace("-", "")
    cache_path = CACHE_DIR / f"{cache_key}.json"

    if not force_refresh and cache_path.exists():
        with open(cache_path, encoding="utf-8") as f:
            cached = json.load(f)
        print(f"[universe_loader] 캐시 로드: {len(cached)}개 종목 ({start}~{end})")
        return cached

    # 정기보고서 공시 기간: 백테스트 시작 3개월 전 ~ 종료 6개월 후
    # (연간보고서가 결산 후 90일 이내 제출되므로 여유 확보)
    dt_start = datetime.strptime(start, "%Y-%m-%d") - timedelta(days=90)
    dt_end   = datetime.strptime(end, "%Y-%m-%d")   + timedelta(days=180)
    bgn_de   = dt_start.strftime("%Y%m%d")
    end_de   = dt_end.strftime("%Y%m%d")

    print(f"[universe_loader] DART 공시 조회 중 ({bgn_de}~{end_de})...")

    kospi_tickers, kosdaq_tickers = await asyncio.gather(
        _fetch_tickers_for_market(bgn_de, end_de, "Y"),
        _fetch_tickers_for_market(bgn_de, end_de, "K"),
    )

    all_tickers = sorted(kospi_tickers | kosdaq_tickers)
    print(
        f"[universe_loader] KOSPI: {len(kospi_tickers)}개 / "
        f"KOSDAQ: {len(kosdaq_tickers)}개 / "
        f"합계: {len(all_tickers)}개"
    )

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(all_tickers, f, ensure_ascii=False)
    print(f"[universe_loader] 캐시 저장: {cache_path}")

    return all_tickers


async def load_historical_universe_ohlcv(
    start: str,
    end: str,
    force_refresh: bool = False,
    max_tickers: int | None = None,
) -> dict:
    """기간 내 상장 종목 OHLCV 일괄 로드.

    Args:
        start / end: 백테스트 구간
        force_refresh: DART 캐시 + OHLCV 캐시 무시
        max_tickers: 테스트용 종목 수 제한 (None = 전체)

    Returns:
        {ticker: DataFrame(date, open, high, low, close, volume)}
    """
    from backtest.data_loader import load_universe_ohlcv

    tickers = await get_historical_tickers(start, end, force_refresh=force_refresh)
    if not tickers:
        return {}

    if max_tickers is not None:
        tickers = tickers[:max_tickers]
        print(f"[universe_loader] --max-tickers={max_tickers} 적용 → {len(tickers)}개")

    # 지표 warm-up 포함 로드 시작일
    load_start = (
        datetime.strptime(start, "%Y-%m-%d") - timedelta(days=425)
    ).strftime("%Y-%m-%d")

    print(f"[universe_loader] OHLCV 로드 시작 ({len(tickers)}개 종목)...")
    return await load_universe_ohlcv(
        tickers=tickers,
        start=load_start,
        end=end,
        force_refresh=force_refresh,
    )
