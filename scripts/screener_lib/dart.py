"""DART 전자공시 API 클라이언트.

환경변수:
    DART_API_KEY: DART OpenAPI 인증키 (https://opendart.fss.or.kr 에서 발급)

종목코드 → DART 고유번호 매핑은 data/dart_corp_codes.json 에 캐시.
캐시가 30일 이상 지나면 자동 갱신.
"""

import asyncio
import io
import json
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET

import aiohttp
from config import settings

DART_API_KEY  = settings.dart_api_key
DART_BASE_URL = "https://opendart.fss.or.kr/api"
CORP_CODE_CACHE = Path("data/dart_corp_codes.json")
CORP_INFO_CACHE = Path("data/dart_corp_info.json")
CACHE_TTL_DAYS  = 30

_TARGET_ACCOUNTS = {
    "매출액", "수익(매출액)",
    "영업이익", "당기순이익",
    "자산총계", "부채총계", "자본총계",
}
_ACCOUNT_FIELD = {
    "매출액":      "revenue",
    "수익(매출액)": "revenue",
    "영업이익":    "op_income",
    "당기순이익":  "net_income",
    "자산총계":    "total_assets",
    "부채총계":    "total_debt",
    "자본총계":    "equity",
}


async def _download_corp_codes() -> dict[str, str]:
    """DART에서 전체 법인코드 ZIP을 받아 종목코드 → 고유번호 매핑 반환."""
    url = f"{DART_BASE_URL}/corpCode.xml?crtfc_key={DART_API_KEY}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            data = await resp.read()

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        xml_data = zf.read("CORPCODE.xml")

    root = ET.fromstring(xml_data)
    mapping: dict[str, str] = {}
    for item in root.findall("list"):
        stock_code = (item.findtext("stock_code") or "").strip()
        corp_code  = (item.findtext("corp_code")  or "").strip()
        if stock_code and corp_code:
            mapping[stock_code] = corp_code
    return mapping


async def _download_corp_info() -> dict[str, dict[str, str]]:
    """DART 전체 법인코드 ZIP을 받아 종목코드별 회사명/법인코드를 반환."""
    url = f"{DART_BASE_URL}/corpCode.xml?crtfc_key={DART_API_KEY}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            data = await resp.read()

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        xml_data = zf.read("CORPCODE.xml")

    root = ET.fromstring(xml_data)
    mapping: dict[str, dict[str, str]] = {}
    for item in root.findall("list"):
        stock_code = (item.findtext("stock_code") or "").strip()
        corp_code = (item.findtext("corp_code") or "").strip()
        corp_name = (item.findtext("corp_name") or "").strip()
        if stock_code and corp_code and corp_name:
            mapping[stock_code] = {
                "corp_code": corp_code,
                "corp_name": corp_name,
            }
    return mapping


async def get_corp_code_map() -> dict[str, str]:
    """종목코드 → DART 고유번호 매핑. 로컬 캐시 우선, 만료 시 자동 갱신."""
    CORP_CODE_CACHE.parent.mkdir(parents=True, exist_ok=True)

    if CORP_CODE_CACHE.exists():
        mtime = datetime.fromtimestamp(CORP_CODE_CACHE.stat().st_mtime)
        if datetime.now() - mtime < timedelta(days=CACHE_TTL_DAYS):
            with open(CORP_CODE_CACHE, encoding="utf-8") as f:
                return json.load(f)

    print("DART 법인코드 다운로드 중...")
    mapping = await _download_corp_codes()
    with open(CORP_CODE_CACHE, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False)
    print(f"법인코드 캐시 저장 완료 ({len(mapping):,}개)")
    return mapping


async def get_corp_info_map() -> dict[str, dict[str, str]]:
    """종목코드 -> {corp_code, corp_name} 매핑. 로컬 캐시 우선, 만료 시 자동 갱신."""
    CORP_INFO_CACHE.parent.mkdir(parents=True, exist_ok=True)

    if CORP_INFO_CACHE.exists():
        mtime = datetime.fromtimestamp(CORP_INFO_CACHE.stat().st_mtime)
        if datetime.now() - mtime < timedelta(days=CACHE_TTL_DAYS):
            with open(CORP_INFO_CACHE, encoding="utf-8") as f:
                return json.load(f)

    print("DART 회사명/법인코드 다운로드 중...")
    mapping = await _download_corp_info()
    with open(CORP_INFO_CACHE, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False)
    print(f"회사명/법인코드 캐시 저장 완료 ({len(mapping):,}개)")
    return mapping


def _latest_annual_params() -> tuple[str, str]:
    """현재 날짜 기준 조회 가능한 가장 최근 사업보고서 연도 반환.

    사업보고서(11011)는 결산 후 90일 이내 제출 → 4월 이후면 전년도 확정.
    """
    today = datetime.today()
    year  = today.year - 1 if today.month >= 4 else today.year - 2
    return str(year), "11011"  # 11011 = 사업보고서


async def fetch_dart_fundamentals(tickers: list[str]) -> dict[str, dict]:
    """여러 종목의 DART 연간 재무제표 일괄 조회.

    Returns:
        {ticker: {
            "revenue": int,       # 매출액 (원)
            "op_income": int,     # 영업이익
            "net_income": int,    # 당기순이익
            "total_assets": int,  # 자산총계
            "total_debt": int,    # 부채총계
            "equity": int,        # 자본총계
            "op_margin": float,   # 영업이익률 (%)
            "debt_ratio": float,  # 부채비율 (%)
            "roe": float,         # ROE (%)
        }}
    """
    if not DART_API_KEY:
        print("경고: DART_API_KEY 미설정. 재무 조건 건너뜀.")
        return {}

    corp_map = await get_corp_code_map()
    bsns_year, reprt_code = _latest_annual_params()

    corp_codes   = {t: corp_map[t] for t in tickers if t in corp_map}
    ticker_by_corp = {v: k for k, v in corp_codes.items()}
    if not corp_codes:
        return {}

    raw: dict[str, dict] = {}  # ticker → {field: amount, "_fs_div": "CFS"/"OFS"}

    async with aiohttp.ClientSession() as session:
        codes_list = list(corp_codes.values())
        for i in range(0, len(codes_list), 100):
            batch = codes_list[i : i + 100]
            params = {
                "crtfc_key":  DART_API_KEY,
                "corp_code":  ",".join(batch),
                "bsns_year":  bsns_year,
                "reprt_code": reprt_code,
            }
            async with session.get(
                f"{DART_BASE_URL}/fnlttMultiAcnt.json",
                params=params,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = await resp.json(content_type=None)

            if data.get("status") != "000":
                continue

            for row in data.get("list", []):
                corp_code  = row.get("corp_code", "")
                account_nm = (row.get("account_nm") or "").strip()
                fs_div     = row.get("fs_div", "OFS")
                ticker     = ticker_by_corp.get(corp_code)

                if not ticker or account_nm not in _TARGET_ACCOUNTS:
                    continue

                entry = raw.setdefault(ticker, {})
                # 연결재무제표(CFS) 우선, 이미 CFS 있으면 별도(OFS) 무시
                if fs_div == "OFS" and entry.get("_fs_div") == "CFS":
                    continue

                amount_str = (row.get("thstrm_amount") or "").replace(",", "")
                try:
                    amount = int(amount_str)
                except ValueError:
                    continue

                field = _ACCOUNT_FIELD.get(account_nm)
                if field:
                    entry[field]     = amount
                    entry["_fs_div"] = fs_div

            await asyncio.sleep(0.3)

    # 파생 지표 계산 후 내부 필드 제거
    result: dict[str, dict] = {}
    for ticker, d in raw.items():
        rev = d.get("revenue")
        op  = d.get("op_income")
        net = d.get("net_income")
        eq  = d.get("equity")
        dbt = d.get("total_debt")

        out = {k: v for k, v in d.items() if not k.startswith("_")}
        if rev and op:
            out["op_margin"] = round(op / rev * 100, 2)
        if eq and dbt:
            out["debt_ratio"] = round(dbt / eq * 100, 2)
        if eq and net:
            out["roe"] = round(net / eq * 100, 2)
        assets = d.get("total_assets")
        if assets and net:
            out["roa"] = round(net / assets * 100, 2)
        if rev and net:
            out["net_margin"] = round(net / rev * 100, 2)
        result[ticker] = out

    return result
