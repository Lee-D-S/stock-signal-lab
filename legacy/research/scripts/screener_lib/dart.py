"""DART 전자공시 API 클라이언트.

환경변수:
    DART_API_KEY: DART OpenAPI 인증키 (https://opendart.fss.or.kr 에서 발급)

종목코드 → DART 고유번호 매핑은 data/dart_corp_codes.json 에 캐시.
캐시가 30일 이상 지나면 자동 갱신.
"""

import asyncio
import io
import json
import os
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET

import aiohttp
from config import settings

DART_API_KEY  = settings.dart_api_key
DART_BASE_URL = "https://opendart.fss.or.kr/api"
ROOT = Path(__file__).resolve().parents[2]
CORP_CODE_CACHE = ROOT / "data" / "dart_corp_codes.json"
CORP_INFO_CACHE = ROOT / "data" / "dart_corp_info.json"
CACHE_TTL_DAYS  = 30
DOWNLOAD_TIMEOUT = aiohttp.ClientTimeout(total=120, sock_connect=30, sock_read=120)
DOWNLOAD_RETRIES = 3
DOWNLOAD_BACKOFF_SECONDS = 5

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


def _is_github_actions() -> bool:
    return os.environ.get("GITHUB_ACTIONS", "").lower() == "true"


def _is_cache_fresh(path: Path) -> bool:
    if not path.exists():
        return False
    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    return datetime.now() - mtime < timedelta(days=CACHE_TTL_DAYS)


def _load_json_cache(path: Path) -> dict | None:
    try:
        if path.exists():
            with open(path, encoding="utf-8") as f:
                return json.load(f)
    except Exception as exc:
        print(f"DART 캐시 로드 실패: {path} ({exc!r})")
    return None


async def _download_corp_codes() -> dict[str, str]:
    """DART에서 전체 법인코드 ZIP을 받아 종목코드 → 고유번호 매핑 반환."""
    url = f"{DART_BASE_URL}/corpCode.xml?crtfc_key={DART_API_KEY}"
    last_error: Exception | None = None
    async with aiohttp.ClientSession() as session:
        for attempt in range(1, DOWNLOAD_RETRIES + 1):
            try:
                async with session.get(url, timeout=DOWNLOAD_TIMEOUT) as resp:
                    data = await resp.read()
                break
            except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                last_error = exc
                if attempt >= DOWNLOAD_RETRIES:
                    raise
                await asyncio.sleep(DOWNLOAD_BACKOFF_SECONDS * attempt)
        else:  # pragma: no cover - defensive fallback
            raise last_error or RuntimeError("DART corp code download failed")

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
    last_error: Exception | None = None
    async with aiohttp.ClientSession() as session:
        for attempt in range(1, DOWNLOAD_RETRIES + 1):
            try:
                async with session.get(url, timeout=DOWNLOAD_TIMEOUT) as resp:
                    data = await resp.read()
                break
            except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                last_error = exc
                if attempt >= DOWNLOAD_RETRIES:
                    raise
                await asyncio.sleep(DOWNLOAD_BACKOFF_SECONDS * attempt)
        else:  # pragma: no cover - defensive fallback
            raise last_error or RuntimeError("DART corp info download failed")

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

    cached = _load_json_cache(CORP_CODE_CACHE)
    if cached and _is_cache_fresh(CORP_CODE_CACHE):
        print(f"DART 법인코드 캐시 사용: {CORP_CODE_CACHE} ({len(cached):,}개)")
        return cached
    if cached and _is_github_actions():
        print(f"DART 법인코드 캐시 사용: stale_allowed_on_actions ({len(cached):,}개)")
        return cached
    if _is_github_actions():
        print(f"DART 법인코드 캐시 없음: Actions에서는 전체 다운로드를 건너뜀 ({CORP_CODE_CACHE})")
        return {}

    print("DART 법인코드 다운로드 중...")
    try:
        mapping = await _download_corp_codes()
    except Exception as exc:
        if cached:
            print(f"DART 법인코드 다운로드 실패, 기존 캐시 사용: {exc!r}")
            return cached
        print(f"DART 법인코드 다운로드 실패, 빈 매핑으로 계속 진행: {exc!r}")
        return {}
    with open(CORP_CODE_CACHE, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False)
    print(f"법인코드 캐시 저장 완료 ({len(mapping):,}개)")
    return mapping


async def get_corp_info_map() -> dict[str, dict[str, str]]:
    """종목코드 -> {corp_code, corp_name} 매핑. 로컬 캐시 우선, 만료 시 자동 갱신."""
    CORP_INFO_CACHE.parent.mkdir(parents=True, exist_ok=True)

    cached = _load_json_cache(CORP_INFO_CACHE)
    if cached and _is_cache_fresh(CORP_INFO_CACHE):
        print(f"DART 회사정보 캐시 사용: {CORP_INFO_CACHE} ({len(cached):,}개)")
        return cached
    if cached and _is_github_actions():
        print(f"DART 회사정보 캐시 사용: stale_allowed_on_actions ({len(cached):,}개)")
        return cached
    if _is_github_actions():
        print(f"DART 회사정보 캐시 없음: Actions에서는 전체 다운로드를 건너뜀 ({CORP_INFO_CACHE})")
        return {}

    print("DART 회사명/법인코드 다운로드 중...")
    try:
        mapping = await _download_corp_info()
    except Exception as exc:
        if cached:
            print(f"DART 회사정보 다운로드 실패, 기존 캐시 사용: {exc!r}")
            return cached
        print(f"DART 회사정보 다운로드 실패, 빈 매핑으로 계속 진행: {exc!r}")
        return {}
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
