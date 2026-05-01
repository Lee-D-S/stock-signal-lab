import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import httpx

from config import settings

logger = logging.getLogger(__name__)

_TOKEN_CACHE_FILE = Path(".token_cache.json")
_REAL_TOKEN_CACHE_FILE = Path(".token_cache_real.json")

@dataclass
class TokenInfo:
    access_token: str
    expires_at: float  # Unix timestamp


_token_cache: TokenInfo | None = None
_real_token_cache: TokenInfo | None = None
_real_token_lock: asyncio.Lock | None = None


def _get_real_lock() -> asyncio.Lock:
    global _real_token_lock
    if _real_token_lock is None:
        _real_token_lock = asyncio.Lock()
    return _real_token_lock


def _remaining_minutes(token: TokenInfo) -> int:
    return max(0, int((token.expires_at - time.time()) // 60))


def _load_from_file() -> TokenInfo | None:
    try:
        if not _TOKEN_CACHE_FILE.exists():
            return None
        data = json.loads(_TOKEN_CACHE_FILE.read_text())
        token = TokenInfo(**data)
        if time.time() < token.expires_at - 60:
            return token
    except Exception:
        pass
    return None


def _save_to_file(token: TokenInfo) -> None:
    try:
        _TOKEN_CACHE_FILE.write_text(json.dumps(asdict(token)))
    except Exception as e:
        logger.warning(f"토큰 파일 저장 실패: {e}")


async def get_access_token() -> str:
    global _token_cache

    if _token_cache and time.time() < _token_cache.expires_at - 60:
        logger.info("KIS 토큰 재사용: memory_cache (잔여 %d분)", _remaining_minutes(_token_cache))
        return _token_cache.access_token

    file_token = _load_from_file()
    if file_token:
        _token_cache = file_token
        logger.info("KIS 토큰 재사용: file_cache (잔여 %d분)", _remaining_minutes(_token_cache))
        return _token_cache.access_token

    logger.info("KIS 토큰 신규 발급: file_cache 만료 또는 없음")
    return await _issue_token()


async def get_real_access_token() -> str:
    """실전 서버 전용 토큰 (KIS_IS_MOCK=true여도 실전 서버 자격증명 사용)."""
    global _real_token_cache

    # 1. 메모리 캐시
    if _real_token_cache and time.time() < _real_token_cache.expires_at - 60:
        logger.info("KIS 실전 토큰 재사용: memory_cache (잔여 %d분)", _remaining_minutes(_real_token_cache))
        return _real_token_cache.access_token

    # 2. 파일 캐시 (프로세스 재시작 후에도 재사용 → 1분 제한 방지)
    try:
        if _REAL_TOKEN_CACHE_FILE.exists():
            saved = json.loads(_REAL_TOKEN_CACHE_FILE.read_text())
            token = TokenInfo(**saved)
            if time.time() < token.expires_at - 60:
                _real_token_cache = token
                logger.info("KIS 실전 토큰 재사용: file_cache (잔여 %d분)", _remaining_minutes(_real_token_cache))
                return _real_token_cache.access_token
    except Exception:
        pass

    # 3. 신규 발급 — 동시 호출 방지 (KIS: 1분당 1회 제한)
    async with _get_real_lock():
        # 락 획득 후 재확인
        if _real_token_cache and time.time() < _real_token_cache.expires_at - 60:
            logger.info("KIS 실전 토큰 재사용: memory_cache_after_lock (잔여 %d분)", _remaining_minutes(_real_token_cache))
            return _real_token_cache.access_token

        url = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"
        payload = {
            "grant_type": "client_credentials",
            "appkey": settings.kis_real_app_key,
            "appsecret": settings.kis_real_app_secret,
        }

        async with httpx.AsyncClient() as http:
            resp = await http.post(url, json=payload, timeout=10)
            if not resp.is_success:
                logger.error("실전 서버 토큰 발급 실패 [%s]: %s", resp.status_code, resp.text)
            resp.raise_for_status()
            data = resp.json()

        expires_in = int(data.get("expires_in", 86400))
        _real_token_cache = TokenInfo(
            access_token=data["access_token"],
            expires_at=time.time() + expires_in,
        )
        try:
            _REAL_TOKEN_CACHE_FILE.write_text(json.dumps(asdict(_real_token_cache)))
        except Exception as e:
            logger.warning("실전 토큰 파일 저장 실패: %s", e)

        logger.info("KIS 실전 토큰 신규 발급 완료 (잔여 %d분)", _remaining_minutes(_real_token_cache))
        return _real_token_cache.access_token


async def _issue_token() -> str:
    global _token_cache

    url = f"{settings.kis_base_url}/oauth2/tokenP"
    payload = {
        "grant_type": "client_credentials",
        "appkey": settings.kis_app_key,
        "appsecret": settings.kis_app_secret,
    }

    async with httpx.AsyncClient() as http:
        resp = await http.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()

    access_token = data["access_token"]
    expires_in = int(data.get("expires_in", 86400))

    _token_cache = TokenInfo(
        access_token=access_token,
        expires_at=time.time() + expires_in,
    )
    _save_to_file(_token_cache)
    logger.info("KIS 토큰 신규 발급 완료 (잔여 %d분)", _remaining_minutes(_token_cache))
    return access_token
