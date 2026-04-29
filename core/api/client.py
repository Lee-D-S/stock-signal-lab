import httpx

from config import settings
from .auth import get_access_token, get_real_access_token

REAL_BASE_URL = "https://openapi.koreainvestment.com:9443"


async def get(path: str, params: dict | None = None, tr_id: str = "") -> dict:
    return await _request("GET", path, params=params, tr_id=tr_id)


async def post(path: str, body: dict | None = None, tr_id: str = "") -> dict:
    return await _request("POST", path, body=body, tr_id=tr_id)


async def get_marketdata(
    path: str,
    params: dict | None = None,
    tr_id: str = "",
    tr_cont: str = "",
) -> dict:
    """시세 조회 전용 — 모의투자 환경에서도 항상 실전 서버 사용.

    반환 dict에 '__tr_cont__' 키로 응답 헤더의 tr_cont 값을 포함.
    tr_cont == 'M' 이면 다음 페이지 존재 → tr_cont='N' 으로 재호출.
    """
    return await _request("GET", path, params=params, tr_id=tr_id, force_real=True, tr_cont=tr_cont)


async def _request(
    method: str,
    path: str,
    params: dict | None = None,
    body: dict | None = None,
    tr_id: str = "",
    force_real: bool = False,
    tr_cont: str = "",
) -> dict:
    if force_real:
        token = await get_real_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "appkey": settings.kis_real_app_key,
            "appsecret": settings.kis_real_app_secret,
            "tr_id": tr_id,
            "tr_cont": tr_cont,
            "Content-Type": "application/json",
        }
        url = f"{REAL_BASE_URL}{path}"
    else:
        token = await get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "appkey": settings.kis_app_key,
            "appsecret": settings.kis_app_secret,
            "tr_id": tr_id,
            "tr_cont": tr_cont,
            "Content-Type": "application/json",
        }
        url = f"{settings.kis_base_url}{path}"

    async with httpx.AsyncClient(timeout=10) as http:
        if method == "GET":
            resp = await http.get(url, headers=headers, params=params)
        else:
            resp = await http.post(url, headers=headers, json=body)

    resp.raise_for_status()
    data = resp.json()

    rt_cd = data.get("rt_cd", "0")
    if rt_cd != "0":
        msg = data.get("msg1", "Unknown error")
        raise RuntimeError(f"API error [{rt_cd}]: {msg}")

    # 페이지네이션 여부를 호출자가 확인할 수 있도록 응답 헤더의 tr_cont 포함
    data["__tr_cont__"] = resp.headers.get("tr_cont", "") or resp.headers.get("tr-cont", "")
    return data
