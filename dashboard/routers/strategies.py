from fastapi import APIRouter, Depends, Header, HTTPException

from config import settings
from scheduler.runner import STRATEGIES

router = APIRouter(prefix="/strategies", tags=["strategies"])


def _verify_api_key(x_api_key: str | None = Header(default=None)) -> None:
    """DASHBOARD_API_KEY가 설정된 경우에만 인증을 강제한다."""
    if settings.dashboard_api_key and x_api_key != settings.dashboard_api_key:
        raise HTTPException(status_code=403, detail="유효하지 않은 API 키입니다.")


@router.get("")
async def get_strategies():
    return [
        {
            "name": s.name,
            "enabled": s.enabled,
            "tickers": s.tickers,
        }
        for s in STRATEGIES
    ]


@router.post("/{name}/toggle")
async def toggle_strategy(name: str, _: None = Depends(_verify_api_key)):
    for s in STRATEGIES:
        if s.name == name:
            s.enabled = not s.enabled
            return {"name": name, "enabled": s.enabled}
    raise HTTPException(status_code=404, detail=f"전략 '{name}'을 찾을 수 없습니다.")


@router.post("/{name}/tickers")
async def update_tickers(name: str, tickers: list[str], _: None = Depends(_verify_api_key)):
    for s in STRATEGIES:
        if s.name == name:
            s.tickers = tickers
            return {"name": name, "tickers": s.tickers}
    raise HTTPException(status_code=404, detail=f"전략 '{name}'을 찾을 수 없습니다.")
