from fastapi import APIRouter, HTTPException

from scheduler.runner import STRATEGIES

router = APIRouter(prefix="/strategies", tags=["strategies"])


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
async def toggle_strategy(name: str):
    for s in STRATEGIES:
        if s.name == name:
            s.enabled = not s.enabled
            return {"name": name, "enabled": s.enabled}
    raise HTTPException(status_code=404, detail=f"전략 '{name}'을 찾을 수 없습니다.")


@router.post("/{name}/tickers")
async def update_tickers(name: str, tickers: list[str]):
    for s in STRATEGIES:
        if s.name == name:
            s.tickers = tickers
            return {"name": name, "tickers": s.tickers}
    raise HTTPException(status_code=404, detail=f"전략 '{name}'을 찾을 수 없습니다.")
