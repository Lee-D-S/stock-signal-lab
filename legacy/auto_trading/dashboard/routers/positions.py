from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core import broker
from models.database import get_session
from models.position import Position

router = APIRouter(prefix="/positions", tags=["positions"])


@router.get("")
async def get_positions(session: AsyncSession = Depends(get_session)):
    """DB 기준 보유 포지션 조회"""
    result = await session.execute(select(Position))
    positions = result.scalars().all()

    return [
        {
            "ticker": p.ticker,
            "name": p.name,
            "quantity": p.quantity,
            "avg_price": p.avg_price,
            "strategy": p.strategy,
            "updated_at": p.updated_at,
        }
        for p in positions
    ]


@router.get("/realtime")
async def get_realtime_balance():
    """증권사 API 실시간 잔고 조회"""
    return await broker.get_balance()
