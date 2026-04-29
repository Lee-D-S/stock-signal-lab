from datetime import date

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models.database import get_session
from models.trade_log import TradeLog

router = APIRouter(prefix="/trades", tags=["trades"])


@router.get("")
async def get_trades(
    start: date | None = Query(None, description="시작일 (YYYY-MM-DD)"),
    end: date | None = Query(None, description="종료일 (YYYY-MM-DD)"),
    ticker: str | None = Query(None),
    side: str | None = Query(None, description="buy 또는 sell"),
    limit: int = Query(100, le=500),
    session: AsyncSession = Depends(get_session),
):
    stmt = select(TradeLog).order_by(TradeLog.created_at.desc()).limit(limit)

    if start:
        stmt = stmt.where(TradeLog.created_at >= start)
    if end:
        stmt = stmt.where(TradeLog.created_at <= end)
    if ticker:
        stmt = stmt.where(TradeLog.ticker == ticker)
    if side:
        stmt = stmt.where(TradeLog.side == side)

    result = await session.execute(stmt)
    trades = result.scalars().all()

    return [
        {
            "id": t.id,
            "ticker": t.ticker,
            "name": t.name,
            "side": t.side,
            "quantity": t.quantity,
            "price": t.price,
            "amount": t.amount,
            "strategy": t.strategy,
            "reason": t.reason,
            "created_at": t.created_at,
        }
        for t in trades
    ]
