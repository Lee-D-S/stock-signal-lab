from datetime import date

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.market_data import get_sector_changes
from models.database import get_session
from models.sector_signal import SectorSignal

router = APIRouter(prefix="/market", tags=["market"])


@router.get("/sectors")
async def get_sectors(market: str = Query("K", description="K=코스피, Q=코스닥")):
    """업종별 등락률 조회"""
    return await get_sector_changes(market=market)


@router.get("/sector-signals")
async def get_sector_signals(
    target_date: date | None = Query(None, description="조회 날짜 (기본: 오늘)"),
    session: AsyncSession = Depends(get_session),
):
    """날짜별 뉴스 섹터 예측 목록"""
    from datetime import datetime
    day = target_date or datetime.now().date()

    result = await session.execute(
        select(SectorSignal)
        .where(SectorSignal.predicted_at >= day)
        .order_by(SectorSignal.predicted_at.desc())
    )
    signals = result.scalars().all()

    return [
        {
            "id": s.id,
            "title": s.title,
            "sector_name": s.sector_name,
            "sentiment": s.sentiment,
            "confidence": s.confidence,
            "reason": s.reason,
            "predicted_at": s.predicted_at,
            "actual_change_rate": s.actual_change_rate,
            "hit": s.hit,
            "verified_at": s.verified_at,
        }
        for s in signals
    ]


@router.get("/sector-accuracy")
async def get_sector_accuracy(session: AsyncSession = Depends(get_session)):
    """섹터 예측 전체 적중률 요약"""
    result = await session.execute(
        select(SectorSignal).where(SectorSignal.hit.is_not(None))
    )
    signals = result.scalars().all()

    if not signals:
        return {"total": 0, "hits": 0, "accuracy": None}

    by_sector: dict[str, dict] = {}
    for s in signals:
        entry = by_sector.setdefault(s.sector_name, {"total": 0, "hits": 0})
        entry["total"] += 1
        if s.hit:
            entry["hits"] += 1

    total = len(signals)
    hits = sum(1 for s in signals if s.hit)

    return {
        "total": total,
        "hits": hits,
        "accuracy": round(hits / total * 100, 1),
        "by_sector": {
            k: {**v, "accuracy": round(v["hits"] / v["total"] * 100, 1)}
            for k, v in sorted(by_sector.items(), key=lambda x: -x[1]["total"])
        },
    }
