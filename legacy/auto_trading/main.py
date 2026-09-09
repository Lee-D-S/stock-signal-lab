import asyncio
import logging

import uvicorn
from sqlalchemy import delete

from config import settings
from core import broker
from dashboard.main import app as dashboard_app
from models.database import AsyncSessionLocal, init_db
from models.position import Position
from scheduler.runner import create_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


async def sync_positions() -> None:
    """시작 시 증권사 실제 잔고로 DB Position 동기화"""
    try:
        holdings = await broker.get_balance()
        async with AsyncSessionLocal() as session:
            await session.execute(delete(Position))
            for h in holdings:
                session.add(Position(
                    ticker=h["ticker"],
                    name=h["name"],
                    quantity=h["quantity"],
                    avg_price=h["avg_price"],
                    strategy="unknown",
                ))
            await session.commit()
        logger.info(f"포지션 동기화 완료: {len(holdings)}종목")
    except Exception as e:
        logger.warning(f"포지션 동기화 실패 (계속 진행): {e}")


async def run_dashboard() -> None:
    config = uvicorn.Config(
        app=dashboard_app,
        host=settings.dashboard_host,
        port=settings.dashboard_port,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    await server.serve()


async def main() -> None:
    logger.info("DB 초기화 중...")
    await init_db()

    logger.info("포지션 동기화 중...")
    await sync_positions()

    logger.info("스케줄러 시작...")
    scheduler = create_scheduler()
    scheduler.start()

    mode = "모의투자" if settings.kis_is_mock else "실거래"
    logger.info(f"Auto Invest 시작 [{mode}] — 대시보드: http://localhost:{settings.dashboard_port}/docs")

    await run_dashboard()


if __name__ == "__main__":
    asyncio.run(main())
