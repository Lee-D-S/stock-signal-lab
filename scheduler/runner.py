import logging
from datetime import datetime

import holidays
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from sqlalchemy import select

from config import settings
from core import broker, market_data
from core.market_data import get_sector_changes
from models.database import AsyncSessionLocal
from models.position import Position
from models.sector_signal import SectorSignal
from models.trade_log import TradeLog
from notifier import telegram
from strategies.base import BaseStrategy
from strategies.ma_cross import MACrossStrategy
from strategies.news_sector import NewsSectorAnalyzer
from strategies.news_sentiment import NewsSentimentStrategy

logger = logging.getLogger(__name__)

# 전략 등록 — 여기에 추가하면 자동으로 실행됨
MA_STRATEGY = MACrossStrategy(
    tickers=["005930", "000660", "035720"],  # 삼성전자, SK하이닉스, 카카오
    short_period=5,
    long_period=20,
    enabled=False,
)

NEWS_STRATEGY = NewsSentimentStrategy(min_confidence=0.75, enabled=False)
SECTOR_ANALYZER = NewsSectorAnalyzer(min_confidence=0.7)

STRATEGIES: list[BaseStrategy] = [MA_STRATEGY, NEWS_STRATEGY]


_kr_holidays = holidays.Korea()


def is_market_open() -> bool:
    """장 운영 시간 여부 확인 (평일 + 공휴일 제외, 09:00~15:30)"""
    now = datetime.now()
    if now.weekday() >= 5:  # 토/일
        return False
    if now.date() in _kr_holidays:
        return False
    market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return market_open <= now <= market_close


async def run_strategy(strategy: BaseStrategy) -> None:
    """단일 전략 실행"""
    if not strategy.enabled or not is_market_open():
        return

    tickers = strategy.tickers

    # 뉴스 전략은 AI가 판단한 종목 포함
    if isinstance(strategy, NewsSentimentStrategy):
        tickers = list(set(tickers + strategy.get_pending_tickers()))

    for ticker in tickers:
        try:
            price_info = await market_data.get_current_price(ticker)
            current_price = price_info["price"]
            name = price_info["name"]

            df = await market_data.get_ohlcv(ticker, period="D", count=60)
            if df.empty:
                continue

            # 매도 먼저 체크 (포지션 보유 종목만)
            if await _has_position(ticker):
                avg_price = await _get_position_avg_price(ticker)
                sell_signal, reason = await _check_stop_take(current_price, avg_price)
                if not sell_signal:
                    sell_signal, reason = await strategy.should_sell(ticker, df)
                if sell_signal:
                    qty = await _get_position_qty(ticker)
                    if qty > 0:
                        result = await broker.sell(ticker, qty, current_price)
                        await _record_trade(ticker, name, "sell", qty, current_price, strategy.name, reason, result.get("order_id", ""))
                        await telegram.notify_sell(ticker, name, qty, current_price, reason)
                        await _remove_position(ticker)
                        logger.info(f"[{strategy.name}] 매도: {name}({ticker}) {qty}주 @ {current_price:,}원")
                continue

            # 매수 체크
            buy_signal, reason = await strategy.should_buy(ticker, df)
            if buy_signal:
                qty = strategy.get_order_quantity(current_price, settings.max_order_amount)
                if qty > 0 and await _can_buy_more():
                    result = await broker.buy(ticker, qty, current_price)
                    await _record_trade(ticker, name, "buy", qty, current_price, strategy.name, reason, result.get("order_id", ""))
                    await telegram.notify_buy(ticker, name, qty, current_price, reason)
                    await _upsert_position(ticker, name, qty, current_price, strategy.name)
                    logger.info(f"[{strategy.name}] 매수: {name}({ticker}) {qty}주 @ {current_price:,}원")

        except Exception as e:
            logger.error(f"[{strategy.name}] {ticker} 처리 오류: {e}")
            await telegram.notify_error(f"{strategy.name}/{ticker}", str(e))


async def run_news_crawl() -> None:
    """뉴스 크롤링 + AI 분석 (별도 스케줄)"""
    try:
        await NEWS_STRATEGY.run_crawl_and_analyze()
    except Exception as e:
        logger.error(f"뉴스 분석 오류: {e}")


async def run_sector_crawl() -> None:
    """뉴스 섹터 감성 분석 (별도 스케줄)"""
    try:
        await SECTOR_ANALYZER.run_crawl_and_analyze()
    except Exception as e:
        logger.error(f"섹터 분석 오류: {e}")


async def run_eod_sector_validation() -> None:
    """장 마감 후 섹터 예측 정확도 검증"""
    try:
        kospi = {s["name"]: s["change_rate"] for s in await get_sector_changes("K")}
        kosdaq = {s["name"]: s["change_rate"] for s in await get_sector_changes("Q")}
        actual = {**kospi, **kosdaq}

        today = datetime.now().date()
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(SectorSignal).where(
                    SectorSignal.predicted_at >= today,
                    SectorSignal.verified_at.is_(None),
                )
            )
            signals = result.scalars().all()

            for sig in signals:
                matched_rate = SECTOR_ANALYZER.match_sector(sig.sector_name, list(actual.keys()))
                change_rate = actual.get(matched_rate) if matched_rate else None

                if change_rate is not None:
                    hit = (sig.sentiment == "positive" and change_rate > 0) or \
                          (sig.sentiment == "negative" and change_rate < 0)
                    sig.actual_change_rate = change_rate
                    sig.hit = hit
                else:
                    sig.hit = None

                sig.verified_at = datetime.now()

            await session.commit()

        verified = len(signals)
        hits = sum(1 for s in signals if s.hit is True)
        logger.info(f"[SectorValidation] 검증 완료: {verified}건, 적중 {hits}건 ({hits/verified*100:.1f}%)" if verified else "[SectorValidation] 검증할 신호 없음")
    except Exception as e:
        logger.error(f"섹터 검증 오류: {e}")


async def run_daily_summary() -> None:
    """장 마감 후 일일 결산 알림"""
    async with AsyncSessionLocal() as session:
        today = datetime.now().date()
        result = await session.execute(
            select(TradeLog).where(TradeLog.created_at >= today)
        )
        trades = result.scalars().all()

    sells = [t for t in trades if t.side == "sell"]
    buys = {t.ticker: t for t in trades if t.side == "buy"}

    total_profit = 0.0
    for sell in sells:
        buy = buys.get(sell.ticker)
        buy_price = buy.price if buy else sell.price  # 당일 매수 없으면 손익 0으로 처리
        total_profit += (sell.price - buy_price) * sell.quantity

    await telegram.notify_daily_summary(total_profit=total_profit, trade_count=len(trades))


def create_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="Asia/Seoul")

    # MA 전략: 5분마다 실행
    scheduler.add_job(run_strategy, "interval", minutes=5, args=[MA_STRATEGY], id="ma_cross")

    # 뉴스 크롤링: N분마다 실행
    scheduler.add_job(
        run_news_crawl,
        "interval",
        minutes=settings.news_crawl_interval_min,
        id="news_crawl",
    )

    # 뉴스 전략 실행: 크롤링 후 2분 뒤부터 5분마다
    scheduler.add_job(run_strategy, "interval", minutes=5, args=[NEWS_STRATEGY], id="news_sentiment")

    # 일일 결산: 평일 15:35
    scheduler.add_job(
        run_daily_summary,
        "cron",
        day_of_week="mon-fri",
        hour=15,
        minute=35,
        id="daily_summary",
    )

    # 섹터 뉴스 분석: 10분마다
    scheduler.add_job(
        run_sector_crawl,
        "interval",
        minutes=10,
        id="sector_crawl",
    )

    # 섹터 예측 검증: 평일 15:40
    scheduler.add_job(
        run_eod_sector_validation,
        "cron",
        day_of_week="mon-fri",
        hour=15,
        minute=40,
        id="sector_validation",
    )

    return scheduler


# ── DB 헬퍼 ──────────────────────────────────────────────────────────────────

async def _has_position(ticker: str) -> bool:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Position).where(Position.ticker == ticker)
        )
        return result.scalar_one_or_none() is not None


async def _get_position_qty(ticker: str) -> int:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Position).where(Position.ticker == ticker)
        )
        pos = result.scalar_one_or_none()
        return pos.quantity if pos else 0


async def _get_position_avg_price(ticker: str) -> float:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Position).where(Position.ticker == ticker)
        )
        pos = result.scalar_one_or_none()
        return pos.avg_price if pos else 0.0


async def _check_stop_take(current_price: int, avg_price: float) -> tuple[bool, str]:
    """손절/익절 조건 체크. avg_price=0이면 스킵."""
    if avg_price <= 0:
        return False, ""
    profit_rate = (current_price - avg_price) / avg_price
    if profit_rate <= settings.stop_loss_pct:
        return True, f"손절 {profit_rate * 100:.1f}%"
    if profit_rate >= settings.take_profit_pct:
        return True, f"익절 {profit_rate * 100:.1f}%"
    return False, ""


async def _can_buy_more() -> bool:
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(Position))
        positions = result.scalars().all()
        return len(positions) < settings.max_positions


async def _record_trade(
    ticker: str, name: str, side: str, qty: int, price: int, strategy: str, reason: str,
    order_id: str = "",
) -> None:
    async with AsyncSessionLocal() as session:
        session.add(TradeLog(
            ticker=ticker,
            name=name,
            side=side,
            quantity=qty,
            price=float(price),
            amount=float(qty * price),
            strategy=strategy,
            reason=reason,
            order_id=order_id,
        ))
        await session.commit()


async def _upsert_position(
    ticker: str, name: str, qty: int, price: int, strategy: str
) -> None:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Position).where(Position.ticker == ticker)
        )
        pos = result.scalar_one_or_none()
        if pos:
            total_qty = pos.quantity + qty
            pos.avg_price = (pos.avg_price * pos.quantity + price * qty) / total_qty
            pos.quantity = total_qty
        else:
            session.add(Position(
                ticker=ticker,
                name=name,
                quantity=qty,
                avg_price=float(price),
                strategy=strategy,
            ))
        await session.commit()


async def _remove_position(ticker: str) -> None:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Position).where(Position.ticker == ticker)
        )
        pos = result.scalar_one_or_none()
        if pos:
            await session.delete(pos)
            await session.commit()
