import logging

from telegram import Bot
from telegram.error import TelegramError

from config import settings

logger = logging.getLogger(__name__)

_bot: Bot | None = None


def _get_bot() -> Bot | None:
    global _bot
    if not settings.telegram_bot_token:
        return None
    if _bot is None:
        _bot = Bot(token=settings.telegram_bot_token)
    return _bot


async def send(message: str) -> None:
    bot = _get_bot()
    if not bot or not settings.telegram_chat_id:
        logger.debug(f"[Telegram] 알림 비활성화: {message}")
        return
    try:
        await bot.send_message(chat_id=settings.telegram_chat_id, text=message)
    except TelegramError as e:
        logger.error(f"[Telegram] 전송 실패: {e}")


async def notify_buy(ticker: str, name: str, quantity: int, price: int, reason: str) -> None:
    msg = (
        f"✅ 매수 체결\n"
        f"종목: {name} ({ticker})\n"
        f"수량: {quantity}주 @ {price:,}원\n"
        f"금액: {quantity * price:,}원\n"
        f"이유: {reason}"
    )
    await send(msg)


async def notify_sell(ticker: str, name: str, quantity: int, price: int, reason: str) -> None:
    msg = (
        f"🔴 매도 체결\n"
        f"종목: {name} ({ticker})\n"
        f"수량: {quantity}주 @ {price:,}원\n"
        f"금액: {quantity * price:,}원\n"
        f"이유: {reason}"
    )
    await send(msg)


async def notify_error(context: str, error: str) -> None:
    msg = f"⚠️ 오류 발생\n위치: {context}\n내용: {error}"
    await send(msg)


async def notify_daily_summary(total_profit: float, trade_count: int) -> None:
    emoji = "📈" if total_profit >= 0 else "📉"
    msg = (
        f"{emoji} 일일 결산\n"
        f"총 손익: {total_profit:+,.0f}원\n"
        f"매매 횟수: {trade_count}회"
    )
    await send(msg)
