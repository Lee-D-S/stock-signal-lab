"""백테스트용 OHLCV 로더 — discovery/data_loader.py 캐시 공유."""

import asyncio
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from discovery.data_loader import get_ohlcv_range  # noqa: E402

_API_DELAY = 0.35


async def load_universe_ohlcv(
    tickers: list[str],
    start: str,
    end: str,
    force_refresh: bool = False,
) -> dict[str, pd.DataFrame]:
    """종목 리스트의 OHLCV를 start~end 범위로 로드 (로컬 캐시 우선).

    Returns:
        {ticker: DataFrame(date, open, high, low, close, volume)} — 빈 DataFrame 제외됨
    """
    result: dict[str, pd.DataFrame] = {}
    total = len(tickers)

    for i, ticker in enumerate(tickers, 1):
        df = await get_ohlcv_range(ticker, start, end, force_refresh=force_refresh)
        if not df.empty:
            result[ticker] = df.reset_index(drop=True)
        if i % 50 == 0:
            print(f"[data_loader] {i}/{total} 로드 완료 ({len(result)}개 유효)")
        if i < total:
            await asyncio.sleep(_API_DELAY)

    print(f"[data_loader] 완료: {len(result)}/{total}개 종목 OHLCV 로드")
    return result
