"""OHLCV 데이터 로더 — 날짜 범위 지정 + 로컬 캐시 (Parquet).

KIS API는 1회 호출당 약 400일 반환 한계가 있으므로
요청 범위를 380일 단위 청크로 나눠 순차 호출한 뒤 합산한다.
캐시가 있으면 API를 호출하지 않고 캐시를 반환한다.
"""

import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))

from core.api.client import get_marketdata

CACHE_DIR = ROOT / "data" / "ohlcv_cache"
_CHUNK_DAYS = 380
_API_DELAY  = 0.35


def _cache_paths(ticker: str) -> tuple[Path, Path]:
    return CACHE_DIR / f"{ticker}.parquet", CACHE_DIR / f"{ticker}.pkl"


def _read_cached_frame(parquet_path: Path, pickle_path: Path) -> pd.DataFrame | None:
    if parquet_path.exists():
        try:
            return pd.read_parquet(parquet_path)
        except Exception:
            pass
    if pickle_path.exists():
        try:
            return pd.read_pickle(pickle_path)
        except Exception:
            pass
    return None


def _write_cached_frame(df: pd.DataFrame, parquet_path: Path, pickle_path: Path) -> None:
    try:
        df.to_parquet(parquet_path, index=False)
    except Exception:
        df.to_pickle(pickle_path)


async def get_ohlcv_range(
    ticker: str,
    start: str,
    end: str,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """날짜 범위 OHLCV 조회 (로컬 캐시 우선).

    Args:
        ticker: 종목 코드 (예: "005930")
        start:  시작일 "YYYY-MM-DD"
        end:    종료일 "YYYY-MM-DD"
        force_refresh: True 이면 캐시 무시하고 API 재조회

    Returns:
        DataFrame (columns: date, open, high, low, close, volume) — 오래된 순 정렬
        빈 DataFrame 반환 시 해당 종목 건너뜀
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path, pickle_path = _cache_paths(ticker)

    start_ts = pd.Timestamp(start)
    end_ts   = pd.Timestamp(end)

    if not force_refresh:
        cached = _read_cached_frame(parquet_path, pickle_path)
    else:
        cached = None
    if cached is not None:
        if (
            not cached.empty
            and cached["date"].min() <= start_ts
            and cached["date"].max() >= end_ts
        ):
            mask = (cached["date"] >= start_ts) & (cached["date"] <= end_ts)
            return cached[mask].reset_index(drop=True)

    # 캐시 미스 → API 청크 호출
    chunks: list[pd.DataFrame] = []
    cur = start_ts
    first = True
    while cur <= end_ts:
        chunk_end = min(cur + timedelta(days=_CHUNK_DAYS), end_ts)
        if not first:
            await asyncio.sleep(_API_DELAY)
        first = False
        chunk = await _fetch_chunk(ticker, cur.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d"))
        if not chunk.empty:
            chunks.append(chunk)
        cur = chunk_end + timedelta(days=1)

    if not chunks:
        return pd.DataFrame()

    df = (
        pd.concat(chunks)
        .drop_duplicates("date")
        .sort_values("date")
        .reset_index(drop=True)
    )

    # 기존 캐시와 병합하여 저장
    if not force_refresh:
        existing = _read_cached_frame(parquet_path, pickle_path)
    else:
        existing = None
    if existing is not None:
        df = (
            pd.concat([existing, df])
            .drop_duplicates("date")
            .sort_values("date")
            .reset_index(drop=True)
        )
    _write_cached_frame(df, parquet_path, pickle_path)

    mask = (df["date"] >= start_ts) & (df["date"] <= end_ts)
    return df[mask].reset_index(drop=True)


async def _fetch_chunk(ticker: str, date_from: str, date_to: str) -> pd.DataFrame:
    try:
        data = await get_marketdata(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD":         ticker,
                "FID_INPUT_DATE_1":       date_from,
                "FID_INPUT_DATE_2":       date_to,
                "FID_PERIOD_DIV_CODE":    "D",
                "FID_ORG_ADJ_PRC":        "0",
            },
            tr_id="FHKST03010100",
        )
        rows = data.get("output2", [])
        if not rows:
            return pd.DataFrame()

        records = [
            {
                "date":   r.get("stck_bsop_date", ""),
                "open":   float(r.get("stck_oprc") or 0),
                "high":   float(r.get("stck_hgpr") or 0),
                "low":    float(r.get("stck_lwpr") or 0),
                "close":  float(r.get("stck_clpr") or 0),
                "volume": float(r.get("acml_vol")  or 0),
            }
            for r in rows
            if r.get("stck_clpr")
        ]
        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records)
        df["date"] = pd.to_datetime(df["date"])
        return df.sort_values("date").reset_index(drop=True)
    except Exception:
        return pd.DataFrame()
