"""Investor flow loader and point-in-time feature join for discovery records."""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(ROOT))

from core.api.client import get_marketdata  # noqa: E402
from .data_loader import _API_DELAY  # noqa: E402


CACHE_DIR = ROOT / "data" / "investor_flow_cache"
_MAX_CHUNKS = 80
logger = logging.getLogger(__name__)


def _cache_paths(ticker: str) -> tuple[Path, Path]:
    return CACHE_DIR / f"{ticker}.parquet", CACHE_DIR / f"{ticker}.pkl"


def _to_number(value: Any) -> float | None:
    if value is None:
        return None
    cleaned = str(value).replace(",", "").strip()
    if cleaned == "":
        return None
    return pd.to_numeric(cleaned, errors="coerce")


def _read_cached_frame(parquet_path: Path, pickle_path: Path) -> pd.DataFrame | None:
    if parquet_path.exists():
        try:
            return pd.read_parquet(parquet_path)
        except (OSError, ValueError, ImportError) as exc:
            logger.warning("failed to read investor flow parquet cache %s: %s", parquet_path, exc)
    if pickle_path.exists():
        try:
            return pd.read_pickle(pickle_path)
        except (OSError, ValueError, ImportError, EOFError) as exc:
            logger.warning("failed to read investor flow pickle cache %s: %s", pickle_path, exc)
    return None


def _write_cached_frame(df: pd.DataFrame, parquet_path: Path, pickle_path: Path) -> None:
    try:
        df.to_parquet(parquet_path, index=False)
    except (OSError, ValueError, ImportError) as exc:
        logger.warning("failed to write investor flow parquet cache %s: %s", parquet_path, exc)
        df.to_pickle(pickle_path)


def _parse_investor_rows(rows: list[dict[str, Any]]) -> pd.DataFrame:
    records = []
    for row in rows:
        date = row.get("stck_bsop_date") or row.get("bsop_date") or row.get("trad_dt")
        if not date:
            continue
        records.append({
            "date": pd.to_datetime(str(date), format="%Y%m%d", errors="coerce"),
            "foreign_qty": _to_number(row.get("frgn_ntby_qty")),
            "institution_qty": _to_number(row.get("orgn_ntby_qty") or row.get("inst_ntby_qty")),
            "individual_qty": _to_number(row.get("prsn_ntby_qty") or row.get("indv_ntby_qty")),
        })

    df = pd.DataFrame(records)
    if df.empty:
        return pd.DataFrame(columns=["date", "foreign_qty", "institution_qty", "individual_qty"])

    return (
        df.dropna(subset=["date"])
        .drop_duplicates("date")
        .sort_values("date")
        .reset_index(drop=True)
    )


async def _fetch_investor_chunk(ticker: str, end_yyyymmdd: str) -> pd.DataFrame:
    data = await get_marketdata(
        "/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily",
        params={
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": ticker,
            "FID_INPUT_DATE_1": end_yyyymmdd,
            "FID_ORG_ADJ_PRC": "",
            "FID_ETC_CLS_CODE": "",
        },
        tr_id="FHPTJ04160001",
    )
    rows = data.get("output2") or data.get("output1") or data.get("output") or []
    return _parse_investor_rows([rows] if isinstance(rows, dict) else rows)


async def get_investor_flow_range(
    ticker: str,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Return daily investor net-buy rows for a ticker, oldest first."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path, pickle_path = _cache_paths(ticker)

    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)

    cached = None if force_refresh else _read_cached_frame(parquet_path, pickle_path)
    if cached is not None and not cached.empty:
        cached["date"] = pd.to_datetime(cached["date"])
        if cached["date"].min() <= start_ts and cached["date"].max() >= end_ts:
            mask = (cached["date"] >= start_ts) & (cached["date"] <= end_ts)
            return cached[mask].reset_index(drop=True)

    frames: list[pd.DataFrame] = []
    cursor = end_ts
    for _ in range(_MAX_CHUNKS):
        try:
            chunk = await _fetch_investor_chunk(ticker, cursor.strftime("%Y%m%d"))
        except RuntimeError as exc:
            logger.warning("investor flow API failed for %s at %s: %s", ticker, cursor.date(), exc)
            break
        if chunk.empty:
            break
        frames.append(chunk)
        min_date = chunk["date"].min()
        if min_date <= start_ts:
            break
        cursor = min_date - pd.Timedelta(days=1)
        await asyncio.sleep(_API_DELAY)

    if not frames:
        return pd.DataFrame(columns=["date", "foreign_qty", "institution_qty", "individual_qty"])

    fetched = (
        pd.concat(frames)
        .drop_duplicates("date")
        .sort_values("date")
        .reset_index(drop=True)
    )

    if cached is not None and not cached.empty:
        fetched = (
            pd.concat([cached, fetched])
            .drop_duplicates("date")
            .sort_values("date")
            .reset_index(drop=True)
        )
    _write_cached_frame(fetched, parquet_path, pickle_path)

    mask = (fetched["date"] >= start_ts) & (fetched["date"] <= end_ts)
    return fetched[mask].reset_index(drop=True)


def add_investor_flow_features(records: pd.DataFrame, investor: pd.DataFrame) -> pd.DataFrame:
    """Add foreign net-buy streak and strength features to one ticker's records."""
    if records.empty:
        return records

    out = records.copy()
    out["date"] = pd.to_datetime(out["date"])

    if investor.empty:
        for col in (
            "foreign_qty",
            "foreign_net_buy_streak",
            "foreign_net_buy_2d_qty",
            "foreign_net_buy_3d_qty",
            "foreign_net_buy_2d_all",
            "foreign_net_buy_3d_all",
            "foreign_net_buy_today_volume_ratio",
        ):
            out[col] = pd.NA
        return out

    flow = investor.copy()
    flow["date"] = pd.to_datetime(flow["date"])
    flow = flow.drop_duplicates("date").sort_values("date").reset_index(drop=True)
    flow["foreign_qty"] = pd.to_numeric(flow["foreign_qty"], errors="coerce")

    is_buy = flow["foreign_qty"] > 0
    streak_group = (~is_buy).cumsum()
    flow["foreign_net_buy_streak"] = is_buy.astype("int64").groupby(streak_group).cumsum()
    flow["foreign_net_buy_2d_qty"] = flow["foreign_qty"].rolling(2, min_periods=2).sum()
    flow["foreign_net_buy_3d_qty"] = flow["foreign_qty"].rolling(3, min_periods=3).sum()
    flow["foreign_net_buy_2d_all"] = (is_buy.rolling(2, min_periods=2).sum() == 2).astype("float64")
    flow["foreign_net_buy_3d_all"] = (is_buy.rolling(3, min_periods=3).sum() == 3).astype("float64")

    keep_cols = [
        "date",
        "foreign_qty",
        "institution_qty",
        "individual_qty",
        "foreign_net_buy_streak",
        "foreign_net_buy_2d_qty",
        "foreign_net_buy_3d_qty",
        "foreign_net_buy_2d_all",
        "foreign_net_buy_3d_all",
    ]
    out = out.merge(flow[keep_cols], on="date", how="left")

    if "volume" in out.columns:
        volume = pd.to_numeric(out["volume"], errors="coerce")
        out["foreign_net_buy_today_volume_ratio"] = pd.NA
        valid_volume = volume > 0
        out.loc[valid_volume, "foreign_net_buy_today_volume_ratio"] = (
            out.loc[valid_volume, "foreign_qty"] / volume.loc[valid_volume]
        )
    else:
        out["foreign_net_buy_today_volume_ratio"] = pd.NA

    return out


async def enrich_records_with_investor_flow(
    records: pd.DataFrame,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Join investor flow features to records by ticker/date without look-ahead."""
    if records.empty or "ticker" not in records.columns or "date" not in records.columns:
        return records

    base = records.copy()
    base["date"] = pd.to_datetime(base["date"])

    enriched: list[pd.DataFrame] = []
    tickers = list(base["ticker"].astype(str).str.zfill(6).unique())
    for idx, ticker in enumerate(tickers, 1):
        part = base[base["ticker"].astype(str).str.zfill(6) == ticker]
        start = part["date"].min() - pd.Timedelta(days=30)
        end = part["date"].max()
        try:
            investor = await get_investor_flow_range(
                ticker,
                start=start,
                end=end,
                force_refresh=force_refresh,
            )
        except RuntimeError as exc:
            logger.warning("skip investor flow join for %s: %s", ticker, exc)
            investor = pd.DataFrame(columns=["date", "foreign_qty", "institution_qty", "individual_qty"])
        enriched.append(add_investor_flow_features(part, investor))

        if idx % 20 == 0 or idx == len(tickers):
            print(f"[investor-flow] {idx}/{len(tickers)}개 종목 조인 완료")
        await asyncio.sleep(_API_DELAY)

    return pd.concat(enriched).sort_index()
