from __future__ import annotations

from typing import Any

import pandas as pd

from core.api.client import get_marketdata


_EXCLUDED_NAME_TOKENS = ("ETF", "ETN", "스팩", "우선", "리츠", "인버스", "레버리지")


async def fetch_market_universe() -> pd.DataFrame:
    """Fetch a broad KOSPI/KOSDAQ market-cap universe from KIS read-only API."""
    frames: list[pd.DataFrame] = []
    for market_code, market_name in (("0001", "KOSPI"), ("1001", "KOSDAQ")):
        rows: list[dict[str, Any]] = []
        continuation = ""
        while True:
            data = await get_marketdata(
                "/uapi/domestic-stock/v1/ranking/market-cap",
                params={
                    "fid_cond_mrkt_div_code": "J",
                    "fid_cond_scr_div_code": "20174",
                    "fid_div_cls_code": "0",
                    "fid_input_iscd": market_code,
                    "fid_trgt_cls_code": "0",
                    "fid_trgt_exls_cls_code": "0",
                    "fid_input_price_1": "",
                    "fid_input_price_2": "",
                    "fid_vol_cnt": "",
                },
                tr_id="FHPST01740000",
                tr_cont=continuation,
            )
            rows.extend(data.get("output", []))
            if data.get("__tr_cont__") != "M":
                break
            continuation = "N"
        frames.append(_normalise_rows(rows, market_name))
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).drop_duplicates("ticker").reset_index(drop=True)


def _normalise_rows(rows: list[dict[str, Any]], market: str) -> pd.DataFrame:
    normalised = []
    for row in rows:
        ticker = str(row.get("mksc_shrn_iscd") or row.get("stck_shrn_iscd") or "").strip()
        name = str(row.get("hts_kor_isnm") or "").strip()
        if not ticker or any(token in name.upper() for token in _EXCLUDED_NAME_TOKENS):
            continue
        normalised.append({
            "ticker": ticker,
            "name": name,
            "market": market,
            "security_type": "COMMON",
            "price": pd.to_numeric(row.get("stck_prpr"), errors="coerce"),
            "avg_value_20d": pd.to_numeric(row.get("acml_tr_pbmn") or row.get("avrg_tr_pbmn"), errors="coerce"),
            "history_days": 300,
            "suspended": False,
        })
    return pd.DataFrame(normalised)
