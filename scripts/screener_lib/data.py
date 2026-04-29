from datetime import datetime, timedelta

import pandas as pd

from core.api.client import get_marketdata

API_DELAY = 0.35


async def get_kis_valuation(ticker: str) -> dict | None:
    """KIS API에서 PER, PBR, EPS, BPS 조회 (inquire-price).

    Returns:
        {"per": float|None, "pbr": float|None, "eps": float|None, "bps": float|None}
        또는 조회 실패 시 None
    """
    try:
        data = await get_marketdata(
            "/uapi/domestic-stock/v1/quotations/inquire-price",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": ticker,
            },
            tr_id="FHKST01010100",
        )
        output = data.get("output") or {}

        def _f(key: str) -> float | None:
            v = output.get(key, "")
            try:
                return float(v) if v else None
            except (ValueError, TypeError):
                return None

        return {"per": _f("per"), "pbr": _f("pbr"), "eps": _f("eps"), "bps": _f("bps")}
    except Exception:
        return None


async def get_ohlcv(ticker: str) -> tuple[pd.DataFrame, int]:
    """일봉 OHLCV + 최근 거래 대금 조회 (400 캘린더일, 오래된 순 정렬).

    Returns:
        (df, trade_amount) — df columns: open/high/low/close/volume
    """
    date_to   = datetime.today().strftime("%Y%m%d")
    date_from = (datetime.today() - timedelta(days=400)).strftime("%Y%m%d")
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
            return pd.DataFrame(), 0

        trade_amount = int(rows[0].get("acml_tr_pbmn", 0) or 0)  # rows[0] = 최신일

        df = pd.DataFrame([
            {
                "open":   float(r.get("stck_oprc") or 0),
                "high":   float(r.get("stck_hgpr") or 0),
                "low":    float(r.get("stck_lwpr") or 0),
                "close":  float(r.get("stck_clpr") or 0),
                "volume": float(r.get("acml_vol")  or 0),
            }
            for r in rows
            if r.get("stck_clpr")
        ])
        return df.iloc[::-1].reset_index(drop=True), trade_amount  # 최신순 → 오래된 순
    except Exception:
        return pd.DataFrame(), 0
