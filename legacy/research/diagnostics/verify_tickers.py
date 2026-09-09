
import asyncio
import sys
import os
import json
from pathlib import Path

# Add root to sys.path
ROOT = Path(os.getcwd())
sys.path.insert(0, str(ROOT))

from core.api.client import get_marketdata
from core.market_data import get_current_price

async def get_stock_info(ticker: str) -> dict:
    try:
        data = await get_marketdata(
            "/uapi/domestic-stock/v1/quotations/search-stock-info",
            params={"PRDT_TYPE_CD": "300", "PDNO": ticker},
            tr_id="CTPF1002R",
        )
        return data.get("output") or {}
    except Exception as e:
        print(f"Error fetching info for {ticker}: {e}")
        return {}

def sector_from_kis_stock_info(stock_info: dict) -> str:
    for key in (
        "idx_bztp_mcls_cd_name",
        "idx_bztp_lcls_cd_name",
        "std_idst_clsf_cd_name",
        "idx_bztp_scls_cd_name",
    ):
        value = str(stock_info.get(key, "") or "").strip()
        if value:
            return value
    return "Unknown"

async def get_real_current_price(ticker: str) -> dict:
    """현재가 조회 (무조건 실전 서버)"""
    data = await get_marketdata(
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
        tr_id="FHKST01010100",
    )
    output = data["output"]
    return {
        "ticker": ticker,
        "price": int(output["stck_prpr"]),
        "name": output.get("hts_kor_isnm", ""),
        "per": output.get("per", ""),
        "pbr": output.get("pbr", ""),
        "eps": output.get("eps", ""),
    }

async def verify_tickers():
    tickers = ["005930", "000660", "035420", "005380", "247540", "042700", "454910"]
    results = {}
    
    for ticker in tickers:
        print(f"Fetching REAL data for {ticker}...")
        price_info = await get_real_current_price(ticker)
        stock_info = await get_stock_info(ticker)
        
        results[ticker] = {
            "name": price_info.get("name"),
            "current_price": price_info.get("price"),
            "per": price_info.get("per"),
            "pbr": price_info.get("pbr"),
            "eps": price_info.get("eps"),
            "sector": sector_from_kis_stock_info(stock_info),
            "full_sector_info": {
                "lcls": stock_info.get("idx_bztp_lcls_cd_name"),
                "mcls": stock_info.get("idx_bztp_mcls_cd_name"),
                "scls": stock_info.get("idx_bztp_scls_cd_name"),
                "std": stock_info.get("std_idst_clsf_cd_name")
            }
        }
        await asyncio.sleep(0.2) # Avoid rate limit
    
    print(json.dumps(results, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    asyncio.run(verify_tickers())
