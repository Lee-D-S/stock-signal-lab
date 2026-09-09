
import asyncio
import sys
import os
from pathlib import Path

# Add root to sys.path
ROOT = Path(os.getcwd())
sys.path.insert(0, str(ROOT))

from core.api.client import get_marketdata
from core.market_data import get_current_price

async def get_stock_info(ticker: str) -> dict:
    data = await get_marketdata(
        "/uapi/domestic-stock/v1/quotations/search-stock-info",
        params={"PRDT_TYPE_CD": "300", "PDNO": ticker},
        tr_id="CTPF1002R",
    )
    return data.get("output") or {}

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
    return ""

async def validate():
    ticker = "005930"
    print(f"--- Real-time Data for {ticker} ---")
    
    price_info = await get_current_price(ticker)
    print(f"Price: {price_info.get('price')}")
    print(f"PER: {price_info.get('per')}")
    print(f"PBR: {price_info.get('pbr')}")
    print(f"EPS: {price_info.get('eps')}")
    
    stock_info = await get_stock_info(ticker)
    sector = sector_from_kis_stock_info(stock_info)
    print(f"Sector: {sector}")
    print(f"Full Stock Info: {stock_info}")

if __name__ == "__main__":
    asyncio.run(validate())
