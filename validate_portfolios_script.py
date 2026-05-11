
import asyncio
import sys
import os
import json
from pathlib import Path

# Add root to sys.path
ROOT = Path(os.getcwd())
sys.path.insert(0, str(ROOT))

from core.api.client import get_marketdata

async def get_real_data(ticker: str) -> dict:
    try:
        # Get price and valuation
        price_data = await get_marketdata(
            "/uapi/domestic-stock/v1/quotations/inquire-price",
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
            tr_id="FHKST01010100",
        )
        # Get stock info for sector
        info_data = await get_marketdata(
            "/uapi/domestic-stock/v1/quotations/search-stock-info",
            params={"PRDT_TYPE_CD": "300", "PDNO": ticker},
            tr_id="CTPF1002R",
        )
        
        output = price_data["output"]
        info = info_data["output"]
        
        # Extract sector
        sector = ""
        for key in ("idx_bztp_mcls_cd_name", "idx_bztp_lcls_cd_name", "std_idst_clsf_cd_name"):
            val = str(info.get(key, "") or "").strip()
            if val:
                sector = val
                break
                
        return {
            "ticker": ticker,
            "current_price": int(output["stck_prpr"]),
            "per": float(output["per"]) if output["per"] else 0.0,
            "pbr": float(output["pbr"]) if output["pbr"] else 0.0,
            "eps": float(output["eps"]) if output["eps"] else 0.0,
            "sector": sector
        }
    except Exception as e:
        return {"error": str(e)}

async def validate_portfolios():
    portfolio_dir = Path("data/portfolios")
    test_files = [
        "test_portfolio_balanced.json",
        "test_portfolio_cash_heavy.json",
        "test_portfolio_growth.json",
        "test_portfolio_risk_test.json"
    ]
    
    # Unique tickers across all test portfolios
    all_tickers = set()
    portfolios_data = {}
    for filename in test_files:
        path = portfolio_dir / filename
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            portfolios_data[filename] = data
            for pos in data.get("positions", []):
                all_tickers.add(pos["ticker"])
    
    # Fetch real data
    print(f"Fetching real data for {len(all_tickers)} tickers...")
    real_data_map = {}
    for ticker in all_tickers:
        real_data_map[ticker] = await get_real_data(ticker)
        await asyncio.sleep(0.2)
        
    # Compare
    for filename, data in portfolios_data.items():
        print(f"\n--- Validating {filename} ---")
        for pos in data.get("positions", []):
            ticker = pos["ticker"]
            real = real_data_map.get(ticker)
            if "error" in real:
                print(f"[{ticker}] Error: {real['error']}")
                continue
            
            price_match = abs(pos["current_price"] - real["current_price"]) < 1
            per_match = abs(pos["valuation"]["per"] - real["per"]) < 0.02
            pbr_match = abs(pos["valuation"]["pbr"] - real["pbr"]) < 0.02
            
            # Sector comparison might be tricky due to encoding, so we check if it's non-empty at least
            # or if it's the same length/pattern
            sector_match = (pos["sector"] == real["sector"])
            
            print(f"[{ticker} {pos.get('name', '')}]")
            print(f"  Price: {pos['current_price']} vs {real['current_price']} -> {'MATCH' if price_match else 'MISMATCH'}")
            print(f"  PER:   {pos['valuation']['per']} vs {real['per']} -> {'MATCH' if per_match else 'MISMATCH'}")
            print(f"  PBR:   {pos['valuation']['pbr']} vs {real['pbr']} -> {'MATCH' if pbr_match else 'MISMATCH'}")
            # We don't print sector status as string yet because of mojibake
            if not sector_match:
                print(f"  Sector MISMATCH (might be encoding)")
            else:
                print(f"  Sector MATCH")

if __name__ == "__main__":
    asyncio.run(validate_portfolios())
