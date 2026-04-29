from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from core.api.client import get_marketdata  # noqa: E402


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", default="005930")
    parser.add_argument("--start", default="2025-12-31")
    parser.add_argument("--end", default="2026-01-12")
    args = parser.parse_args()

    data = await get_marketdata(
        "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        params={
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": args.ticker,
            "FID_INPUT_DATE_1": args.start.replace("-", ""),
            "FID_INPUT_DATE_2": args.end.replace("-", ""),
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "0",
        },
        tr_id="FHKST03010100",
    )
    rows = data.get("output2") or []
    print(f"rt_cd={data.get('rt_cd')} msg_cd={data.get('msg_cd')} msg1={data.get('msg1')}")
    print(f"rows={len(rows)}")
    print(rows[:3])


if __name__ == "__main__":
    asyncio.run(main())
