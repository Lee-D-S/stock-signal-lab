"""업종별 등락률 조회 스크립트"""

import asyncio
import sys

sys.path.insert(0, ".")

from dotenv import load_dotenv
load_dotenv()

from core.market_data import get_sector_changes


async def main():
    for market, label in [("K", "코스피"), ("Q", "코스닥")]:
        print(f"\n{'=' * 55}")
        print(f"  {label} 업종별 등락률")
        print(f"{'=' * 55}")
        print(f"{'업종명':<18} {'등락률':>7}  {'현재지수':>10}  {'상승':>4} {'하락':>4} {'보합':>4}")
        print(f"{'-' * 55}")

        sectors = await get_sector_changes(market=market)
        sectors.sort(key=lambda x: x["change_rate"], reverse=True)

        for s in sectors:
            sign = "▲" if s["change_rate"] > 0 else ("▼" if s["change_rate"] < 0 else " ")
            print(
                f"{s['name']:<18} {sign}{abs(s['change_rate']):>5.2f}%"
                f"  {s['current']:>10.2f}"
                f"  {s['up_count']:>4} {s['down_count']:>4} {s['flat_count']:>4}"
            )


if __name__ == "__main__":
    asyncio.run(main())
