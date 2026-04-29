"""거래량 상위 Top10 종목의 PER 조회"""

import asyncio
import sys

sys.path.insert(0, ".")

from dotenv import load_dotenv

load_dotenv()

from core.market_data import get_current_price, get_volume_rank


async def main():
    print("거래량 상위 Top10 종목 PER 조회 중...\n")

    top10 = await get_volume_rank(top_n=10)

    results = []
    for stock in top10:
        info = await get_current_price(stock["ticker"])
        results.append({
            "순위": stock["rank"],
            "종목코드": stock["ticker"],
            "종목명": stock["name"],
            "현재가": f"{stock['price']:,}",
            "거래량": f"{stock['volume']:,}",
            "등락률": f"{stock['change_rate']:+.2f}%",
            "PER": info["per"] if info["per"] else "N/A",
            "PBR": info["pbr"] if info["pbr"] else "N/A",
            "EPS": info["eps"] if info["eps"] else "N/A",
        })

    header = f"{'순위':>4}  {'종목코드':>8}  {'종목명':^12}  {'현재가':>10}  {'거래량':>15}  {'등락률':>8}  {'PER':>8}  {'PBR':>6}  {'EPS':>10}"
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['순위']:>4}  {r['종목코드']:>8}  {r['종목명']:^12}  "
            f"{r['현재가']:>10}  {r['거래량']:>15}  {r['등락률']:>8}  "
            f"{r['PER']:>8}  {r['PBR']:>6}  {r['EPS']:>10}"
        )


if __name__ == "__main__":
    asyncio.run(main())
