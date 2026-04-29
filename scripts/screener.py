"""멀티 조건 기술적 지표 종목 스크리너

Usage:
    python scripts/screener.py --by marcap --to 300 --ma-align 60,120,240 --sort amount
    python scripts/screener.py --ma-align 5,20,60,120 --rsi-max 50 --macd-positive
    python scripts/screener.py --bb-breakout --min-amount 50000000000
    python scripts/screener.py --obv-rising --vol-above-ma --stoch-max 50
    python scripts/screener.py --fib-support --ma-align 60,120,240
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))  # 프로젝트 루트 (core.* 임포트용)
sys.path.insert(0, str(Path(__file__).parent))         # scripts/ (screener_lib 임포트용)

from dotenv import load_dotenv

load_dotenv()

from screener_lib.dart import fetch_dart_fundamentals
from screener_lib.data import API_DELAY, get_kis_valuation, get_ohlcv
from screener_lib.indicators import add_all_args, all_labels, calc_all, check_all
from screener_lib.indicators.fundamentals import needs_dart
from screener_lib.indicators.valuation import needs_valuation
from screener_lib.output import print_results
from screener_lib.universe import get_stock_universe
from screener_lib.utils import _fmt_amount


async def main() -> None:
    parser = argparse.ArgumentParser(description="멀티 조건 기술적 지표 종목 스크리너")
    parser.add_argument("--by",   choices=["volume", "marcap"], default="volume",
                        help="종목 선정 기준 (기본: volume)")
    parser.add_argument("--from", dest="rank_from", type=int, default=1)
    parser.add_argument("--to",   dest="rank_to",   type=int, default=9999)
    parser.add_argument("--sort", choices=["change_rate", "amount", "rsi", "stoch"],
                        default="change_rate", help="결과 정렬 기준 (기본: change_rate)")
    parser.add_argument("--min-amount", type=int, default=0, metavar="WON",
                        help="최소 거래 대금 (원, 예: 50000000000 = 500억)")
    add_all_args(parser)
    args = parser.parse_args()

    conditions = all_labels(args)
    if args.min_amount:
        conditions.append(f"거래대금≥{_fmt_amount(args.min_amount)}")

    label_by   = "시가총액순" if args.by == "marcap" else "거래량순"
    label_sort = {"change_rate": "등락률", "amount": "거래대금",
                  "rsi": "RSI", "stoch": "스토캐스틱"}[args.sort]

    print("[ 멀티 조건 기술적 지표 스크리너 ]")
    print(f"종목 선정: {label_by}  |  정렬: {label_sort}")
    print(f"조건: {' & '.join(conditions) if conditions else '없음 (전 종목 출력)'}\n")

    print("종목 조회 중...")
    all_stocks = await get_stock_universe(args.by)
    if not all_stocks:
        print("종목 데이터 없음")
        return

    total     = len(all_stocks)
    stocks    = all_stocks[args.rank_from - 1 : args.rank_to]
    actual_to = min(args.rank_to, total)
    print(f"총 {total}개 | {args.rank_from}~{actual_to}위 ({len(stocks)}개) 분석 중...")

    # DART 재무 조건이 있으면 대상 종목 전체를 사전 일괄 조회
    dart_data: dict = {}
    if needs_dart(args):
        tickers = [s["ticker"] for s in stocks]
        print("DART 재무 데이터 일괄 조회 중...")
        dart_data = await fetch_dart_fundamentals(tickers)
        print(f"DART 데이터 수신 완료 ({len(dart_data)}개)\n")
    else:
        print()

    results = []
    for i, stock in enumerate(stocks, 1):
        df, trade_amount = await get_ohlcv(stock["ticker"])
        await asyncio.sleep(API_DELAY)

        if df.empty or len(df) < 20:
            continue

        if args.min_amount and trade_amount < args.min_amount:
            continue

        ind = calc_all(df)
        ind["close"]        = float(df["close"].iloc[-1])
        ind["trade_amount"] = trade_amount
        ind["dart"]         = dart_data.get(stock["ticker"])  # None이면 재무조건 탈락

        if needs_valuation(args):
            ind["valuation"] = await get_kis_valuation(stock["ticker"])
            await asyncio.sleep(API_DELAY)

        if check_all(ind, args):
            results.append({**stock, "trade_amount": trade_amount, "ind": ind})

        if i % 30 == 0:
            print(f"  {i}/{len(stocks)} 처리 중... (통과: {len(results)}개)")

    results.sort(
        key={
            "change_rate": lambda x: float(x["change_rate"]),
            "amount":      lambda x: x["trade_amount"],
            "rsi":         lambda x: x["ind"].get("rsi")     or 0.0,
            "stoch":       lambda x: x["ind"].get("stoch_k") or 0.0,
        }[args.sort],
        reverse=True,
    )

    print()
    print_results(results, args)


if __name__ == "__main__":
    asyncio.run(main())
