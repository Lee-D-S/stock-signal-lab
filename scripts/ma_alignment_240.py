"""이동평균선 장기 정배열 종목 스크리너

60일 > 120일 > 240일 이평선이 정배열인 종목 탐색.

Usage:
    python scripts/ma_alignment_240.py                          # 거래량순 전체
    python scripts/ma_alignment_240.py --by marcap              # 시가총액순
    python scripts/ma_alignment_240.py --sort amount            # 거래 대금 내림차순
    python scripts/ma_alignment_240.py --min-amount 100000000000  # 거래 대금 1000억 이상
    python scripts/ma_alignment_240.py --from 1 --to 300
"""

import argparse
import asyncio
import sys
from datetime import datetime, timedelta

sys.path.insert(0, ".")

from dotenv import load_dotenv

load_dotenv()

from core.api.client import get_marketdata

API_DELAY = 0.35


def _parse_rows(rows: list[dict]) -> list[dict]:
    return [
        {
            "ticker": r.get("mksc_shrn_iscd") or r.get("stck_shrn_iscd", ""),
            "name": r.get("hts_kor_isnm", ""),
            "price": int(r.get("stck_prpr", 0) or 0),
            "change_rate": r.get("prdy_ctrt", "0"),
        }
        for r in rows
        if r.get("mksc_shrn_iscd") or r.get("stck_shrn_iscd")
    ]


async def _fetch_volume_rank(market_code: str) -> list[dict]:
    data = await get_marketdata(
        "/uapi/domestic-stock/v1/quotations/volume-rank",
        params={
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_COND_SCR_DIV_CODE": "20171",
            "FID_INPUT_ISCD": market_code,
            "FID_DIV_CLS_CODE": "0",
            "FID_BLNG_CLS_CODE": "0",
            "FID_TRGT_CLS_CODE": "111111111",
            "FID_TRGT_EXLS_CLS_CODE": "000000",
            "FID_INPUT_PRICE_1": "",
            "FID_INPUT_PRICE_2": "",
            "FID_VOL_CNT": "",
            "FID_INPUT_DATE_1": "",
        },
        tr_id="FHPST01710000",
    )
    return _parse_rows(data.get("output", []))


async def _fetch_market_cap() -> list[dict]:
    result = []
    tr_cont = ""
    while True:
        data = await get_marketdata(
            "/uapi/domestic-stock/v1/ranking/market-cap",
            params={
                "fid_cond_mrkt_div_code": "J",
                "fid_cond_scr_div_code": "20174",
                "fid_div_cls_code": "0",
                "fid_input_iscd": "0000",
                "fid_trgt_cls_code": "0",
                "fid_trgt_exls_cls_code": "0",
                "fid_input_price_1": "",
                "fid_input_price_2": "",
                "fid_vol_cnt": "",
            },
            tr_id="FHPST01740000",
            tr_cont=tr_cont,
        )
        result.extend(_parse_rows(data.get("output", [])))
        if data.get("__tr_cont__", "") != "M":
            break
        tr_cont = "N"
        await asyncio.sleep(0.2)
    return result


async def get_stock_universe(by: str) -> list[dict]:
    if by == "marcap":
        return await _fetch_market_cap()
    else:
        kospi, kosdaq = await asyncio.gather(
            _fetch_volume_rank("0001"),
            _fetch_volume_rank("1001"),
        )
        seen = set()
        result = []
        for stock in kospi + kosdaq:
            if stock["ticker"] and stock["ticker"] not in seen:
                seen.add(stock["ticker"])
                result.append(stock)
        return result


async def get_ohlcv(ticker: str) -> tuple[list[float], int]:
    """일봉 종가 + 최근 거래 대금 조회.

    Returns:
        (closes, trade_amount) — closes는 오래된 순, trade_amount는 가장 최근 거래일 기준.
    """
    date_to = datetime.today().strftime("%Y%m%d")
    date_from = (datetime.today() - timedelta(days=400)).strftime("%Y%m%d")
    try:
        data = await get_marketdata(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": ticker,
                "FID_INPUT_DATE_1": date_from,
                "FID_INPUT_DATE_2": date_to,
                "FID_PERIOD_DIV_CODE": "D",
            },
            tr_id="FHKST03010100",
        )
        rows = data.get("output2", [])
        closes = [float(r["stck_clpr"]) for r in rows if r.get("stck_clpr")]
        # rows[0]이 가장 최근 거래일
        trade_amount = int(rows[0].get("acml_tr_pbmn", 0) or 0) if rows else 0
        closes.reverse()  # API는 최신순 → 오래된 순으로 변환
        return closes, trade_amount
    except Exception:
        return [], 0


def sma(closes: list[float], period: int) -> float | None:
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period


def check_alignment(closes: list[float]) -> tuple[bool, dict]:
    """60 > 120 > 240일 정배열 여부와 각 MA 값 반환"""
    ma = {
        60:  sma(closes, 60),
        120: sma(closes, 120),
        240: sma(closes, 240),
    }
    if any(v is None for v in ma.values()):
        return False, ma
    aligned = ma[60] > ma[120] > ma[240]
    return aligned, ma


def _fmt_amount(won: int) -> str:
    """거래 대금을 억 단위로 포맷"""
    eok = won // 100_000_000
    if eok >= 10_000:
        return f"{eok / 10_000:.1f}조"
    return f"{eok:,}억"


async def main():
    parser = argparse.ArgumentParser(description="이동평균선 장기 정배열 종목 스크리너")
    parser.add_argument("--by", choices=["volume", "marcap"], default="volume",
                        help="종목 선정 기준: volume=거래량순위, marcap=시가총액순위 (기본: volume)")
    parser.add_argument("--from", dest="rank_from", type=int, default=1,
                        help="순위 시작 (기본: 1)")
    parser.add_argument("--to", dest="rank_to", type=int, default=9999,
                        help="순위 끝 (기본: 전체)")
    parser.add_argument("--sort", choices=["change_rate", "amount"], default="change_rate",
                        help="결과 정렬 기준: change_rate=등락률, amount=거래 대금 (기본: change_rate)")
    parser.add_argument("--min-amount", dest="min_amount", type=int, default=0,
                        help="최소 거래 대금 필터 (원 단위, 예: 100000000000 = 1000억)")
    args = parser.parse_args()

    if args.rank_from < 1 or args.rank_to < args.rank_from:
        print("오류: --from 은 1 이상, --to 는 --from 이상이어야 합니다.")
        return

    label_by = "시가총액순" if args.by == "marcap" else "거래량순"
    label_sort = "거래 대금" if args.sort == "amount" else "등락률"
    print("이동평균선 장기 정배열 스크리닝 (60일 > 120일 > 240일)")
    print(f"종목 선정 기준: {label_by}  |  정렬: {label_sort}", end="")
    if args.min_amount:
        print(f"  |  최소 거래 대금: {_fmt_amount(args.min_amount)}", end="")
    print("\n")

    print("종목 조회 중...")
    all_stocks = await get_stock_universe(args.by)
    if not all_stocks:
        print("종목 데이터 없음")
        return

    total = len(all_stocks)
    if args.rank_from > total:
        print(f"오류: 조회된 종목이 {total}개뿐입니다. --from 을 {total} 이하로 설정하세요.")
        return

    stocks = all_stocks[args.rank_from - 1 : args.rank_to]
    actual_to = min(args.rank_to, total)
    print(f"총 {total}개 조회 | {args.rank_from}~{actual_to}위 ({len(stocks)}개) 분석 중...\n")

    results = []
    for i, stock in enumerate(stocks, 1):
        closes, trade_amount = await get_ohlcv(stock["ticker"])
        await asyncio.sleep(API_DELAY)

        if len(closes) < 240:
            continue

        if args.min_amount and trade_amount < args.min_amount:
            continue

        aligned, ma = check_alignment(closes)
        if aligned:
            results.append({
                **stock,
                **{f"ma{k}": v for k, v in ma.items()},
                "trade_amount": trade_amount,
            })

        if i % 30 == 0:
            print(f"  {i}/{len(stocks)} 처리 중... (통과: {len(results)}개)")

    if args.sort == "amount":
        results.sort(key=lambda x: x["trade_amount"], reverse=True)
    else:
        results.sort(key=lambda x: float(x["change_rate"]), reverse=True)

    print()
    if not results:
        print("정배열 종목 없음")
        return

    print(f"정배열 종목: {len(results)}개\n")
    header = (
        f"{'종목코드':>8}  {'종목명':^14}  {'현재가':>9}  {'등락률':>7}  "
        f"{'거래대금':>9}  {'MA60':>9}  {'MA120':>9}  {'MA240':>9}"
    )
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['ticker']:>8}  {r['name']:^14}  {r['price']:>9,}  "
            f"{float(r['change_rate']):>+6.2f}%  "
            f"{_fmt_amount(r['trade_amount']):>9}  "
            f"{r['ma60']:>9,.1f}  {r['ma120']:>9,.1f}  {r['ma240']:>9,.1f}"
        )


if __name__ == "__main__":
    asyncio.run(main())
