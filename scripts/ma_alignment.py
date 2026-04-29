"""이동평균선 정배열 종목 스크리너

5일 > 20일 > 60일 > 120일 이평선이 정배열인 종목 탐색.

Usage:
    python scripts/ma_alignment.py               # 거래량 상위 200개
    python scripts/ma_alignment.py --top 500
"""

import argparse
import asyncio
import sys

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
    """시가총액 상위 — 코스피+코스닥 전체, 페이지네이션으로 전 종목 조회"""
    result = []
    tr_cont = ""
    while True:
        data = await get_marketdata(
            "/uapi/domestic-stock/v1/ranking/market-cap",
            params={
                "fid_cond_mrkt_div_code": "J",
                "fid_cond_scr_div_code": "20174",
                "fid_div_cls_code": "0",   # 전체(보통주+우선주)
                "fid_input_iscd": "0000",  # 전체(코스피+코스닥)
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
        tr_cont = "N"  # 다음 페이지 요청 시 "N" 전송 (응답이 "M"이어도)
        await asyncio.sleep(0.2)
    return result


async def get_stock_universe(by: str) -> list[dict]:
    """코스피 + 코스닥 종목 합산 조회"""
    if by == "marcap":
        return await _fetch_market_cap()
    else:  # volume
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


async def get_closes(ticker: str) -> list[float]:
    """일봉 종가 130개 조회 (오래된 순). 120일 MA 계산에 충분한 여유분."""
    try:
        data = await get_marketdata(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": ticker,
                "FID_INPUT_DATE_1": "",
                "FID_INPUT_DATE_2": "",
                "FID_PERIOD_DIV_CODE": "D",
            },
            tr_id="FHKST03010100",
        )
        rows = data.get("output2", [])
        closes = [float(r["stck_clpr"]) for r in rows if r.get("stck_clpr")]
        closes.reverse()  # API는 최신순 → 오래된 순으로 변환
        return closes
    except Exception:
        return []


def sma(closes: list[float], period: int) -> float | None:
    if len(closes) < period:
        return None
    return sum(closes[-period:]) / period


def check_alignment(closes: list[float]) -> tuple[bool, dict]:
    """5 > 20 > 60 > 120일 정배열 여부와 각 MA 값 반환"""
    ma = {
        5:   sma(closes, 5),
        20:  sma(closes, 20),
        60:  sma(closes, 60),
        120: sma(closes, 120),
    }
    if any(v is None for v in ma.values()):
        return False, ma
    aligned = ma[5] > ma[20] > ma[60] > ma[120]
    return aligned, ma


async def main():
    parser = argparse.ArgumentParser(description="이동평균선 정배열 종목 스크리너")
    parser.add_argument("--by", choices=["volume", "marcap"], default="volume",
                        help="종목 선정 기준: volume=거래량순위, marcap=시가총액순위 (기본: volume)")
    parser.add_argument("--from", dest="rank_from", type=int, default=1,
                        help="순위 시작 (기본: 1)")
    parser.add_argument("--to", dest="rank_to", type=int, default=9999,
                        help="순위 끝 (기본: 전체)")
    args = parser.parse_args()

    if args.rank_from < 1 or args.rank_to < args.rank_from:
        print("오류: --from 은 1 이상, --to 는 --from 이상이어야 합니다.")
        return

    label_by = "시가총액순" if args.by == "marcap" else "거래량순"
    print("이동평균선 정배열 스크리닝 (5일 > 20일 > 60일 > 120일)")
    print(f"종목 선정 기준: {label_by}\n")

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
        closes = await get_closes(stock["ticker"])
        await asyncio.sleep(API_DELAY)

        if len(closes) < 120:
            continue

        aligned, ma = check_alignment(closes)
        if aligned:
            results.append({**stock, **{f"ma{k}": v for k, v in ma.items()}})

        if i % 30 == 0:
            print(f"  {i}/{len(stocks)} 처리 중... (통과: {len(results)}개)")

    results.sort(key=lambda x: float(x["change_rate"]), reverse=True)

    print()
    if not results:
        print("정배열 종목 없음")
        return

    print(f"정배열 종목: {len(results)}개\n")
    header = (
        f"{'종목코드':>8}  {'종목명':^14}  {'현재가':>9}  {'등락률':>7}  "
        f"{'MA5':>9}  {'MA20':>9}  {'MA60':>9}  {'MA120':>9}"
    )
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['ticker']:>8}  {r['name']:^14}  {r['price']:>9,}  "
            f"{float(r['change_rate']):>+6.2f}%  "
            f"{r['ma5']:>9,.1f}  {r['ma20']:>9,.1f}  "
            f"{r['ma60']:>9,.1f}  {r['ma120']:>9,.1f}"
        )


if __name__ == "__main__":
    asyncio.run(main())
