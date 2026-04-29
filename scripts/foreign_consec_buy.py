"""외국인 N일 이상 연속 순매수/순매도 종목 스크리너

Usage:
    python scripts/foreign_consec_buy.py                      # 순매수 10일 이상
    python scripts/foreign_consec_buy.py --days 20            # 순매수 20일 이상
    python scripts/foreign_consec_buy.py --mode sell          # 순매도 10일 이상
    python scripts/foreign_consec_buy.py --mode sell --days 15
    python scripts/foreign_consec_buy.py --mode both --days 10 # 매수+매도 동시 출력
"""

import argparse
import asyncio
import sys
from datetime import datetime, timedelta

sys.path.insert(0, ".")

from dotenv import load_dotenv

load_dotenv()

from config import settings
from core.api import client
from core.api.client import get_marketdata

API_DELAY = 0.35  # KIS API rate limit 대응 (초)


def last_trading_day() -> str:
    """오늘이 주말이거나 15:40 이전이면 가장 최근 거래일(평일) 반환. YYYYMMDD 형식."""
    now = datetime.now()
    d = now.date()
    # 토요일(5), 일요일(6) 또는 당일 15:40 이전이면 전 거래일로
    if d.weekday() >= 5:
        # 토→금, 일→금
        d -= timedelta(days=d.weekday() - 4)
    elif now.hour < 15 or (now.hour == 15 and now.minute < 40):
        # 장 마감 전 → 전날 (전날도 주말이면 계속 앞으로)
        d -= timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


async def get_foreign_top_stocks(top_n: int = 150, rank_sort: str = "0") -> list[dict]:
    """외국인 순매수/순매도 상위 종목 조회 (foreign_institution_total)

    rank_sort: "0" = 순매수 상위, "1" = 순매도 상위
    """
    data = await get_marketdata(
        "/uapi/domestic-stock/v1/quotations/foreign-institution-total",
        params={
            "FID_COND_MRKT_DIV_CODE": "V",
            "FID_COND_SCR_DIV_CODE": "16449",
            "FID_INPUT_ISCD": "0000",
            "FID_DIV_CLS_CODE": "0",
            "FID_RANK_SORT_CLS_CODE": rank_sort,
            "FID_ETC_CLS_CODE": "1",        # 외국인만
        },
        tr_id="FHPTJ04400000",
    )
    items = data.get("output", [])
    return [
        {
            "ticker": item["mksc_shrn_iscd"],
            "name": item["hts_kor_isnm"],
            "today_ntby_qty": int(item.get("frgn_ntby_qty", 0) or 0),
            "today_ntby_pbmn": int(item.get("frgn_ntby_tr_pbmn", 0) or 0),
            "price": int(item.get("stck_prpr", 0) or 0),
            "change_rate": item.get("prdy_ctrt", "0"),
        }
        for item in items[:top_n]
        if item.get("mksc_shrn_iscd")
    ]


async def get_daily_investor_rows(ticker: str) -> list[dict]:
    """주식현재가 투자자 일별 조회 (inquire_investor) — 실전 서버 경유."""
    try:
        data = await get_marketdata(
            "/uapi/domestic-stock/v1/quotations/inquire-investor",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": ticker,
            },
            tr_id="FHKST01010900",
        )
        return data.get("output", []) or []
    except Exception as e:
        print(f"  [WARN] {ticker} 조회 실패: {e}")
        return []


def count_consecutive(rows: list[dict], mode: str) -> tuple[int, int]:
    """연속 순매수 또는 순매도 일수와 누적 수량 반환.

    mode: "buy"  → frgn_ntby_qty > 0 연속
          "sell" → frgn_ntby_qty < 0 연속
    """
    rows_sorted = sorted(
        rows,
        key=lambda r: r.get("stck_bsop_date", ""),
        reverse=True,
    )

    consec = 0
    cumulative = 0
    for row in rows_sorted:
        qty = int(row.get("frgn_ntby_qty", 0) or 0)
        is_hit = qty > 0 if mode == "buy" else qty < 0
        if is_hit:
            consec += 1
            cumulative += qty
        else:
            break

    return consec, cumulative


async def screen_mode(mode: str, min_days: int, top_n: int, debug: bool = False) -> list[dict]:
    """단일 모드(buy 또는 sell) 스크리닝 수행 후 결과 리스트 반환"""
    rank_sort = "0" if mode == "buy" else "1"
    label = "순매수" if mode == "buy" else "순매도"

    print(f"외국인 {label} 상위 종목 조회 중...")
    top_stocks = await get_foreign_top_stocks(top_n=top_n, rank_sort=rank_sort)
    if not top_stocks:
        print("데이터 없음. 장 중(09:30 이후)에 실행하거나 당일 데이터 확인 필요.")
        return []
    print(f"{len(top_stocks)}개 종목 확인. 일별 데이터 분석 중...\n")

    results = []
    debug_done = False
    for i, stock in enumerate(top_stocks, 1):
        ticker = stock["ticker"]
        rows = await get_daily_investor_rows(ticker)
        await asyncio.sleep(API_DELAY)

        if not rows:
            continue

        # 첫 번째 성공 종목의 원본 데이터 출력
        if debug and not debug_done:
            print(f"\n[DEBUG] {ticker} ({stock['name']}) 원본 데이터 (최근 5행):")
            rows_sorted = sorted(rows, key=lambda r: r.get("stck_bsop_date", ""), reverse=True)
            for row in rows_sorted[:5]:
                date = row.get("stck_bsop_date", "?")
                qty = row.get("frgn_ntby_qty", "N/A")
                print(f"  날짜={date}  frgn_ntby_qty={qty}")
            print()
            debug_done = True

        consec, cumulative = count_consecutive(rows, mode)
        if consec >= min_days:
            results.append({**stock, "consec_days": consec, "cumulative_qty": cumulative})

        if i % 20 == 0:
            print(f"  {i}/{len(top_stocks)} 처리 중... (현재 통과: {len(results)}개)")

    results.sort(key=lambda x: x["consec_days"], reverse=True)
    return results


def print_results(results: list[dict], mode: str, min_days: int) -> None:
    label = "순매수" if mode == "buy" else "순매도"
    col_today = f"오늘{label}(주)"
    col_cumul = f"기간누적{label}(주)"

    print()
    if not results:
        print(f"외국인 {min_days}일 이상 연속 {label} 종목 없음")
        return

    print(f"외국인 {min_days}일 이상 연속 {label} 종목: {len(results)}개\n")
    header = (
        f"{'종목코드':>8}  {'종목명':^14}  {'연속일':>5}  "
        f"{'현재가':>9}  {'등락률':>7}  {col_today:>14}  {col_cumul:>18}"
    )
    print(header)
    print("-" * len(header))
    for r in results:
        print(
            f"{r['ticker']:>8}  {r['name']:^14}  {r['consec_days']:>4}일  "
            f"{r['price']:>9,}  {float(r['change_rate']):>+6.2f}%  "
            f"{abs(r['today_ntby_qty']):>14,}  {abs(r['cumulative_qty']):>18,}"
        )


async def main():
    parser = argparse.ArgumentParser(description="외국인 연속 순매수/순매도 종목 스크리너")
    parser.add_argument("--days", type=int, default=10, help="최소 연속 일수 (기본: 10)")
    parser.add_argument("--top", type=int, default=150, help="탐색 범위 상위 종목 수 (기본: 150)")
    parser.add_argument(
        "--mode",
        choices=["buy", "sell", "both"],
        default="buy",
        help="buy=순매수, sell=순매도, both=둘 다 (기본: buy)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="기준일 YYYYMMDD (기본: 가장 최근 거래일 자동 계산)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="첫 번째 종목의 원본 API 데이터 출력 (데이터 구조 확인용)",
    )
    args = parser.parse_args()

    if not settings.kis_real_app_key or not settings.kis_real_app_secret:
        print("오류: .env에 KIS_REAL_APP_KEY / KIS_REAL_APP_SECRET 가 설정되지 않았습니다.")
        print("시세 조회 API는 실전 서버 키가 필요합니다. (KIS_IS_MOCK=true여도 별도 필요)")
        return

    base_date = args.date if args.date else last_trading_day()
    modes = ["buy", "sell"] if args.mode == "both" else [args.mode]

    print(f"기준일: {base_date}  |  최소 연속 일수: {args.days}일  |  탐색 범위: 상위 {args.top}개\n")

    for mode in modes:
        if args.mode == "both":
            label = "순매수" if mode == "buy" else "순매도"
            print(f"{'='*50}")
            print(f"  [{label} 스크리닝]")
            print(f"{'='*50}")
        results = await screen_mode(mode, args.days, args.top, debug=args.debug)
        print_results(results, mode, args.days)
        if args.mode == "both" and mode == "buy":
            print()


if __name__ == "__main__":
    asyncio.run(main())
