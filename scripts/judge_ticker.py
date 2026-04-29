#!/usr/bin/env python3
"""단일 종목을 저장소의 조건 로직으로 판정한다.

Usage:
    python scripts/judge_ticker.py 두산에너빌리티
    python scripts/judge_ticker.py 034020
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from dotenv import load_dotenv

load_dotenv()

from core.market_data import get_current_price  # noqa: E402
from screener_lib.dart import fetch_dart_fundamentals, get_corp_info_map  # noqa: E402
from screener_lib.data import get_kis_valuation, get_ohlcv  # noqa: E402
from screener_lib.indicators import calc_all  # noqa: E402
from scoring.scorer import (  # noqa: E402
    CONDITION_DESC,
    CONDITION_GROUP,
    GROUP_NAMES,
    _eval_conditions,
    compute_score,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="단일 종목 조건 판정")
    parser.add_argument("query", help="종목코드(6자리) 또는 종목명")
    return parser.parse_args()


async def resolve_ticker(query: str) -> tuple[str, str]:
    query = query.strip()
    if query.isdigit() and len(query) == 6:
        return query, ""

    corp_info = await get_corp_info_map()
    exact_matches = [
        (ticker, info["corp_name"])
        for ticker, info in corp_info.items()
        if info.get("corp_name") == query
    ]
    if len(exact_matches) == 1:
        return exact_matches[0]

    partial_matches = [
        (ticker, info["corp_name"])
        for ticker, info in corp_info.items()
        if query in info.get("corp_name", "")
    ]
    if len(partial_matches) == 1:
        return partial_matches[0]
    if partial_matches:
        sample = ", ".join(f"{name}({ticker})" for ticker, name in partial_matches[:10])
        raise ValueError(f"종목명이 여러 개로 매칭됩니다: {sample}")

    raise ValueError(f"종목명을 찾지 못했습니다: {query}")


def verdict_from_score(score: float) -> str:
    if score >= 0.65:
        return "상승 우세"
    if score >= 0.50:
        return "약한 상승 우세"
    if score >= 0.35:
        return "중립"
    if score >= 0.20:
        return "약한 하락 우세"
    return "하락 우세"


def condition_status(value: bool | None) -> str:
    if value is True:
        return "PASS"
    if value is False:
        return "FAIL"
    return "N/A"


async def main() -> None:
    args = parse_args()
    ticker, resolved_name = await resolve_ticker(args.query)

    price_info = await get_current_price(ticker)
    valuation = await get_kis_valuation(ticker)
    df, trade_amount = await get_ohlcv(ticker)
    dart = (await fetch_dart_fundamentals([ticker])).get(ticker)

    if df.empty:
        raise RuntimeError(f"OHLCV 조회 실패: {ticker}")

    name = resolved_name or price_info.get("name") or ticker

    ind = calc_all(df)
    ind["close"] = float(df["close"].iloc[-1])
    ind["valuation"] = valuation
    if dart is not None:
        ind["dart"] = dart

    conditions = _eval_conditions(ind)
    score, details = compute_score(ind)

    passed = sum(1 for v in conditions.values() if v is True)
    failed = sum(1 for v in conditions.values() if v is False)
    unavailable = sum(1 for v in conditions.values() if v is None)

    print(f"[ 종합 판정 ] {name} ({ticker})")
    print(f"현재가: {price_info['price']:,}원")
    print(f"거래대금: {trade_amount:,}원")
    print(f"스코어: {score:.1%}")
    print(f"결론: {verdict_from_score(score)}")
    print(f"조건 집계: PASS {passed} / FAIL {failed} / N/A {unavailable}")
    print()

    print("[ 그룹별 점수 ]")
    for group in GROUP_NAMES:
        detail = details[group]
        fill_rate = detail["fill_rate"]
        fill_text = "N/A" if fill_rate is None else f"{fill_rate:.0%}"
        print(
            f"- {group}: {detail['met']}/{detail['total']} "
            f"(fill {fill_text}, contribution {detail['contribution']:.1%})"
        )
    print()

    print("[ 밸류에이션 ]")
    if valuation is None:
        print("- 조회 실패")
    else:
        print(
            f"- PER {valuation.get('per')} | PBR {valuation.get('pbr')} | "
            f"EPS {valuation.get('eps')} | BPS {valuation.get('bps')}"
        )
    print()

    print("[ 재무 ]")
    if dart is None:
        print("- 조회 실패")
    else:
        print(
            f"- 매출 {dart.get('revenue')} | 영업이익 {dart.get('op_income')} | "
            f"영업이익률 {dart.get('op_margin')}%"
        )
        print(
            f"- 자본 {dart.get('equity')} | 부채 {dart.get('total_debt')} | "
            f"부채비율 {dart.get('debt_ratio')}%"
        )
    print()

    print("[ 조건별 결과 ]")
    for group in GROUP_NAMES:
        for name_key, result in conditions.items():
            if CONDITION_GROUP[name_key] != group:
                continue
            print(f"- {group:<11} {condition_status(result):<4} {CONDITION_DESC[name_key]}")

    if len(df) < 240:
        print()
        print(f"참고: 확보된 일봉이 {len(df)}개라 MA120/240 기반 장기 추세 조건 일부는 N/A일 수 있습니다.")


if __name__ == "__main__":
    asyncio.run(main())
