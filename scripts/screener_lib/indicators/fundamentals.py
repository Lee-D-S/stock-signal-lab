"""DART 재무제표 기반 펀더멘털 조건.

ind["dart"] 에 dart.fetch_dart_fundamentals() 결과가 주입되어 있어야 함.
조건이 하나라도 지정된 경우 screener.py 가 DART API를 사전 일괄 조회함.

제공 필드:
    revenue     매출액 (원)
    op_income   영업이익 (원)
    net_income  당기순이익 (원)
    total_assets 자산총계
    total_debt  부채총계
    equity      자본총계
    op_margin   영업이익률 (%)
    debt_ratio  부채비율 (%)
    roe         ROE (%)
"""

import argparse

import pandas as pd


def calculate(df: pd.DataFrame) -> dict:
    # DART 데이터는 screener.py 에서 ind["dart"] 로 외부 주입
    return {}


def needs_dart(args: argparse.Namespace) -> bool:
    return any([
        args.roe_min        is not None,
        args.roa_min        is not None,
        args.op_margin_min  is not None,
        args.net_margin_min is not None,
        args.debt_max       is not None,
        args.revenue_min    is not None,
        args.op_income_min  is not None,
        args.net_income_min is not None,
    ])


def add_args(parser: argparse.ArgumentParser) -> None:
    g = parser.add_argument_group("펀더멘털 (DART 재무제표, DART_API_KEY 필요)")
    g.add_argument("--roe-min",        type=float, default=None, metavar="PCT",
                   help="ROE 최솟값 %% (예: 15)")
    g.add_argument("--roa-min",        type=float, default=None, metavar="PCT",
                   help="ROA 최솟값 %% (예: 5)")
    g.add_argument("--op-margin-min",  type=float, default=None, metavar="PCT",
                   help="영업이익률 최솟값 %% (예: 10)")
    g.add_argument("--net-margin-min", type=float, default=None, metavar="PCT",
                   help="순이익률 최솟값 %% (예: 5)")
    g.add_argument("--debt-max",       type=float, default=None, metavar="PCT",
                   help="부채비율 최댓값 %% (예: 100)")
    g.add_argument("--revenue-min",    type=int,   default=None, metavar="WON",
                   help="최소 매출액 (원, 예: 100000000000 = 1000억)")
    g.add_argument("--op-income-min",  type=int,   default=None, metavar="WON",
                   help="최소 영업이익 (원, 예: 10000000000 = 100억)")
    g.add_argument("--net-income-min", type=int,   default=None, metavar="WON",
                   help="최소 당기순이익 (원, 예: 5000000000 = 50억)")


def check(ind: dict, args: argparse.Namespace) -> bool:
    if not needs_dart(args):
        return True

    dart = ind.get("dart")
    if dart is None:
        return False  # DART 데이터 없으면 탈락

    if args.roe_min is not None:
        roe = dart.get("roe")
        if roe is None or roe < args.roe_min:
            return False

    if args.roa_min is not None:
        roa = dart.get("roa")
        if roa is None or roa < args.roa_min:
            return False

    if args.op_margin_min is not None:
        margin = dart.get("op_margin")
        if margin is None or margin < args.op_margin_min:
            return False

    if args.net_margin_min is not None:
        nm = dart.get("net_margin")
        if nm is None or nm < args.net_margin_min:
            return False

    if args.debt_max is not None:
        debt = dart.get("debt_ratio")
        if debt is None or debt > args.debt_max:
            return False

    if args.revenue_min is not None:
        rev = dart.get("revenue")
        if rev is None or rev < args.revenue_min:
            return False

    if args.op_income_min is not None:
        op = dart.get("op_income")
        if op is None or op < args.op_income_min:
            return False

    if args.net_income_min is not None:
        net = dart.get("net_income")
        if net is None or net < args.net_income_min:
            return False

    return True


def condition_labels(args: argparse.Namespace) -> list[str]:
    labels = []
    if args.roe_min        is not None: labels.append(f"ROE≥{args.roe_min}%")
    if args.roa_min        is not None: labels.append(f"ROA≥{args.roa_min}%")
    if args.op_margin_min  is not None: labels.append(f"영업이익률≥{args.op_margin_min}%")
    if args.net_margin_min is not None: labels.append(f"순이익률≥{args.net_margin_min}%")
    if args.debt_max       is not None: labels.append(f"부채비율≤{args.debt_max}%")
    if args.revenue_min    is not None:
        labels.append(f"매출액≥{args.revenue_min // 100_000_000}억")
    if args.op_income_min  is not None:
        labels.append(f"영업이익≥{args.op_income_min // 100_000_000}억")
    if args.net_income_min is not None:
        labels.append(f"순이익≥{args.net_income_min // 100_000_000}억")
    return labels
