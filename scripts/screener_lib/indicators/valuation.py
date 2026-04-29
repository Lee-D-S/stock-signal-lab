"""KIS API 기반 밸류에이션 지표 (PER, PBR, EPS, BPS).

ind["valuation"] 에 data.get_kis_valuation() 결과가 주입되어 있어야 함.
조건이 하나라도 지정된 경우 screener.py 가 루프 안에서 KIS inquire-price API를 개별 조회함.

주의: PER=0 은 적자 종목이므로 조건 불통과 처리.
"""

import argparse

import pandas as pd


def calculate(df: pd.DataFrame) -> dict:
    return {}


def needs_valuation(args: argparse.Namespace) -> bool:
    return any([
        args.per_max is not None,
        args.pbr_max is not None,
        args.eps_min is not None,
        args.bps_min is not None,
    ])


def add_args(parser: argparse.ArgumentParser) -> None:
    g = parser.add_argument_group("밸류에이션 (KIS API — DART 불필요)")
    g.add_argument("--per-max", type=float, default=None, metavar="N",
                   help="PER 최댓값 (예: 15) — 0 이하(적자)는 자동 제외")
    g.add_argument("--pbr-max", type=float, default=None, metavar="N",
                   help="PBR 최댓값 (예: 1.5)")
    g.add_argument("--eps-min", type=float, default=None, metavar="WON",
                   help="EPS 최솟값 원 (예: 1000)")
    g.add_argument("--bps-min", type=float, default=None, metavar="WON",
                   help="BPS 최솟값 원 (예: 10000)")


def check(ind: dict, args: argparse.Namespace) -> bool:
    if not needs_valuation(args):
        return True

    val = ind.get("valuation")
    if val is None:
        return False

    if args.per_max is not None:
        per = val.get("per")
        if per is None or per <= 0 or per > args.per_max:
            return False

    if args.pbr_max is not None:
        pbr = val.get("pbr")
        if pbr is None or pbr <= 0 or pbr > args.pbr_max:
            return False

    if args.eps_min is not None:
        eps = val.get("eps")
        if eps is None or eps < args.eps_min:
            return False

    if args.bps_min is not None:
        bps = val.get("bps")
        if bps is None or bps < args.bps_min:
            return False

    return True


def condition_labels(args: argparse.Namespace) -> list[str]:
    labels = []
    if args.per_max is not None: labels.append(f"PER≤{args.per_max}")
    if args.pbr_max is not None: labels.append(f"PBR≤{args.pbr_max}")
    if args.eps_min is not None: labels.append(f"EPS≥{args.eps_min:.0f}원")
    if args.bps_min is not None: labels.append(f"BPS≥{args.bps_min:.0f}원")
    return labels
