import argparse

import pandas as pd

from ..utils import _safe


def calculate(df: pd.DataFrame) -> dict:
    close = df["close"]
    if len(close) < 20:
        return {}
    mid = close.rolling(20).mean()
    std = close.rolling(20).std()
    upper = mid + (std * 2)
    lower = mid - (std * 2)
    width = ((upper - lower) / mid) * 100
    return {
        "bb_upper":      _safe(upper.iloc[-1]),
        "bb_width":      _safe(width.iloc[-1]),
        "bb_width_prev": _safe(width.iloc[-6]) if len(width) >= 6 else None,
    }


def add_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--bb-breakout", action="store_true",
                        help="현재가 > 볼린저 상단 (강한 상승 신호)")
    parser.add_argument("--bb-squeeze",  action="store_true",
                        help="볼린저 밴드 수축 중 (변동성 폭발 임박)")


def check(ind: dict, args: argparse.Namespace) -> bool:
    if args.bb_breakout:
        ub    = ind.get("bb_upper")
        close = ind.get("close")
        if ub is None or close is None or close <= ub:
            return False
    if args.bb_squeeze:
        bw, bwp = ind.get("bb_width"), ind.get("bb_width_prev")
        if bw is None or bwp is None or bw >= bwp:
            return False
    return True


def condition_labels(args: argparse.Namespace) -> list[str]:
    labels = []
    if args.bb_breakout: labels.append("BB상단돌파")
    if args.bb_squeeze:  labels.append("BB스퀴즈")
    return labels
