import argparse

import pandas as pd

from ..utils import _safe


def calculate(df: pd.DataFrame) -> dict:
    close = df["close"]
    if len(close) < 35:
        return {}
    ema_fast = close.ewm(span=12, adjust=False).mean()
    ema_slow = close.ewm(span=26, adjust=False).mean()
    macd = ema_fast - ema_slow
    signal = macd.ewm(span=9, adjust=False).mean()
    hist = macd - signal
    return {
        "macd_hist":      _safe(hist.iloc[-1]),
        "macd_hist_prev": _safe(hist.iloc[-2]),
    }


def add_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--macd-positive", action="store_true",
                        help="MACD 히스토그램 > 0")
    parser.add_argument("--macd-cross-up", action="store_true",
                        help="MACD 히스토그램 음→양 전환 (골든크로스)")


def check(ind: dict, args: argparse.Namespace) -> bool:
    if args.macd_positive:
        h = ind.get("macd_hist")
        if h is None or h <= 0:
            return False
    if args.macd_cross_up:
        h, hp = ind.get("macd_hist"), ind.get("macd_hist_prev")
        if h is None or hp is None or not (h > 0 and hp <= 0):
            return False
    return True


def condition_labels(args: argparse.Namespace) -> list[str]:
    labels = []
    if args.macd_positive: labels.append("MACD히스토>0")
    if args.macd_cross_up: labels.append("MACD골든크로스")
    return labels
