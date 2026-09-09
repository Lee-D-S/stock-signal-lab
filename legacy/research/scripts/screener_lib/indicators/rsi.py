import argparse

import pandas as pd

from ..utils import _safe


def calculate(df: pd.DataFrame) -> dict:
    close = df["close"]
    if len(close) < 15:
        return {}
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / 14, min_periods=14, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return {"rsi": _safe(rsi.iloc[-1])}


def add_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--rsi-min", type=float, default=None, metavar="VAL",
                        help="RSI 최솟값 (이상)")
    parser.add_argument("--rsi-max", type=float, default=None, metavar="VAL",
                        help="RSI 최댓값 (이하)")


def check(ind: dict, args: argparse.Namespace) -> bool:
    r = ind.get("rsi")
    if args.rsi_min is not None and (r is None or r < args.rsi_min):
        return False
    if args.rsi_max is not None and (r is None or r > args.rsi_max):
        return False
    return True


def condition_labels(args: argparse.Namespace) -> list[str]:
    labels = []
    if args.rsi_min is not None: labels.append(f"RSI≥{args.rsi_min}")
    if args.rsi_max is not None: labels.append(f"RSI≤{args.rsi_max}")
    return labels
