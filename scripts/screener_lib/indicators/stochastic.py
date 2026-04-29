import argparse

import pandas as pd

from ..utils import _safe


def calculate(df: pd.DataFrame) -> dict:
    if len(df) < 16:
        return {}
    low_min = df["low"].rolling(14).min()
    high_max = df["high"].rolling(14).max()
    raw_k = ((df["close"] - low_min) / (high_max - low_min)) * 100
    stoch_k = raw_k.rolling(3).mean()
    return {"stoch_k": _safe(stoch_k.iloc[-1])}


def add_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--stoch-min", type=float, default=None, metavar="VAL",
                        help="스토캐스틱 %%K 최솟값 (이상)")
    parser.add_argument("--stoch-max", type=float, default=None, metavar="VAL",
                        help="스토캐스틱 %%K 최댓값 (이하)")


def check(ind: dict, args: argparse.Namespace) -> bool:
    k = ind.get("stoch_k")
    if args.stoch_min is not None and (k is None or k < args.stoch_min):
        return False
    if args.stoch_max is not None and (k is None or k > args.stoch_max):
        return False
    return True


def condition_labels(args: argparse.Namespace) -> list[str]:
    labels = []
    if args.stoch_min is not None: labels.append(f"Stoch≥{args.stoch_min}")
    if args.stoch_max is not None: labels.append(f"Stoch≤{args.stoch_max}")
    return labels
