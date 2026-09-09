import argparse

import pandas as pd

from ..utils import _safe


def calculate(df: pd.DataFrame) -> dict:
    volume   = df["volume"]
    vol_ma20 = _safe(volume.rolling(20).mean().iloc[-1])
    vol_today = float(volume.iloc[-1])
    return {
        "vol_today":    vol_today,
        "vol_ma20":     vol_ma20,
        "vol_above_ma": vol_ma20 is not None and vol_today > vol_ma20,
    }


def add_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--vol-above-ma", action="store_true",
                        help="오늘 거래량 > 20일 평균 거래량")


def check(ind: dict, args: argparse.Namespace) -> bool:
    if args.vol_above_ma and not ind.get("vol_above_ma", False):
        return False
    return True


def condition_labels(args: argparse.Namespace) -> list[str]:
    return ["거래량>MA20"] if args.vol_above_ma else []
