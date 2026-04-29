import argparse

import pandas as pd

from ..utils import _safe


def calculate(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"obv_rising": False}
    direction = df["close"].diff().apply(lambda v: 1 if v > 0 else (-1 if v < 0 else 0))
    obv = (direction * df["volume"]).fillna(0).cumsum()
    obv_ma5  = _safe(obv.rolling(5).mean().iloc[-1])
    obv_ma20 = _safe(obv.rolling(20).mean().iloc[-1])
    return {
        "obv_rising": obv_ma5 is not None and obv_ma20 is not None and obv_ma5 > obv_ma20
    }


def add_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--obv-rising", action="store_true",
                        help="OBV 상승 추세 (OBV MA5 > MA20)")


def check(ind: dict, args: argparse.Namespace) -> bool:
    if args.obv_rising and not ind.get("obv_rising", False):
        return False
    return True


def condition_labels(args: argparse.Namespace) -> list[str]:
    return ["OBV상승"] if args.obv_rising else []
