import argparse

import pandas as pd

_FIB_RATIOS   = [0.236, 0.382, 0.5, 0.618, 0.786]
_FIB_TOL      = 0.02   # 현재가 ±2% 이내를 지지선 근처로 판단
_FIB_LOOKBACK = 60     # 고점/저점 탐색 기간 (거래일)


def calculate(df: pd.DataFrame) -> dict:
    lb  = min(_FIB_LOOKBACK, len(df))
    hi  = float(df["high"].iloc[-lb:].max())
    lo  = float(df["low"].iloc[-lb:].min())
    rng = hi - lo
    return {
        "fib_levels": [lo + rng * (1 - r) for r in _FIB_RATIOS] if rng > 0 else []
    }


def add_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--fib-support", action="store_true",
                        help="현재가가 피보나치 지지선 ±2%% 이내")


def check(ind: dict, args: argparse.Namespace) -> bool:
    if not args.fib_support:
        return True
    close = ind.get("close")
    if close is None:
        return False
    tol = close * _FIB_TOL
    return any(abs(close - lvl) <= tol for lvl in ind.get("fib_levels", []))


def condition_labels(args: argparse.Namespace) -> list[str]:
    return ["피보나치지지"] if args.fib_support else []
