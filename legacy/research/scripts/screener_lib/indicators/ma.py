import argparse

import pandas as pd

from ..utils import _safe


def calculate(df: pd.DataFrame) -> dict:
    close = df["close"]
    return {f"ma{p}": _safe(close.rolling(p).mean().iloc[-1]) for p in [5, 20, 60, 120, 240]}


def add_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--ma-align",
        type=lambda s: [int(x) for x in s.split(",")],
        default=None,
        metavar="PERIODS",
        help="이동평균 정배열 조건 (예: 60,120,240 → MA60>MA120>MA240)",
    )


def check(ind: dict, args: argparse.Namespace) -> bool:
    if not args.ma_align:
        return True
    mas = [ind.get(f"ma{p}") for p in args.ma_align]
    if any(m is None for m in mas):
        return False
    return all(mas[i] > mas[i + 1] for i in range(len(mas) - 1))


def condition_labels(args: argparse.Namespace) -> list[str]:
    if args.ma_align:
        return ["MA정배열(" + "→".join(str(p) for p in args.ma_align) + ")"]
    return []
