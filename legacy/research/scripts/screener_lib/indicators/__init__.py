"""지표 레지스트리.

새 지표 추가 방법:
  1. screener_lib/indicators/ 에 파일 생성
  2. calculate / add_args / check / condition_labels 4개 함수 구현
  3. 아래 INDICATORS 리스트에 등록
"""

import argparse
import pandas as pd

from . import bollinger, fibonacci, fundamentals, ma, macd, obv, rsi, stochastic, valuation, volume

INDICATORS = [ma, macd, bollinger, rsi, stochastic, obv, volume, fibonacci, fundamentals, valuation]


def calc_all(df: pd.DataFrame) -> dict:
    ind = {}
    for indicator in INDICATORS:
        ind.update(indicator.calculate(df))
    return ind


def add_all_args(parser: argparse.ArgumentParser) -> None:
    for indicator in INDICATORS:
        indicator.add_args(parser)


def check_all(ind: dict, args: argparse.Namespace) -> bool:
    return all(indicator.check(ind, args) for indicator in INDICATORS)


def all_labels(args: argparse.Namespace) -> list[str]:
    labels = []
    for indicator in INDICATORS:
        labels.extend(indicator.condition_labels(args))
    return labels
