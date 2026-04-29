"""Factor Research IC 결과 -> 팩터군별 가중치 변환.

run_discovery.py 가 저장한 *_ic_ranking.csv 를 읽어
군별 평균 IC 절댓값을 정규화한 가중치를 반환한다.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .scorer import DEFAULT_WEIGHTS, GROUP_NAMES

# discovery FEATURE_COLS -> 팩터군 매핑
_FEATURE_TO_GROUP: dict[str, str] = {
    "price_vs_ma5":   "trend",
    "price_vs_ma20":  "trend",
    "price_vs_ma60":  "trend",
    "price_vs_ma120": "trend",
    "price_vs_ma240": "trend",
    "ma_align_short": "trend",
    "ma_align_long":  "trend",
    "vol_ratio":      "trend",
    "macd_hist_norm": "momentum",
    "macd_cross_up":  "momentum",
    "rsi":            "momentum",
    "stoch_k":        "momentum",
    "obv_rising":     "momentum",
    "bb_position":    "volatility",
    "bb_squeeze":     "volatility",
    "fib_support":    "volatility",
}


def load_ic_weights(ic_csv_path: str | Path) -> dict[str, float]:
    """IC CSV에서 군별 가중치를 계산한다.

    데이터가 없는 군(value, fundamental 등)은 DEFAULT_WEIGHTS 기본값 사용.
    결과의 합은 항상 1.0이다.
    """
    df = pd.read_csv(ic_csv_path)

    group_ics: dict[str, list[float]] = {g: [] for g in GROUP_NAMES}
    for _, row in df.iterrows():
        feat = str(row.get("feature", ""))
        ic   = row.get("ic")
        if ic is None or pd.isna(ic):
            continue
        group = _FEATURE_TO_GROUP.get(feat)
        if group:
            group_ics[group].append(abs(float(ic)))

    weights: dict[str, float] = {}
    for g in GROUP_NAMES:
        ics = group_ics.get(g, [])
        weights[g] = sum(ics) / len(ics) if ics else DEFAULT_WEIGHTS.get(g, 0.20)

    total = sum(weights.values())
    if total == 0:
        return dict(DEFAULT_WEIGHTS)
    return {g: w / total for g, w in weights.items()}


def print_weights(weights: dict[str, float]) -> None:
    """군별 가중치 테이블을 출력한다."""
    GROUP_KO = {
        "momentum":    "모멘텀",
        "trend":       "추세",
        "value":       "가치",
        "fundamental": "펀더멘털",
        "volatility":  "변동성/수급",
    }
    print()
    print("[ 팩터군 가중치 ]")
    for g in GROUP_NAMES:
        w = weights.get(g, 0.0)
        bar = "#" * int(w * 100 / 5)
        print(f"  {GROUP_KO.get(g, g):<10}  {w:.1%}  {bar}")
    print()
