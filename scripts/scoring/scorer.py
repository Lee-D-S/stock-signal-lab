"""팩터군 스코어 계산.

## 조건 추가 방법
    1. 아래 _CONDITIONS 리스트에 Condition(...) 항목 하나 추가
    2. 끝. 다른 파일은 수정 불필요.

    예시:
        Condition(
            name="new_cond",
            group="momentum",           # 5개 군 중 하나
            description="설명",
            evaluate=lambda ind: (ind["some_key"] > 0) if ind.get("some_key") is not None else None,
        ),

## ind 딕셔너리 키 참조
    calc_all(df) 반환값:
        ma5/20/60/120/240, macd_hist, macd_hist_prev,
        bb_upper, bb_width, bb_width_prev, rsi, stoch_k,
        obv_rising, vol_today, vol_ma20, vol_above_ma, fib_levels
    score_ticker() 가 추가 주입:
        close        — 최신 종가
        dart         — DART 재무데이터 dict (없으면 펀더멘털 군 제외)
        valuation    — KIS 밸류에이션 dict (없으면 가치 군 제외)
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pandas as pd

ROOT    = Path(__file__).parent.parent.parent
SCRIPTS = Path(__file__).parent.parent
for p in (str(ROOT), str(SCRIPTS)):
    if p not in sys.path:
        sys.path.insert(0, p)

from screener_lib.indicators import calc_all  # noqa: E402

# 기본 균등 가중치 (각 군 20%)
DEFAULT_WEIGHTS: dict[str, float] = {
    "momentum":    0.20,
    "trend":       0.20,
    "value":       0.20,
    "fundamental": 0.20,
    "volatility":  0.20,
}


@dataclass
class Condition:
    """팩터 조건 하나를 표현하는 단위.

    evaluate(ind) -> True/False/None
        None = 필요한 데이터가 ind에 없음 (해당 조건은 스코어 계산에서 제외)
    """
    name:        str
    group:       str
    description: str
    evaluate:    Callable[[dict], bool | None]


# ── 조건 목록 ────────────────────────────────────────────────────────────────
# 새 조건 추가 시 여기에만 항목을 추가하면 된다.
# ─────────────────────────────────────────────────────────────────────────────
def _ma_align_short(ind: dict) -> bool | None:
    ma5, ma20, ma60 = ind.get("ma5"), ind.get("ma20"), ind.get("ma60")
    if ma5 is None or ma20 is None or ma60 is None:
        return None
    return bool(ma5 > ma20 > ma60)


def _ma_align_long(ind: dict) -> bool | None:
    ma60, ma120, ma240 = ind.get("ma60"), ind.get("ma120"), ind.get("ma240")
    if ma60 is None or ma120 is None or ma240 is None:
        return None
    return bool(ma60 > ma120 > ma240)


def _fib_support(ind: dict) -> bool | None:
    close = ind.get("close")
    fibs  = ind.get("fib_levels") or []
    if close is None or not fibs:
        return None
    tol = close * 0.02
    return bool(any(abs(close - lvl) <= tol for lvl in fibs))


def _disabled_for_hold20(_ind: dict) -> None:
    """20거래일/거래대금 유니버스 검증에서 탈락한 조건은 스코어에서 제외."""
    return None


_CONDITIONS: list[Condition] = [

    # ── 모멘텀 ────────────────────────────────────────────────────────────────
    Condition(
        # 2026-04-28 hold20/거래대금60 검증: RSI<=40만 train/val 모두 KEEP.
        name="rsi_low", group="momentum", description="RSI <= 40 (과매도 반등)",
        evaluate=lambda ind: (ind["rsi"] <= 40) if ind.get("rsi") is not None else None,
    ),
    Condition(
        # hold20 검증에서 K<=30/40/50/60 모두 DROP이라 20거래일 점수에서는 제외.
        name="stoch_low", group="momentum", description="Stochastic %K <= 50 (과매도 구간)",
        evaluate=_disabled_for_hold20,
    ),
    Condition(
        # hold20 검증에서 CHECK. 단독 신뢰도는 높지 않지만 방향성 필터로 유지.
        name="macd_positive", group="momentum", description="MACD 히스토그램 > 0 (상승 세력 우위)",
        evaluate=lambda ind: (ind["macd_hist"] > 0) if ind.get("macd_hist") is not None else None,
    ),
    Condition(
        # hold20 검증에서 DROP이라 20거래일 점수에서는 제외.
        name="macd_rising", group="momentum", description="MACD 히스토그램 증가 (모멘텀 강화)",
        evaluate=_disabled_for_hold20,
    ),
    Condition(
        # hold20 검증에서 validation 약화로 DROP이라 20거래일 점수에서는 제외.
        name="obv_rising", group="momentum", description="OBV MA5 > MA20 (수급 상승 추세)",
        evaluate=_disabled_for_hold20,
    ),

    # ── 추세 ──────────────────────────────────────────────────────────────────
    Condition(
        # hold20 검증에서 KEEP. 20거래일 기준 가장 안정적인 추세 조건.
        name="ma_align_short", group="trend", description="단기 MA 정배열 (5>20>60)",
        evaluate=_ma_align_short,
    ),
    Condition(
        # hold20 검증에서 DROP. 장기 정배열은 60일 이상 horizon에서 재평가.
        name="ma_align_long", group="trend", description="장기 MA 정배열 (60>120>240)",
        evaluate=_disabled_for_hold20,
    ),
    Condition(
        # hold20 검증에서 CHECK. 완만한 추세 필터로 유지.
        name="bb_above_mid", group="trend", description="현재가 > MA20 (BB 중간선 위)",
        evaluate=lambda ind: (
            bool(ind["close"] > ind["ma20"])
            if ind.get("close") is not None and ind.get("ma20") is not None
            else None
        ),
    ),
    Condition(
        # hold20 검증에서 CHECK. 급증 배수보다 MA20 이상 조건이 더 안정적.
        name="vol_above_ma", group="trend", description="거래량 > 20일 평균 거래량",
        evaluate=lambda ind: bool(ind["vol_above_ma"]) if "vol_above_ma" in ind else None,
    ),

    # ── 가치 (밸류에이션 데이터 필요) ────────────────────────────────────────
    Condition(
        name="per_low", group="value", description="PER 0~15 (저평가)",
        evaluate=lambda ind: (
            bool(0 < ind["valuation"]["per"] <= 15)
            if ind.get("valuation") and ind["valuation"].get("per") is not None
            else None
        ),
    ),
    Condition(
        name="pbr_low", group="value", description="PBR 0~1.5 (저평가)",
        evaluate=lambda ind: (
            bool(0 < ind["valuation"]["pbr"] <= 1.5)
            if ind.get("valuation") and ind["valuation"].get("pbr") is not None
            else None
        ),
    ),
    Condition(
        name="eps_positive", group="value", description="EPS > 0 (흑자)",
        evaluate=lambda ind: (
            bool(ind["valuation"]["eps"] > 0)
            if ind.get("valuation") and ind["valuation"].get("eps") is not None
            else None
        ),
    ),
    Condition(
        name="bps_positive", group="value", description="BPS > 0 (순자산 양수)",
        evaluate=lambda ind: (
            bool(ind["valuation"]["bps"] > 0)
            if ind.get("valuation") and ind["valuation"].get("bps") is not None
            else None
        ),
    ),

    # ── 펀더멘털 (DART 데이터 필요) ──────────────────────────────────────────
    Condition(
        # hold20에는 horizon이 짧고 유효 샘플이 부족해 장기 조건으로 보류.
        name="roe_good", group="fundamental", description="ROE >= 10%",
        evaluate=_disabled_for_hold20,
    ),
    Condition(
        # hold20에는 horizon이 짧고 유효 샘플이 부족해 장기 조건으로 보류.
        name="roa_good", group="fundamental", description="ROA >= 5%",
        evaluate=_disabled_for_hold20,
    ),
    Condition(
        # hold20 long 참고 검증에서는 3%가 가장 양호했지만, 20거래일에는 참고용만 사용.
        name="op_margin_good", group="fundamental", description="영업이익률 >= 3% (참고)",
        evaluate=_disabled_for_hold20,
    ),
    Condition(
        # hold20 검증에서 모든 부채비율 후보가 DROP이라 20거래일 점수에서는 제외.
        name="debt_low", group="fundamental", description="부채비율 <= 100%",
        evaluate=_disabled_for_hold20,
    ),
    Condition(
        # 최소 품질 필터지만 20거래일 조건 탐색 대상은 아니어서 기존 유지.
        name="net_income_pos", group="fundamental", description="당기순이익 > 0",
        evaluate=lambda ind: (
            bool(ind["dart"]["net_income"] > 0)
            if ind.get("dart") and ind["dart"].get("net_income") is not None
            else None
        ),
    ),

    # ── 변동성/수급 ───────────────────────────────────────────────────────────
    Condition(
        # hold20 검증에서 DROP이라 20거래일 점수에서는 제외.
        name="bb_squeeze", group="volatility", description="볼린저 밴드 수축 중 (변동성 폭발 임박)",
        evaluate=_disabled_for_hold20,
    ),
    Condition(
        # hold20 검증에서 수익률은 높지만 validation 승률/IC가 약해 제외.
        name="bb_breakout", group="volatility", description="현재가 > 볼린저 상단 (강한 돌파)",
        evaluate=_disabled_for_hold20,
    ),
    Condition(
        # hold20 검증에서 DROP이라 20거래일 점수에서는 제외.
        name="fib_support", group="volatility", description="피보나치 지지선 ±2% 이내",
        evaluate=_disabled_for_hold20,
    ),
    Condition(
        # hold20 검증에서 1.2x~3.0x 모두 DROP. 급증 조건 대신 vol_above_ma를 사용.
        name="vol_surge", group="volatility", description="거래량 급증 (MA20 x 1.5 이상)",
        evaluate=_disabled_for_hold20,
    ),
    Condition(
        # hold20 검증에서 stoch_low가 약해 Stoch 기반 보조 조건도 제외.
        name="stoch_mid", group="volatility", description="스토캐스틱 20~80 중간대 (추세 중심)",
        evaluate=_disabled_for_hold20,
    ),
]

# ── 파생 상수 (직접 수정 불필요) ─────────────────────────────────────────────
CONDITION_NAMES = [c.name for c in _CONDITIONS]
CONDITION_GROUP = {c.name: c.group for c in _CONDITIONS}
CONDITION_DESC  = {c.name: c.description for c in _CONDITIONS}
CONDITION_REVIEW_STATUS = {
    "rsi_low": "KEEP",
    "stoch_low": "DISABLED_HOLD20",
    "macd_positive": "KEEP",
    "macd_rising": "DISABLED_HOLD20",
    "obv_rising": "DISABLED_HOLD20",
    "ma_align_short": "KEEP",
    "ma_align_long": "DISABLED_HOLD20",
    "bb_above_mid": "KEEP",
    "vol_above_ma": "KEEP",
    "per_low": "RETEST",
    "pbr_low": "RETEST",
    "eps_positive": "KEEP",
    "bps_positive": "KEEP",
    "roe_good": "DISABLED_HOLD20",
    "roa_good": "DISABLED_HOLD20",
    "op_margin_good": "DISABLED_HOLD20",
    "debt_low": "DISABLED_HOLD20",
    "net_income_pos": "KEEP",
    "bb_squeeze": "DISABLED_HOLD20",
    "bb_breakout": "DISABLED_HOLD20",
    "fib_support": "DISABLED_HOLD20",
    "vol_surge": "DISABLED_HOLD20",
    "stoch_mid": "DISABLED_HOLD20",
}
CONDITION_REVIEW_NOTE = {
    "rsi_low": "Hold20 amount-universe calibration selected RSI<=40 over the previous RSI<=50.",
    "stoch_low": "Disabled for hold20: K<=30/40/50/60 all failed train/validation checks.",
    "macd_positive": "Reasonable directional filter; keep unless broader calibration contradicts it.",
    "macd_rising": "Disabled for hold20: validation IC/win-rate were weak.",
    "obv_rising": "Disabled for hold20: train looked strong but validation weakened.",
    "ma_align_short": "Hold20 amount-universe calibration kept MA5>MA20>MA60.",
    "ma_align_long": "Disabled for hold20; evaluate on 60+ day horizons instead.",
    "bb_above_mid": "Hold20 amount-universe calibration kept Close>MA20 as a CHECK trend filter.",
    "vol_above_ma": "Hold20 amount-universe calibration kept volume above MA20; surge multiples were weaker.",
    "per_low": "Global PER<=15 is too blunt; prefer sector-relative or percentile-based logic.",
    "pbr_low": "Global PBR<=1.5 is sector-sensitive and likely too rigid.",
    "eps_positive": "Useful minimum profitability sanity check.",
    "bps_positive": "Useful balance-sheet sanity check.",
    "roe_good": "Disabled for hold20: fundamental horizon is longer and samples were insufficient.",
    "roa_good": "Disabled for hold20: fundamental horizon is longer and samples were insufficient.",
    "op_margin_good": "Disabled for hold20; 3% was only a long-horizon reference candidate.",
    "debt_low": "Disabled for hold20: all debt-ratio candidates failed.",
    "net_income_pos": "Valid minimum quality filter.",
    "bb_squeeze": "Disabled for hold20: validation degraded sharply.",
    "bb_breakout": "Disabled for hold20: high average returns but weak validation IC/win-rate.",
    "fib_support": "Disabled for hold20: failed train/validation checks.",
    "vol_surge": "Disabled for hold20: 1.2x/1.5x/2.0x/3.0x all failed.",
    "stoch_mid": "Disabled for hold20 because Stochastic-based thresholds were weak.",
}
GROUP_NAMES     = list(dict.fromkeys(c.group for c in _CONDITIONS))  # 등장 순서 보존


def _eval_conditions(ind: dict) -> dict[str, bool | None]:
    """모든 조건을 평가해 {name: True/False/None} 딕셔너리를 반환한다."""
    return {c.name: c.evaluate(ind) for c in _CONDITIONS}


def compute_score(
    ind: dict,
    weights: dict[str, float] | None = None,
) -> tuple[float, dict[str, dict]]:
    """ind를 받아 팩터군 스코어를 계산한다.

    Returns:
        score:   0.0 ~ 1.0
        details: {group: {met, total, fill_rate, weight, contribution}}
    """
    if weights is None:
        weights = DEFAULT_WEIGHTS

    conds = _eval_conditions(ind)

    # 군별 조건 목록 구성
    group_conds: dict[str, list[str]] = {}
    for name in CONDITION_NAMES:
        group_conds.setdefault(CONDITION_GROUP[name], []).append(name)

    details: dict[str, dict] = {}
    for gname, cnames in group_conds.items():
        available = [(c, conds[c]) for c in cnames if conds.get(c) is not None]
        if not available:
            details[gname] = {
                "met": 0, "total": 0, "fill_rate": None,
                "weight": weights.get(gname, 0.0), "contribution": 0.0,
            }
            continue
        met   = sum(1 for _, v in available if v)
        total = len(available)
        details[gname] = {
            "met": met, "total": total, "fill_rate": met / total,
            "weight": weights.get(gname, 0.0), "contribution": 0.0,
        }

    # 데이터 있는 군만 재가중
    valid = {g: d for g, d in details.items() if d["fill_rate"] is not None}
    if not valid:
        return 0.0, details

    w_sum = sum(weights.get(g, 0.0) for g in valid)
    if w_sum == 0:
        return 0.0, details

    score = 0.0
    for gname, d in valid.items():
        w = weights.get(gname, 0.0) / w_sum
        d["contribution"] = d["fill_rate"] * w
        score += d["contribution"]

    return score, details


def score_ticker(
    df: pd.DataFrame,
    weights: dict[str, float] | None = None,
    dart: dict | None = None,
    valuation: dict | None = None,
) -> tuple[float, dict[str, dict]]:
    """OHLCV DataFrame 한 종목에 대해 스코어를 계산한다."""
    ind = calc_all(df)
    ind["close"] = float(df["close"].iloc[-1])
    if dart is not None:
        ind["dart"] = dart
    if valuation is not None:
        ind["valuation"] = valuation
    return compute_score(ind, weights)
