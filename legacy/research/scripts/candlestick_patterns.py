from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class CandleSignal:
    pattern_id: str
    pattern_name: str
    category: str
    prediction_direction: str
    confidence: int
    basis: str


def _num(row: pd.Series, key: str) -> float:
    value = row.get(key)
    if value is None or pd.isna(value):
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _shape(row: pd.Series) -> dict[str, float | str]:
    open_price = _num(row, "open")
    high = _num(row, "high")
    low = _num(row, "low")
    close = _num(row, "close")

    real_range = max(high - low, 0.0)
    body = abs(close - open_price)
    upper = max(high - max(open_price, close), 0.0)
    lower = max(min(open_price, close) - low, 0.0)
    body_ratio = body / real_range if real_range else 0.0

    if close > open_price:
        direction = "bull"
    elif close < open_price:
        direction = "bear"
    else:
        direction = "flat"

    return {
        "direction": direction,
        "range": real_range,
        "body": body,
        "body_ratio": body_ratio,
        "upper": upper,
        "lower": lower,
        "open": open_price,
        "close": close,
        "high": high,
        "low": low,
    }


def _trend(df: pd.DataFrame, index: int, lookback: int = 5) -> str:
    if index < lookback:
        return "flat"
    start = float(df.iloc[index - lookback]["close"])
    end = float(df.iloc[index]["close"])
    if start == 0:
        return "flat"
    change = (end - start) / start
    if change >= 0.02:
        return "bull"
    if change <= -0.02:
        return "bear"
    return "flat"


def _avg_body(df: pd.DataFrame, index: int, lookback: int = 10) -> float:
    start = max(0, index - lookback + 1)
    window = df.iloc[start : index + 1]
    if window.empty:
        return 0.0
    return float((window["close"] - window["open"]).abs().mean() or 0.0)


def _gap_up(prev: pd.Series, cur: pd.Series) -> bool:
    return _num(cur, "low") > _num(prev, "high")


def _gap_down(prev: pd.Series, cur: pd.Series) -> bool:
    return _num(cur, "high") < _num(prev, "low")


def _is_doji(shape: dict[str, float | str], threshold: float = 0.1) -> bool:
    return float(shape["body_ratio"]) <= threshold


def _body_low(shape: dict[str, float | str]) -> float:
    return min(float(shape["open"]), float(shape["close"]))


def _body_high(shape: dict[str, float | str]) -> float:
    return max(float(shape["open"]), float(shape["close"]))


def _inside_body(inner: dict[str, float | str], outer: dict[str, float | str]) -> bool:
    return _body_low(outer) <= _body_low(inner) and _body_high(inner) <= _body_high(outer)


def _similar_close(left: dict[str, float | str], right: dict[str, float | str], tolerance: float = 0.003) -> bool:
    ref = max(abs(float(left["close"])), abs(float(right["close"])), 1.0)
    return abs(float(left["close"]) - float(right["close"])) / ref <= tolerance


def _is_long_body(shape: dict[str, float | str], avg_body: float, multiplier: float = 1.0) -> bool:
    return float(shape["body"]) >= avg_body * multiplier if avg_body > 0 else False


def _is_small_body(shape: dict[str, float | str], avg_body: float, multiplier: float = 0.6) -> bool:
    return float(shape["body"]) <= avg_body * multiplier if avg_body > 0 else False


def _body_engulfs(outer: dict[str, float | str], inner: dict[str, float | str]) -> bool:
    return _body_low(outer) <= _body_low(inner) and _body_high(outer) >= _body_high(inner)


def _closes_near_high(shape: dict[str, float | str], threshold: float = 0.25) -> bool:
    rng = float(shape["range"])
    if rng <= 0:
        return False
    return (float(shape["high"]) - float(shape["close"])) / rng <= threshold


def _closes_near_low(shape: dict[str, float | str], threshold: float = 0.25) -> bool:
    rng = float(shape["range"])
    if rng <= 0:
        return False
    return (float(shape["close"]) - float(shape["low"])) / rng <= threshold


def _is_marubozu(shape: dict[str, float | str], threshold: float = 0.05) -> bool:
    rng = float(shape["range"])
    if rng <= 0:
        return False
    return float(shape["upper"]) / rng <= threshold and float(shape["lower"]) / rng <= threshold


def _is_high_wave(shape: dict[str, float | str]) -> bool:
    rng = float(shape["range"])
    if rng <= 0:
        return False
    return float(shape["body_ratio"]) <= 0.2 and float(shape["upper"]) / rng >= 0.35 and float(shape["lower"]) / rng >= 0.35


def _open_near_low(shape: dict[str, float | str], threshold: float = 0.1) -> bool:
    rng = float(shape["range"])
    if rng <= 0:
        return False
    return (float(shape["open"]) - float(shape["low"])) / rng <= threshold


def _open_near_high(shape: dict[str, float | str], threshold: float = 0.1) -> bool:
    rng = float(shape["range"])
    if rng <= 0:
        return False
    return (float(shape["high"]) - float(shape["open"])) / rng <= threshold


def _as_signal(
    pattern_id: str,
    pattern_name: str,
    category: str,
    prediction_direction: str,
    confidence: int,
    basis: str,
) -> CandleSignal:
    return CandleSignal(
        pattern_id=pattern_id,
        pattern_name=pattern_name,
        category=category,
        prediction_direction=prediction_direction,
        confidence=confidence,
        basis=basis,
    )


def detect_at(df: pd.DataFrame, index: int) -> list[CandleSignal]:
    if df.empty or index < 0 or index >= len(df):
        return []

    row = df.iloc[index]
    shape = _shape(row)
    avg_body = _avg_body(df, index)
    trend = _trend(df, index)

    prev = df.iloc[index - 1] if index >= 1 else None
    prev2 = df.iloc[index - 2] if index >= 2 else None
    prev3 = df.iloc[index - 3] if index >= 3 else None
    prev4 = df.iloc[index - 4] if index >= 4 else None

    prev_shape = _shape(prev) if prev is not None else None
    prev2_shape = _shape(prev2) if prev2 is not None else None
    prev3_shape = _shape(prev3) if prev3 is not None else None
    prev4_shape = _shape(prev4) if prev4 is not None else None

    signals: list[CandleSignal] = []

    def add(
        condition: bool,
        pattern_id: str,
        pattern_name: str,
        category: str,
        prediction_direction: str,
        confidence: int,
        basis: str,
    ) -> None:
        if condition:
            signals.append(_as_signal(pattern_id, pattern_name, category, prediction_direction, confidence, basis))

    # Single candle patterns.
    add(
        bool(shape["direction"] == "bull" and _is_long_body(shape, avg_body, 1.8)),
        "single_long_bull",
        "장대 양봉",
        "단일 캔들 패턴",
        "up",
        75,
        "최근 평균 몸통 대비 큰 양봉",
    )
    add(
        bool(shape["direction"] == "bear" and _is_long_body(shape, avg_body, 1.8)),
        "single_long_bear",
        "장대 음봉",
        "단일 캔들 패턴",
        "down",
        75,
        "최근 평균 몸통 대비 큰 음봉",
    )
    add(
        bool(shape["direction"] == "bull" and _is_marubozu(shape)),
        "single_white_marubozu",
        "마루보즈 양봉",
        "단일 캔들 패턴",
        "up",
        80,
        "꼬리가 거의 없는 강한 양봉",
    )
    add(
        bool(shape["direction"] == "bear" and _is_marubozu(shape)),
        "single_black_marubozu",
        "마루보즈 음봉",
        "단일 캔들 패턴",
        "down",
        80,
        "꼬리가 거의 없는 강한 음봉",
    )
    add(
        bool(_is_doji(shape)),
        "single_doji",
        "도지",
        "단일 캔들 패턴",
        "neutral",
        65,
        "시가와 종가가 거의 같은 균형 캔들",
    )
    add(
        bool(_is_doji(shape) and float(shape["upper"]) > float(shape["body"]) and float(shape["lower"]) > float(shape["body"])),
        "single_cross_doji",
        "십자형",
        "단일 캔들 패턴",
        "neutral",
        70,
        "위아래 꼬리가 있는 십자형 도지",
    )
    add(
        bool(_is_doji(shape) and float(shape["upper"]) / max(float(shape["range"]), 1.0) >= 0.35 and float(shape["lower"]) / max(float(shape["range"]), 1.0) >= 0.35),
        "single_long_legged_doji",
        "롱레그드 도지",
        "단일 캔들 패턴",
        "neutral",
        70,
        "긴 위아래 꼬리를 가진 도지",
    )
    add(
        bool(float(shape["body_ratio"]) <= 0.35 and float(shape["upper"]) > float(shape["body"]) and float(shape["lower"]) > float(shape["body"])),
        "single_spinning_top",
        "팽이형",
        "단일 캔들 패턴",
        "neutral",
        65,
        "작은 몸통과 양쪽 꼬리",
    )
    add(
        bool(_is_high_wave(shape)),
        "single_high_wave",
        "하이웨이브 캔들",
        "단일 캔들 패턴",
        "neutral",
        65,
        "작은 몸통과 긴 위아래 꼬리로 변동성 확대",
    )
    add(
        bool(_is_doji(shape) and float(shape["lower"]) / max(float(shape["range"]), 1.0) >= 0.6 and float(shape["upper"]) / max(float(shape["range"]), 1.0) <= 0.1),
        "single_dragonfly_doji",
        "잠자리형 도지",
        "단일 캔들 패턴",
        "up",
        70,
        "긴 아래꼬리 도지",
    )
    add(
        bool(_is_doji(shape) and float(shape["upper"]) / max(float(shape["range"]), 1.0) >= 0.6 and float(shape["lower"]) / max(float(shape["range"]), 1.0) <= 0.1),
        "single_gravestone_doji",
        "비석형 도지",
        "단일 캔들 패턴",
        "down",
        70,
        "긴 위꼬리 도지",
    )

    # Reversal / continuation single-candle patterns.
    add(
        bool(trend == "bear" and shape["direction"] == "bull" and _open_near_low(shape) and _closes_near_high(shape)),
        "bullish_belt_hold",
        "상승 벨트홀드",
        "추세 반전형",
        "up",
        72,
        "하락 뒤 저가 부근 시가에서 강하게 상승",
    )
    add(
        bool(trend == "bull" and shape["direction"] == "bear" and _open_near_high(shape) and _closes_near_low(shape)),
        "bearish_belt_hold",
        "하락 벨트홀드",
        "추세 반전형",
        "down",
        72,
        "상승 뒤 고가 부근 시가에서 강하게 하락",
    )
    add(
        bool(trend == "bear" and float(shape["lower"]) >= float(shape["body"]) * 2 and float(shape["upper"]) <= float(shape["body"]) * 0.5),
        "reversal_hammer",
        "망치형",
        "추세 반전형",
        "up",
        70,
        "하락 흐름 뒤 긴 아래꼬리",
    )
    add(
        bool(trend == "bull" and float(shape["lower"]) >= float(shape["body"]) * 2 and float(shape["upper"]) <= float(shape["body"]) * 0.5),
        "reversal_hanging_man",
        "교수형",
        "추세 반전형",
        "down",
        70,
        "상승 흐름 뒤 긴 아래꼬리",
    )
    add(
        bool(trend == "bear" and float(shape["upper"]) >= float(shape["body"]) * 2 and float(shape["lower"]) <= float(shape["body"]) * 0.5),
        "reversal_inverted_hammer",
        "역망치형",
        "추세 반전형",
        "up",
        70,
        "하락 흐름 뒤 긴 위꼬리",
    )
    add(
        bool(trend == "bull" and float(shape["upper"]) >= float(shape["body"]) * 2 and float(shape["lower"]) <= float(shape["body"]) * 0.5),
        "reversal_shooting_star",
        "유성형",
        "추세 반전형",
        "down",
        70,
        "상승 흐름 뒤 긴 위꼬리",
    )

    if prev is not None and prev_shape is not None:
        # Two-candle patterns.
        add(
            bool(prev_shape["direction"] == "bear" and shape["direction"] == "bull" and _body_engulfs(shape, prev_shape)),
            "bullish_engulfing",
            "상승 장악형",
            "추세 반전형",
            "up",
            80,
            "음봉 몸통을 다음 양봉이 감쌈",
        )
        add(
            bool(prev_shape["direction"] == "bull" and shape["direction"] == "bear" and _body_engulfs(shape, prev_shape)),
            "bearish_engulfing",
            "하락 장악형",
            "추세 반전형",
            "down",
            80,
            "양봉 몸통을 다음 음봉이 감쌈",
        )
        add(
            bool(prev_shape["direction"] == "bear" and shape["direction"] == "bull" and _is_small_body(shape, avg_body) and _inside_body(shape, prev_shape)),
            "bullish_harami",
            "상승 잉태형",
            "추세 반전형",
            "up",
            70,
            "하락 흐름의 큰 음봉 안에 작은 양봉",
        )
        add(
            bool(prev_shape["direction"] == "bull" and shape["direction"] == "bear" and _is_small_body(shape, avg_body) and _inside_body(shape, prev_shape)),
            "bearish_harami",
            "하락 잉태형",
            "추세 반전형",
            "down",
            70,
            "상승 흐름의 큰 양봉 안에 작은 음봉",
        )
        add(
            bool(prev_shape["direction"] == "bear" and _is_doji(shape) and _inside_body(shape, prev_shape)),
            "bullish_harami_cross",
            "상승 십자 잉태형",
            "추세 반전형",
            "up",
            72,
            "큰 음봉 안에 도지 출현",
        )
        add(
            bool(prev_shape["direction"] == "bull" and _is_doji(shape) and _inside_body(shape, prev_shape)),
            "bearish_harami_cross",
            "하락 십자 잉태형",
            "추세 반전형",
            "down",
            72,
            "큰 양봉 안에 도지 출현",
        )
        add(
            bool(prev_shape["direction"] == "bear" and shape["direction"] == "bull" and float(shape["close"]) > (float(prev_shape["open"]) + float(prev_shape["close"])) / 2 and float(shape["close"]) < float(prev_shape["open"])),
            "bullish_piercing",
            "상승 관통형",
            "추세 반전형",
            "up",
            72,
            "전일 음봉 중간 이상 회복",
        )
        add(
            bool(prev_shape["direction"] == "bull" and shape["direction"] == "bear" and float(shape["close"]) < (float(prev_shape["open"]) + float(prev_shape["close"])) / 2 and float(shape["close"]) > float(prev_shape["open"])),
            "bearish_dark_cloud",
            "흑운형",
            "추세 반전형",
            "down",
            72,
            "전일 양봉 중간 아래로 밀림",
        )
        add(
            bool(prev_shape["direction"] == "bear" and shape["direction"] == "bull" and _similar_close(prev_shape, shape)),
            "bullish_counterattack",
            "상승 반격형",
            "추세 반전형",
            "up",
            65,
            "하락 후 반대 양봉이 전일 종가 부근 회복",
        )
        add(
            bool(prev_shape["direction"] == "bull" and shape["direction"] == "bear" and _similar_close(prev_shape, shape)),
            "bearish_counterattack",
            "하락 반격형",
            "추세 반전형",
            "down",
            65,
            "상승 후 반대 음봉이 전일 종가 부근까지 밀림",
        )
        add(
            bool(prev_shape["direction"] != shape["direction"] and _is_long_body(prev_shape, avg_body, 1.0) and _is_long_body(shape, avg_body, 1.0) and _similar_close(prev_shape, shape, 0.25)),
            "railroad_tracks",
            "기찻길형",
            "추세 반전형",
            "neutral",
            60,
            "비슷한 크기의 반대 방향 장대봉 2개",
        )
        add(
            bool(prev_shape["direction"] == "bear" and float(shape["low"]) <= float(prev_shape["low"]) * 1.003),
            "tweezer_bottom",
            "트위저 바텀",
            "추세 반전형",
            "up",
            65,
            "비슷한 저점이 이틀 연속 형성",
        )
        add(
            bool(prev_shape["direction"] == "bull" and float(shape["high"]) >= float(prev_shape["high"]) * 0.997),
            "tweezer_top",
            "트위저 탑",
            "추세 반전형",
            "down",
            65,
            "비슷한 고점이 이틀 연속 형성",
        )
        add(
            bool(prev_shape["direction"] == "bear" and shape["direction"] == "bull" and _similar_close(prev_shape, shape, 0.01)),
            "bullish_separating_line",
            "상승 갈림길형",
            "추세 지속형",
            "up",
            65,
            "같은 시가 부근에서 양봉 전환",
        )
        add(
            bool(prev_shape["direction"] == "bull" and shape["direction"] == "bear" and _similar_close(prev_shape, shape, 0.01)),
            "bearish_separating_line",
            "하락 갈림길형",
            "추세 지속형",
            "down",
            65,
            "같은 시가 부근에서 음봉 전환",
        )
        add(
            bool(prev_shape["direction"] == "bear" and shape["direction"] == "bull" and _gap_down(prev, row)),
            "bullish_kicker",
            "상승 키커",
            "추세 반전형",
            "up",
            80,
            "장대 음봉 뒤 상승 갭 장대 양봉",
        )
        add(
            bool(prev_shape["direction"] == "bull" and shape["direction"] == "bear" and _gap_up(prev, row)),
            "bearish_kicker",
            "하락 키커",
            "추세 반전형",
            "down",
            80,
            "장대 양봉 뒤 하락 갭 장대 음봉",
        )
        add(
            bool(_gap_up(prev, row)),
            "rising_window",
            "라이징 윈도우",
            "추세 지속형",
            "up",
            70,
            "전일 고가 위로 상승 갭 형성",
        )
        add(
            bool(_gap_down(prev, row)),
            "falling_window",
            "폴링 윈도우",
            "추세 지속형",
            "down",
            70,
            "전일 저가 아래로 하락 갭 형성",
        )

    if prev is not None and prev2 is not None and prev_shape is not None and prev2_shape is not None:
        # Three-candle patterns.
        add(
            bool(prev2_shape["direction"] == "bear" and prev_shape["direction"] == "bear" and shape["direction"] == "bull" and _body_engulfs(shape, prev_shape) and float(shape["close"]) > (float(prev2_shape["open"]) + float(prev2_shape["close"])) / 2),
            "three_inside_up",
            "쓰리 인사이드 업",
            "추세 반전형",
            "up",
            82,
            "상승 잉태형 뒤 확인 양봉",
        )
        add(
            bool(prev2_shape["direction"] == "bull" and prev_shape["direction"] == "bull" and shape["direction"] == "bear" and _body_engulfs(shape, prev_shape) and float(shape["close"]) < (float(prev2_shape["open"]) + float(prev2_shape["close"])) / 2),
            "three_inside_down",
            "쓰리 인사이드 다운",
            "추세 반전형",
            "down",
            82,
            "하락 잉태형 뒤 확인 음봉",
        )
        add(
            bool(prev2_shape["direction"] == "bear" and prev_shape["direction"] == "bull" and shape["direction"] == "bull" and float(shape["close"]) > float(prev2_shape["high"])),
            "three_outside_up",
            "쓰리 아웃사이드 업",
            "추세 반전형",
            "up",
            82,
            "상승 장악형 뒤 추가 상승 확인",
        )
        add(
            bool(prev2_shape["direction"] == "bull" and prev_shape["direction"] == "bear" and shape["direction"] == "bear" and float(shape["close"]) < float(prev2_shape["low"])),
            "three_outside_down",
            "쓰리 아웃사이드 다운",
            "추세 반전형",
            "down",
            82,
            "하락 장악형 뒤 추가 하락 확인",
        )
        add(
            bool(prev2_shape["direction"] == "bull" and prev_shape["direction"] == "bull" and shape["direction"] == "bull" and _is_long_body(prev2_shape, avg_body) and _is_long_body(prev_shape, avg_body) and _is_long_body(shape, avg_body) and float(prev2["close"]) < float(prev["close"]) < float(row["close"])),
            "three_white_soldiers",
            "적삼병",
            "추세 지속형",
            "up",
            85,
            "연속 3개 강한 양봉",
        )
        add(
            bool(prev2_shape["direction"] == "bear" and prev_shape["direction"] == "bear" and shape["direction"] == "bear" and _is_long_body(prev2_shape, avg_body) and _is_long_body(prev_shape, avg_body) and _is_long_body(shape, avg_body) and float(prev2["close"]) > float(prev["close"]) > float(row["close"])),
            "three_black_crows",
            "흑삼병",
            "추세 지속형",
            "down",
            85,
            "연속 3개 강한 음봉",
        )
        add(
            bool(prev2_shape["direction"] == "bear" and _is_doji(prev2_shape) and _is_doji(prev_shape) and _is_doji(shape)),
            "bullish_tri_star",
            "상승 세 십자형",
            "추세 반전형",
            "up",
            68,
            "하락 흐름 뒤 3개 연속 도지",
        )
        add(
            bool(prev2_shape["direction"] == "bull" and _is_doji(prev2_shape) and _is_doji(prev_shape) and _is_doji(shape)),
            "bearish_tri_star",
            "하락 세 십자형",
            "추세 반전형",
            "down",
            68,
            "상승 흐름 뒤 3개 연속 도지",
        )
        add(
            bool(prev2_shape["direction"] == "bull" and prev_shape["direction"] == "bear" and shape["direction"] == "bear" and _gap_up(prev2, prev) and _gap_up(prev, row) and float(shape["low"]) > float(prev2["high"])),
            "rising_tasuki_gap",
            "상승 타스키 갭형",
            "추세 지속형",
            "up",
            70,
            "상승 갭을 완전히 메우지 않음",
        )
        add(
            bool(prev2_shape["direction"] == "bear" and prev_shape["direction"] == "bull" and shape["direction"] == "bull" and _gap_down(prev2, prev) and _gap_down(prev, row) and float(shape["high"]) < float(prev2["low"])),
            "falling_tasuki_gap",
            "하락 타스키 갭형",
            "추세 지속형",
            "down",
            70,
            "하락 갭을 완전히 메우지 않음",
        )
        add(
            bool(prev2_shape["direction"] == "bear" and prev_shape["direction"] == "bear" and shape["direction"] == "bull" and float(shape["close"]) < float(prev2["open"]) and float(shape["close"]) > float(prev2["close"])),
            "bullish_three_line_strike",
            "강세 삼선 반격형",
            "추세 지속형",
            "up",
            80,
            "3개 양봉 뒤 긴 음봉 반격이 나와도 상승 지속으로 분류",
        )
        add(
            bool(prev2_shape["direction"] == "bull" and prev_shape["direction"] == "bull" and shape["direction"] == "bear" and float(shape["close"]) > float(prev2["open"]) and float(shape["close"]) < float(prev2["close"])),
            "bearish_three_line_strike",
            "약세 삼선 반격형",
            "추세 지속형",
            "down",
            80,
            "3개 음봉 뒤 긴 양봉 반격이 나와도 하락 지속으로 분류",
        )

    if prev is not None and prev2 is not None and prev3 is not None and prev2_shape is not None and prev3_shape is not None:
        # Four/five-candle continuation patterns.
        add(
            bool(prev3_shape["direction"] == "bull" and prev2_shape["direction"] == "bull" and prev_shape["direction"] == "bull" and shape["direction"] == "bear" and _is_small_body(shape, avg_body, 1.0) and float(shape["close"]) < float(prev3["close"]) and float(shape["close"]) > float(prev3["open"])),
            "rising_three_methods",
            "상승 삼법형",
            "추세 지속형",
            "up",
            78,
            "강한 양봉 뒤 내부 조정 후 고점 돌파",
        )
        add(
            bool(prev3_shape["direction"] == "bull" and prev2_shape["direction"] == "bear" and prev_shape["direction"] == "bear" and shape["direction"] == "bull" and _is_small_body(prev2_shape, avg_body, 1.0) and _is_small_body(prev_shape, avg_body, 1.0) and float(shape["close"]) > float(prev3["high"])),
            "mat_hold",
            "매트 홀드",
            "추세 지속형",
            "up",
            78,
            "상승 갭 뒤 짧은 조정 후 재상승",
        )
        add(
            bool(prev3_shape["direction"] == "bear" and prev2_shape["direction"] == "bear" and prev_shape["direction"] == "bear" and shape["direction"] == "bull" and _is_small_body(shape, avg_body, 1.0) and float(shape["close"]) > float(prev3["close"]) and float(shape["close"]) < float(prev3["open"])),
            "falling_three_methods",
            "하락 삼법형",
            "추세 지속형",
            "down",
            78,
            "강한 음봉 뒤 내부 반등 후 저점 이탈",
        )

    # Abandoned baby / star patterns.
    if prev2 is not None and prev3 is not None and prev2_shape is not None and prev3_shape is not None:
        if _is_doji(prev_shape or shape) if False else False:
            pass

    if prev2 is not None and prev3 is not None:
        middle = prev
        if middle is not None and prev2_shape is not None and prev_shape is not None and prev3_shape is not None:
            middle_shape = prev_shape
            add(
                bool(prev2_shape["direction"] == "bear" and _is_long_body(prev2_shape, avg_body, 1.0) and _is_doji(middle_shape, 0.2) and shape["direction"] == "bull" and _gap_down(prev2, middle) and _gap_up(middle, row)),
                "bullish_abandoned_baby",
                "상승 버려진 아이",
                "추세 반전형",
                "up",
                82,
                "하락 뒤 도지가 양쪽 갭으로 고립",
            )
            add(
                bool(prev2_shape["direction"] == "bull" and _is_long_body(prev2_shape, avg_body, 1.0) and _is_doji(middle_shape, 0.2) and shape["direction"] == "bear" and _gap_up(prev2, middle) and _gap_down(middle, row)),
                "bearish_abandoned_baby",
                "하락 버려진 아이",
                "추세 반전형",
                "down",
                82,
                "상승 뒤 도지가 양쪽 갭으로 고립",
            )
            add(
                bool(prev2_shape["direction"] == "bear" and _is_long_body(prev2_shape, avg_body, 1.0) and _is_small_body(middle_shape, avg_body, 0.6) and shape["direction"] == "bull" and float(shape["close"]) > float(prev2["high"])),
                "morning_star",
                "샛별형",
                "추세 반전형",
                "up",
                82,
                "큰 음봉 뒤 작은 몸통, 다음 양봉 회복",
            )
            add(
                bool(prev2_shape["direction"] == "bear" and _is_long_body(prev2_shape, avg_body, 1.0) and _is_doji(middle_shape) and shape["direction"] == "bull" and float(shape["close"]) > float(prev2["high"])),
                "morning_doji_star",
                "모닝 도지 스타",
                "추세 반전형",
                "up",
                82,
                "샛별형의 중간봉이 도지",
            )
            add(
                bool(prev2_shape["direction"] == "bull" and _is_long_body(prev2_shape, avg_body, 1.0) and _is_small_body(middle_shape, avg_body, 0.6) and shape["direction"] == "bear" and float(shape["close"]) < float(prev2["low"])),
                "evening_star",
                "석별형",
                "추세 반전형",
                "down",
                82,
                "큰 양봉 뒤 작은 몸통, 다음 음봉 이탈",
            )
            add(
                bool(prev2_shape["direction"] == "bull" and _is_long_body(prev2_shape, avg_body, 1.0) and _is_doji(middle_shape) and shape["direction"] == "bear" and float(shape["close"]) < float(prev2["low"])),
                "evening_doji_star",
                "이브닝 도지 스타",
                "추세 반전형",
                "down",
                82,
                "석별형의 중간봉이 도지",
            )

    # These two patterns are directional envelope styles around the prior body.
    if prev is not None and prev_shape is not None:
        if index >= 1:
            add(
                bool(prev_shape["direction"] == "bear" and shape["direction"] == "bull" and _body_low(shape) > _body_low(prev_shape) and _body_high(shape) < _body_high(prev_shape)),
                "bullish_counterattack",
                "상승 반격형",
                "추세 반전형",
                "up",
                65,
                "하락 후 반대 양봉이 전일 종가 부근 회복",
            )
            add(
                bool(prev_shape["direction"] == "bull" and shape["direction"] == "bear" and _body_low(shape) < _body_low(prev_shape) and _body_high(shape) > _body_high(prev_shape)),
                "bearish_counterattack",
                "하락 반격형",
                "추세 반전형",
                "down",
                65,
                "상승 후 반대 음봉이 전일 종가 부근까지 밀림",
            )

    return signals


def detect_all(df: pd.DataFrame) -> list[dict[str, Any]]:
    required = {"date", "open", "high", "low", "close"}
    if df.empty or not required.issubset(df.columns):
        return []

    work = df.sort_values("date").reset_index(drop=True)
    rows: list[dict[str, Any]] = []

    for index in range(len(work)):
        for signal in detect_at(work, index):
            row = work.iloc[index]
            rows.append(
                {
                    "signal_date": pd.Timestamp(row["date"]).strftime("%Y-%m-%d"),
                    "pattern_id": signal.pattern_id,
                    "pattern_name": signal.pattern_name,
                    "category": signal.category,
                    "prediction_direction": signal.prediction_direction,
                    "confidence": signal.confidence,
                    "basis": signal.basis,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row["volume"]) if "volume" in work.columns and pd.notna(row.get("volume")) else "",
                    "trade_amount": float(row["trade_amount"]) if "trade_amount" in work.columns and pd.notna(row.get("trade_amount")) else "",
                }
            )

    return rows
