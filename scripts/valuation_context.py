from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
BASE_DIR = ROOT / "ai 주가 변동 원인 분석"
VALUATION_CSV = BASE_DIR / "03_원천데이터" / "기업별_PER_EPS_현재스냅샷.csv"

VALUATION_COLUMNS = [
    "valuation_per",
    "valuation_eps",
    "valuation_per_group",
    "valuation_eps_status",
    "valuation_profit_trend",
    "valuation_profit_quality",
    "valuation_class",
    "valuation_trap_check",
    "valuation_memo",
]


def _as_ticker(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip().zfill(6)


def load_valuation_snapshot(path: Path = VALUATION_CSV) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=["ticker", *VALUATION_COLUMNS])
    df = pd.read_csv(path, encoding="utf-8-sig", dtype={"코드": str})
    if df.empty or "코드" not in df.columns:
        return pd.DataFrame(columns=["ticker", *VALUATION_COLUMNS])

    rename_map = {
        "코드": "ticker",
        "PER": "valuation_per",
        "EPS": "valuation_eps",
        "PER구간": "valuation_per_group",
        "EPS상태": "valuation_eps_status",
        "3년순이익방향": "valuation_profit_trend",
        "이익체력판단": "valuation_profit_quality",
        "밸류에이션분류": "valuation_class",
        "저평가함정체크": "valuation_trap_check",
        "해석메모": "valuation_memo",
    }
    out = df.rename(columns=rename_map)
    out["ticker"] = out["ticker"].map(_as_ticker)
    keep = ["ticker", *[col for col in VALUATION_COLUMNS if col in out.columns]]
    return out[keep].drop_duplicates("ticker", keep="last")


def attach_valuation(df: pd.DataFrame, path: Path = VALUATION_CSV) -> pd.DataFrame:
    out = df.copy()
    if "ticker" not in out.columns:
        for col in VALUATION_COLUMNS:
            out[col] = ""
        return out

    out["ticker"] = out["ticker"].map(_as_ticker)
    valuation = load_valuation_snapshot(path)
    if valuation.empty:
        for col in VALUATION_COLUMNS:
            if col not in out.columns:
                out[col] = ""
        return out

    existing = [col for col in VALUATION_COLUMNS if col in out.columns]
    if existing:
        out = out.drop(columns=existing)
    out = out.merge(valuation, on="ticker", how="left")
    for col in VALUATION_COLUMNS:
        if col not in out.columns:
            out[col] = ""
        out[col] = out[col].fillna("")
    return out

