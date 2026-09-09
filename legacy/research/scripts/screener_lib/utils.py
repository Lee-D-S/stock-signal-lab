import pandas as pd


def _safe(val) -> float | None:
    return None if (val is None or pd.isna(val)) else float(val)


def _fmt_amount(won: int) -> str:
    eok = won // 100_000_000
    if eok >= 10_000:
        return f"{eok / 10_000:.1f}조"
    return f"{eok:,}억"
