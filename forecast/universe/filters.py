from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class UniverseFilterConfig:
    min_price: float = 1_000
    min_avg_value_20d: float = 100_000_000
    min_history_days: int = 252


def apply_universe_filters(frame: pd.DataFrame, *, as_of: pd.Timestamp, config: UniverseFilterConfig | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    config = config or UniverseFilterConfig()
    required = {"ticker", "market", "security_type"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Universe frame is missing columns: {sorted(missing)}")
    rows = frame.copy()
    rows["as_of"] = as_of
    rows["listed_from"] = pd.to_datetime(rows.get("listed_from"), errors="coerce")
    rows["listed_to"] = pd.to_datetime(rows.get("listed_to"), errors="coerce")
    suspended = rows["suspended"] if "suspended" in rows else pd.Series(False, index=rows.index)
    rows["suspended"] = suspended.fillna(False).astype(bool)
    rows["filter_reason"] = ""

    def reject(mask: pd.Series, reason: str) -> None:
        rows.loc[mask & rows["filter_reason"].eq(""), "filter_reason"] = reason

    reject(~rows["market"].isin(["KOSPI", "KOSDAQ", "K", "Q"]), "market")
    reject(~rows["security_type"].astype(str).str.upper().isin(["COMMON", "COMMON_STOCK", "보통주"]), "security_type")
    reject(rows["suspended"], "suspended")
    reject(rows["listed_from"].notna() & (rows["listed_from"] > as_of), "not_listed")
    reject(rows["listed_to"].notna() & (rows["listed_to"] < as_of), "delisted")
    if "price" in rows:
        reject(pd.to_numeric(rows["price"], errors="coerce") < config.min_price, "low_price")
    if "avg_value_20d" in rows:
        reject(pd.to_numeric(rows["avg_value_20d"], errors="coerce") < config.min_avg_value_20d, "low_liquidity")
    if "history_days" in rows:
        reject(pd.to_numeric(rows["history_days"], errors="coerce") < config.min_history_days, "short_history")
    accepted = rows[rows["filter_reason"].eq("")].drop(columns=["filter_reason"])
    rejected = rows[~rows["filter_reason"].eq("")]
    return accepted.reset_index(drop=True), rejected.reset_index(drop=True)
