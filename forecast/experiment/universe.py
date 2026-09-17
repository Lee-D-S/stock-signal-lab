from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


_EXCLUDED_NAME_TOKENS = ("ETF", "ETN", "스팩", "우선", "리츠", "인버스", "레버리지")
_MARKET_CAP_COLUMNS = ("market_cap", "market_capitalization", "mkt_cap", "market_cap_value")
_LIQUIDITY_COLUMNS = ("avg_value_20d", "average_value_20d", "trading_value_20d")
_HISTORY_COLUMNS = ("history_days", "listed_days")
_SUSPENSION_COLUMNS = ("suspended", "is_suspended", "trading_halt")


@dataclass(frozen=True)
class UniverseConfig:
    selection_date: str = "2024-12-31"
    size: int = 50
    min_history_days: int = 250
    min_avg_value_20d: float = 0.0
    selection_rule: str = "KOSPI/KOSDAQ common stocks by market cap with quality filters"


@dataclass(frozen=True)
class UniverseBuildResult:
    manifest: pd.DataFrame
    rejected: pd.DataFrame


def _first_present(frame: pd.DataFrame, names: tuple[str, ...], *, required: bool = False) -> str | None:
    for name in names:
        if name in frame.columns:
            return name
    if required:
        raise ValueError(f"Candidate universe is missing one of: {', '.join(names)}")
    return None


def _normalise_ticker(value: object) -> str:
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(6)


def _select_columns(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.rename(
        columns={
            "_market_cap": "market_cap",
            "_liquidity": "avg_value_20d",
            "_history_days": "history_days",
            "_suspended": "suspended",
        }
    ).copy()
    columns = [
        "selection_date", "selection_rule", "selection_rank", "ticker", "name", "market",
        "security_type", "market_cap", "avg_value_20d", "history_days", "suspended",
        "eligible", "selected", "exclusion_reason",
    ]
    for column in columns:
        if column not in result:
            result[column] = pd.NA
    return result[columns]


def build_fixed_universe(candidates: pd.DataFrame, *, config: UniverseConfig | None = None) -> UniverseBuildResult:
    """Select and freeze a reproducible company cohort from a dated candidate snapshot."""
    config = config or UniverseConfig()
    if config.size <= 0:
        raise ValueError("Universe size must be positive")
    if "ticker" not in candidates.columns:
        raise ValueError("Candidate universe requires ticker")

    frame = candidates.copy()
    frame["ticker"] = frame["ticker"].map(_normalise_ticker)
    if frame["ticker"].duplicated().any():
        duplicates = sorted(frame.loc[frame["ticker"].duplicated(), "ticker"].unique())
        raise ValueError(f"Candidate universe contains duplicate tickers: {duplicates}")

    for column in ("name", "market", "security_type"):
        if column not in frame:
            frame[column] = ""
    market_cap_column = _first_present(frame, _MARKET_CAP_COLUMNS, required=True)
    liquidity_column = _first_present(frame, _LIQUIDITY_COLUMNS)
    history_column = _first_present(frame, _HISTORY_COLUMNS)
    suspension_column = _first_present(frame, _SUSPENSION_COLUMNS)

    frame["_market_cap"] = pd.to_numeric(frame[market_cap_column], errors="coerce")
    frame["_liquidity"] = pd.to_numeric(frame[liquidity_column], errors="coerce") if liquidity_column else 0.0
    frame["_history_days"] = pd.to_numeric(frame[history_column], errors="coerce") if history_column else 0.0
    frame["_suspended"] = frame[suspension_column].fillna(False).astype(bool) if suspension_column else False
    name_upper = frame["name"].fillna("").astype(str).str.upper()
    security_upper = frame["security_type"].fillna("").astype(str).str.upper()

    reasons = pd.Series("", index=frame.index, dtype="object")

    def reject(mask: pd.Series, reason: str) -> None:
        reasons.loc[mask & reasons.eq("")] = reason

    reject(~security_upper.isin({"", "COMMON", "COMMON_STOCK", "보통주"}), "not_common_stock")
    reject(name_upper.apply(lambda name: any(token in name for token in _EXCLUDED_NAME_TOKENS)), "excluded_security_name")
    reject(frame["_suspended"], "suspended")
    reject(frame["_history_days"] < config.min_history_days, "insufficient_history")
    reject(frame["_liquidity"] < config.min_avg_value_20d, "insufficient_liquidity")
    reject(frame["_market_cap"].isna(), "missing_market_cap")

    frame["exclusion_reason"] = reasons.replace("", pd.NA)
    eligible = frame["exclusion_reason"].isna()
    ranked = frame.loc[eligible].sort_values(["_market_cap", "ticker"], ascending=[False, True]).copy()
    if len(ranked) < config.size:
        raise ValueError(f"Only {len(ranked)} candidates pass filters; {config.size} are required")

    selected = ranked.head(config.size).copy()
    selected["selection_rank"] = range(1, config.size + 1)
    selected["selection_date"] = config.selection_date
    selected["selection_rule"] = config.selection_rule
    selected["eligible"] = True
    selected["selected"] = True

    rejected = frame.loc[~frame.index.isin(selected.index)].copy()
    rejected["selection_rank"] = pd.NA
    rejected["selection_date"] = config.selection_date
    rejected["selection_rule"] = config.selection_rule
    rejected["eligible"] = rejected["exclusion_reason"].isna()
    rejected["selected"] = False
    return UniverseBuildResult(
        manifest=_select_columns(selected).reset_index(drop=True),
        rejected=_select_columns(rejected).reset_index(drop=True),
    )


def validate_fixed_universe(frame: pd.DataFrame, *, expected_size: int = 50, selection_date: str = "2024-12-31") -> pd.DataFrame:
    """Validate a stored selected-only manifest before it enters the daily path."""
    required = {"ticker", "selection_date", "selected"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Universe manifest is missing columns: {sorted(missing)}")
    result = frame.loc[frame["selected"].astype(bool)].copy()
    if len(result) != expected_size:
        raise ValueError(f"Universe manifest has {len(result)} selected companies; expected {expected_size}")
    if result["ticker"].astype(str).duplicated().any():
        raise ValueError("Universe manifest contains duplicate tickers")
    if result["selection_date"].astype(str).nunique() != 1 or str(result["selection_date"].iloc[0]) != selection_date:
        raise ValueError(f"Universe manifest must be selected on {selection_date}")
    return result.reset_index(drop=True)


def write_universe_files(result: UniverseBuildResult, *, csv_path: Path, rejected_path: Path | None = None) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    result.manifest.to_csv(csv_path, index=False, encoding="utf-8-sig")
    if rejected_path is not None:
        rejected_path.parent.mkdir(parents=True, exist_ok=True)
        result.rejected.to_csv(rejected_path, index=False, encoding="utf-8-sig")
