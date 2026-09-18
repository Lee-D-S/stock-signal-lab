from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from forecast.data.storage import snapshot_id, write_markdown, write_parquet
from forecast.labels.daily_ground_truth import (
    DEFAULT_OUTPUT_DIR,
    collect_fixed_universe_prices,
    load_fixed_universe,
)


DEFAULT_RAW_START = date(2013, 12, 1)
DEFAULT_TRAIN_START = date(2015, 1, 1)
DEFAULT_TRAIN_END = date(2024, 12, 31)
DEFAULT_LOOKBACK_DAYS = (DEFAULT_TRAIN_START - DEFAULT_RAW_START).days


@dataclass(frozen=True)
class HistoricalPriceArtifacts:
    raw_path: Path
    raw_csv_path: Path
    coverage_path: Path
    summary_path: Path
    errors_path: Path | None
    raw_rows: int
    ticker_count: int
    error_rows: int
    snapshot_id: str


def prepare_training_prices(
    prices: pd.DataFrame,
    universe: pd.DataFrame,
    *,
    raw_start: date,
    raw_end: date,
) -> pd.DataFrame:
    """Filter raw KIS prices and attach fixed-universe metadata."""
    required = {"ticker", "date", "close"}
    missing = required - set(prices.columns)
    if missing:
        raise ValueError(f"Price frame is missing required columns: {sorted(missing)}")

    result = prices.copy()
    result["ticker"] = result["ticker"].astype(str).str.zfill(6)
    result["date"] = pd.to_datetime(result["date"], errors="raise").dt.tz_localize(None)
    result["close"] = pd.to_numeric(result["close"], errors="coerce")
    result = result[result["date"].between(pd.Timestamp(raw_start), pd.Timestamp(raw_end))].copy()
    if result["close"].isna().any() or result["close"].le(0).any():
        raise ValueError("Training prices contain missing or non-positive close values")
    if result.duplicated(["ticker", "date"]).any():
        raise ValueError("Training prices contain duplicate ticker/date rows")

    metadata = universe[[column for column in ("ticker", "name", "market") if column in universe.columns]].copy()
    metadata["ticker"] = metadata["ticker"].astype(str).str.zfill(6)
    result = result.merge(metadata, on="ticker", how="left", validate="many_to_one")
    if result["name"].isna().any() if "name" in result else False:
        raise ValueError("Training prices contain tickers outside the fixed universe")
    result["price_basis"] = "adjusted_close"
    result["price_source"] = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
    columns = [
        "ticker", "name", "market", "date", "open", "high", "low", "close",
        "volume", "turnover", "price_basis", "price_source",
    ]
    return result[[column for column in columns if column in result]].sort_values(["ticker", "date"]).reset_index(drop=True)


def build_price_coverage_report(prices: pd.DataFrame, universe: pd.DataFrame) -> pd.DataFrame:
    """Return one coverage row per fixed-universe ticker."""
    metadata = universe[[column for column in ("ticker", "name", "market") if column in universe.columns]].copy()
    metadata["ticker"] = metadata["ticker"].astype(str).str.zfill(6)
    grouped = prices.groupby("ticker", as_index=False).agg(
        rows=("date", "size"),
        first_date=("date", "min"),
        last_date=("date", "max"),
        unique_dates=("date", "nunique"),
    )
    report = metadata.merge(grouped, on="ticker", how="left")
    report["rows"] = report["rows"].fillna(0).astype(int)
    report["unique_dates"] = report["unique_dates"].fillna(0).astype(int)
    report["coverage_status"] = report["rows"].gt(0).map({True: "ok", False: "missing"})
    return report.sort_values("ticker").reset_index(drop=True)


def write_historical_price_artifacts(
    prices: pd.DataFrame,
    coverage: pd.DataFrame,
    errors: pd.DataFrame,
    *,
    output_dir: Path,
    raw_start: date,
    train_start: date,
    train_end: date,
    snapshot: str,
) -> HistoricalPriceArtifacts:
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_path = output_dir / "raw_prices_train_2013-12-01_2024-12-31.parquet"
    raw_csv_path = output_dir / "raw_prices_train_2013-12-01_2024-12-31.csv"
    coverage_path = output_dir / "raw_prices_train_coverage.csv"
    summary_path = output_dir / "raw_prices_train_collection.md"
    errors_path = output_dir / "raw_prices_train_collection_errors.csv" if not errors.empty else None

    manifest = write_parquet(
        prices,
        raw_path,
        artifact_type="experiment_training_raw_prices",
        schema_version="experiment-training-raw-prices-1",
        as_of=train_end.isoformat(),
        code_version="forecast-training-prices-1",
    )
    prices.to_csv(raw_csv_path, index=False, encoding="utf-8-sig")
    coverage.to_csv(coverage_path, index=False, encoding="utf-8-sig")
    if errors_path is not None:
        errors.to_csv(errors_path, index=False, encoding="utf-8-sig")

    missing_tickers = coverage.loc[coverage["coverage_status"].eq("missing"), "ticker"].tolist()
    write_markdown(
        summary_path,
        "Historical training price collection",
        {
            "Period": (
                f"- raw_start: `{raw_start.isoformat()}`\n"
                f"- train_start: `{train_start.isoformat()}`\n"
                f"- train_end: `{train_end.isoformat()}`\n"
                "- source: KIS adjusted daily OHLCV\n"
                "- 2024-12-31 is included as a cutoff; the last Korean market trading date may be 2024-12-30"
            ),
            "Validation": (
                f"- snapshot_id: `{snapshot}`\n"
                f"- raw_rows: `{len(prices)}`\n"
                f"- tickers: `{prices['ticker'].nunique()}`\n"
                f"- missing_tickers: `{missing_tickers or 'none'}`\n"
                f"- collection_errors: `{len(errors)}`"
            ),
            "Artifacts": (
                f"- raw Parquet: `{manifest.path}`\n"
                f"- raw CSV: `{raw_csv_path}`\n"
                f"- coverage: `{coverage_path}`\n"
                f"- errors: `{errors_path or 'none'}`"
            ),
        },
    )
    return HistoricalPriceArtifacts(
        raw_path=raw_path,
        raw_csv_path=raw_csv_path,
        coverage_path=coverage_path,
        summary_path=summary_path,
        errors_path=errors_path,
        raw_rows=len(prices),
        ticker_count=prices["ticker"].nunique(),
        error_rows=len(errors),
        snapshot_id=snapshot,
    )


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Collect KIS historical training prices for the fixed 50-company universe")
    parser.add_argument("--universe", type=Path, default=Path("data/forecast_experiment/universe_2024-12-31.csv"))
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--raw-start", default=DEFAULT_RAW_START.isoformat())
    parser.add_argument("--train-start", default=DEFAULT_TRAIN_START.isoformat())
    parser.add_argument("--train-end", default=DEFAULT_TRAIN_END.isoformat())
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--window-days", type=int, default=120)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--allow-partial", action="store_true", help="write partial artifacts without failing on API window errors")
    args = parser.parse_args()

    raw_start = date.fromisoformat(args.raw_start)
    train_start = date.fromisoformat(args.train_start)
    train_end = date.fromisoformat(args.train_end)
    universe = load_fixed_universe(args.universe)
    prices, errors = asyncio.run(
        collect_fixed_universe_prices(
            universe,
            start=train_start,
            end=train_end,
            lookback_days=args.lookback_days,
            window_days=args.window_days,
            concurrency=args.concurrency,
            retries=args.retries,
        )
    )
    prices = prepare_training_prices(prices, universe, raw_start=raw_start, raw_end=train_end)
    coverage = build_price_coverage_report(prices, universe)
    snapshot = snapshot_id("kis-adjusted-close-training", raw_start, train_start, train_end, tuple(universe["ticker"]), len(prices))
    artifacts = write_historical_price_artifacts(
        prices,
        coverage,
        errors,
        output_dir=args.output_dir,
        raw_start=raw_start,
        train_start=train_start,
        train_end=train_end,
        snapshot=snapshot,
    )
    missing_tickers = coverage.loc[coverage["coverage_status"].eq("missing")]
    print(json.dumps({
        "raw_prices": str(artifacts.raw_path),
        "raw_csv": str(artifacts.raw_csv_path),
        "coverage": str(artifacts.coverage_path),
        "raw_rows": artifacts.raw_rows,
        "tickers": artifacts.ticker_count,
        "missing_tickers": int(len(missing_tickers)),
        "collection_errors": artifacts.error_rows,
        "snapshot_id": artifacts.snapshot_id,
    }, ensure_ascii=False, indent=2))
    if (artifacts.error_rows or len(missing_tickers)) and not args.allow_partial:
        raise SystemExit("Historical collection is incomplete; inspect the coverage/errors artifacts or rerun with --allow-partial")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
