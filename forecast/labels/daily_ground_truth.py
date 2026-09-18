from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from forecast.data.kis import KISDataClient
from forecast.data.storage import snapshot_id, write_markdown, write_parquet


PRICE_ENDPOINT = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
DEFAULT_UNIVERSE = Path("data/forecast_experiment/universe_2024-12-31.csv")
DEFAULT_OUTPUT_DIR = Path("data/forecast_experiment")
DEFAULT_TARGET_START = date(2025, 1, 1)
DEFAULT_TARGET_END = date(2025, 12, 31)


@dataclass(frozen=True)
class GroundTruthArtifacts:
    raw_prices_path: Path
    labels_path: Path
    labels_csv_path: Path
    summary_path: Path
    errors_path: Path | None
    raw_rows: int
    label_rows: int
    mature_rows: int
    error_rows: int
    snapshot_id: str


def load_fixed_universe(path: Path, *, expected_size: int = 50) -> pd.DataFrame:
    """Load and validate the dated fixed-company universe."""
    universe = pd.read_csv(path, dtype={"ticker": "string"})
    required = {"ticker", "name"}
    missing = required - set(universe.columns)
    if missing:
        raise ValueError(f"Universe is missing required columns: {sorted(missing)}")
    if "selected" in universe.columns:
        selected = universe["selected"].astype(str).str.lower().isin({"true", "1", "yes"})
        universe = universe[selected].copy()
    universe["ticker"] = universe["ticker"].astype(str).str.replace(r"\.0$", "", regex=True).str.zfill(6)
    if universe["ticker"].duplicated().any():
        raise ValueError("Fixed universe contains duplicate tickers")
    if len(universe) != expected_size:
        raise ValueError(f"Expected {expected_size} selected companies, found {len(universe)}")
    return universe.reset_index(drop=True)


def _date_windows(start: date, end: date, *, window_days: int) -> list[tuple[date, date]]:
    if start > end:
        raise ValueError("start must not be after end")
    if window_days < 1:
        raise ValueError("window_days must be positive")
    windows: list[tuple[date, date]] = []
    current = start
    while current <= end:
        window_end = min(end, current + timedelta(days=window_days - 1))
        windows.append((current, window_end))
        current = window_end + timedelta(days=1)
    return windows


async def collect_fixed_universe_prices(
    universe: pd.DataFrame,
    *,
    start: date,
    end: date,
    lookback_days: int = 15,
    window_days: int = 90,
    concurrency: int = 4,
    retries: int = 2,
    client: KISDataClient | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Collect adjusted KIS daily prices for the fixed universe.

    KIS daily chart responses are bounded, so the requested period is split into
    calendar windows. The lookback supplies the prior trading-day close needed
    for the first 2025 target date.
    """
    if concurrency < 1:
        raise ValueError("concurrency must be positive")
    if retries < 0:
        raise ValueError("retries must not be negative")

    request_client = client or KISDataClient()
    collection_start = start - timedelta(days=lookback_days)
    windows = _date_windows(collection_start, end, window_days=window_days)
    semaphore = asyncio.Semaphore(concurrency)

    async def fetch_window(ticker: str, window_start: date, window_end: date) -> tuple[pd.DataFrame, dict[str, Any] | None]:
        last_error: Exception | None = None
        for attempt in range(retries + 1):
            try:
                async with semaphore:
                    frame = await request_client.fetch_ohlcv(
                        ticker,
                        start=window_start,
                        end=window_end,
                        count=200,
                    )
                if frame.empty:
                    return frame, {
                        "ticker": ticker,
                        "window_start": window_start.isoformat(),
                        "window_end": window_end.isoformat(),
                        "attempts": attempt + 1,
                        "error": "empty_response",
                    }
                return frame, None
            except Exception as exc:  # noqa: BLE001 - preserve per-window failure details
                last_error = exc
                if attempt < retries:
                    await asyncio.sleep(0.5 * (attempt + 1))
        return pd.DataFrame(), {
            "ticker": ticker,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "attempts": retries + 1,
            "error": f"{type(last_error).__name__}: {last_error}",
        }

    async def fetch_ticker(ticker: str) -> tuple[list[pd.DataFrame], list[dict[str, Any]]]:
        frames: list[pd.DataFrame] = []
        errors: list[dict[str, Any]] = []
        for window_start, window_end in windows:
            frame, error = await fetch_window(ticker, window_start, window_end)
            if not frame.empty:
                frames.append(frame)
            if error is not None:
                errors.append(error)
        return frames, errors

    results = await asyncio.gather(*(fetch_ticker(str(ticker)) for ticker in universe["ticker"]))
    frames = [frame for ticker_frames, _ in results for frame in ticker_frames]
    errors = [error for _, ticker_errors in results for error in ticker_errors]
    if frames:
        prices = pd.concat(frames, ignore_index=True)
        prices["ticker"] = prices["ticker"].astype(str).str.zfill(6)
        prices["date"] = pd.to_datetime(prices["date"], errors="raise").dt.tz_localize(None)
        prices = (
            prices[(prices["date"] >= pd.Timestamp(collection_start)) & (prices["date"] <= pd.Timestamp(end))]
            .drop_duplicates(["ticker", "date"], keep="last")
            .sort_values(["ticker", "date"])
            .reset_index(drop=True)
        )
    else:
        prices = pd.DataFrame()
    return prices, pd.DataFrame(errors, columns=["ticker", "window_start", "window_end", "attempts", "error"])


def build_daily_direction_labels(
    prices: pd.DataFrame,
    universe: pd.DataFrame,
    *,
    target_start: date,
    target_end: date,
    snapshot: str,
) -> pd.DataFrame:
    """Build one target-date row per fixed company and market trading date."""
    required = {"ticker", "date", "close"}
    missing = required - set(prices.columns)
    if missing:
        raise ValueError(f"Price frame is missing required columns: {sorted(missing)}")
    if prices.empty:
        raise ValueError("Cannot build labels from an empty price frame")

    market = prices.copy()
    market["ticker"] = market["ticker"].astype(str).str.zfill(6)
    market["date"] = pd.to_datetime(market["date"], errors="raise").dt.tz_localize(None)
    market["close"] = pd.to_numeric(market["close"], errors="coerce")
    market = market[market["close"].gt(0)].drop_duplicates(["ticker", "date"], keep="last")
    market = market.sort_values(["ticker", "date"]).reset_index(drop=True)

    target_dates = sorted(
        market.loc[
            market["date"].between(pd.Timestamp(target_start), pd.Timestamp(target_end)),
            "date",
        ].unique()
    )
    if not target_dates:
        raise ValueError("No market dates fall inside the target period")

    universe_columns = [column for column in ("ticker", "name", "market") if column in universe.columns]
    fixed = universe[universe_columns].copy()
    fixed["ticker"] = fixed["ticker"].astype(str).str.zfill(6)
    fixed["_join_key"] = 1
    dates = pd.DataFrame({"target_date": pd.to_datetime(target_dates)})
    dates["_join_key"] = 1
    grid = fixed.merge(dates, on="_join_key", how="inner").drop(columns="_join_key")

    rows: list[pd.DataFrame] = []
    for ticker, ticker_grid in grid.groupby("ticker", sort=False):
        history = market[market["ticker"].eq(ticker)][["date", "close"]].sort_values("date")
        target = ticker_grid.sort_values("target_date").copy()
        prior = pd.merge_asof(
            target[["target_date"]],
            history.rename(columns={"date": "feature_asof", "close": "close_asof"}),
            left_on="target_date",
            right_on="feature_asof",
            direction="backward",
            allow_exact_matches=False,
        )
        exact = history.rename(columns={"date": "target_date", "close": "target_close"})
        result = target.merge(prior, on="target_date", how="left").merge(exact, on="target_date", how="left")
        result["future_return_1d"] = result["target_close"] / result["close_asof"] - 1
        result["direction_1d"] = pd.Series(pd.NA, index=result.index, dtype="Int64")
        mature = result["target_close"].notna() & result["close_asof"].notna()
        result.loc[mature, "direction_1d"] = (result.loc[mature, "future_return_1d"] > 0).astype("int8")
        result["maturity_status"] = "mature"
        result.loc[result["target_close"].isna(), "maturity_status"] = "missing_price"
        result.loc[result["target_close"].notna() & result["close_asof"].isna(), "maturity_status"] = "missing_prior_close"
        rows.append(result)

    labels = pd.concat(rows, ignore_index=True)
    labels["price_basis"] = "adjusted_close"
    labels["price_source"] = PRICE_ENDPOINT
    labels["snapshot_id"] = snapshot
    columns = [
        "ticker", "name", "market", "feature_asof", "target_date", "close_asof", "target_close",
        "future_return_1d", "direction_1d", "maturity_status", "price_basis", "price_source", "snapshot_id",
    ]
    return labels[columns].sort_values(["ticker", "target_date"]).reset_index(drop=True)


def write_ground_truth_artifacts(
    prices: pd.DataFrame,
    labels: pd.DataFrame,
    errors: pd.DataFrame,
    *,
    output_dir: Path,
    target_start: date,
    target_end: date,
    snapshot: str,
) -> GroundTruthArtifacts:
    output_dir.mkdir(parents=True, exist_ok=True)
    raw_path = output_dir / "raw_prices_2025.parquet"
    labels_path = output_dir / "labels_2025.parquet"
    labels_csv_path = output_dir / "labels_2025.csv"
    summary_path = output_dir / "labels_2025_collection.md"
    errors_path = output_dir / "labels_2025_collection_errors.csv" if not errors.empty else None

    raw_manifest = write_parquet(
        prices,
        raw_path,
        artifact_type="experiment_raw_prices",
        schema_version="experiment-raw-prices-1",
        as_of=target_end.isoformat(),
        code_version="forecast-ground-truth-1",
    )
    label_manifest = write_parquet(
        labels,
        labels_path,
        artifact_type="experiment_ground_truth_labels",
        schema_version="experiment-ground-truth-labels-1",
        as_of=target_end.isoformat(),
        code_version="forecast-ground-truth-1",
    )
    labels.to_csv(labels_csv_path, index=False, encoding="utf-8-sig")
    if errors_path is not None:
        errors.to_csv(errors_path, index=False, encoding="utf-8-sig")

    mature_rows = int(labels["maturity_status"].eq("mature").sum())
    write_markdown(
        summary_path,
        "2025 fixed-universe direction labels",
        {
            "Period": (
                f"- target_start: `{target_start.isoformat()}`\n"
                f"- target_end: `{target_end.isoformat()}`\n"
                "- comparison: target-date adjusted close vs previous available trading-date adjusted close\n"
                "- unchanged close: `direction_1d=0`"
            ),
            "Collection": (
                f"- snapshot_id: `{snapshot}`\n"
                f"- raw_rows: `{len(prices)}`\n"
                f"- label_rows: `{len(labels)}`\n"
                f"- mature_rows: `{mature_rows}`\n"
                f"- collection_errors: `{len(errors)}`"
            ),
            "Artifacts": (
                f"- raw prices: `{raw_manifest.path}`\n"
                f"- labels Parquet: `{label_manifest.path}`\n"
                f"- labels CSV: `{labels_csv_path}`\n"
                f"- errors: `{errors_path or 'none'}`"
            ),
        },
    )
    return GroundTruthArtifacts(
        raw_prices_path=raw_path,
        labels_path=labels_path,
        labels_csv_path=labels_csv_path,
        summary_path=summary_path,
        errors_path=errors_path,
        raw_rows=len(prices),
        label_rows=len(labels),
        mature_rows=mature_rows,
        error_rows=len(errors),
        snapshot_id=snapshot,
    )


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Collect 2025 KIS direction labels for the fixed 50-company universe")
    parser.add_argument("--universe", type=Path, default=DEFAULT_UNIVERSE)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--target-start", default=DEFAULT_TARGET_START.isoformat())
    parser.add_argument("--target-end", default=DEFAULT_TARGET_END.isoformat())
    parser.add_argument("--lookback-days", type=int, default=15)
    parser.add_argument("--window-days", type=int, default=90)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--allow-partial", action="store_true", help="write partial artifacts without failing on API window errors")
    args = parser.parse_args()

    target_start = date.fromisoformat(args.target_start)
    target_end = date.fromisoformat(args.target_end)
    universe = load_fixed_universe(args.universe)
    prices, errors = asyncio.run(
        collect_fixed_universe_prices(
            universe,
            start=target_start,
            end=target_end,
            lookback_days=args.lookback_days,
            window_days=args.window_days,
            concurrency=args.concurrency,
            retries=args.retries,
        )
    )
    if prices.empty:
        raise SystemExit("KIS returned no prices; no label artifact was created")
    snapshot = snapshot_id("kis-adjusted-close", target_start, target_end, tuple(universe["ticker"]), len(prices))
    labels = build_daily_direction_labels(
        prices,
        universe,
        target_start=target_start,
        target_end=target_end,
        snapshot=snapshot,
    )
    artifacts = write_ground_truth_artifacts(
        prices,
        labels,
        errors,
        output_dir=args.output_dir,
        target_start=target_start,
        target_end=target_end,
        snapshot=snapshot,
    )
    print(json.dumps({
        "raw_prices": str(artifacts.raw_prices_path),
        "labels": str(artifacts.labels_path),
        "labels_csv": str(artifacts.labels_csv_path),
        "label_rows": artifacts.label_rows,
        "mature_rows": artifacts.mature_rows,
        "collection_errors": artifacts.error_rows,
        "snapshot_id": artifacts.snapshot_id,
    }, ensure_ascii=False, indent=2))
    if artifacts.error_rows and not args.allow_partial:
        raise SystemExit("KIS collection had errors; inspect labels_2025_collection_errors.csv or rerun with --allow-partial")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
