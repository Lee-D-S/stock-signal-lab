from __future__ import annotations

import argparse
import asyncio
from datetime import date
from pathlib import Path

import pandas as pd

from forecast.data.collect import collect_dart_financials, collect_kis_history
from forecast.data.storage import default_artifact_root, snapshot_id, write_markdown, write_parquet
from forecast.features.builder import build_numeric_features
from forecast.labels.returns import add_return_labels
from forecast.universe.point_in_time import build_point_in_time_universe


def collect_online(universe_path: Path, *, as_of: str | None = None, artifact_root: Path | None = None) -> dict[str, object]:
    universe = pd.read_parquet(universe_path) if universe_path.suffix.lower() == ".parquet" else pd.read_csv(universe_path)
    reference_date = pd.Timestamp(as_of or date.today()).normalize()
    accepted, rejected = build_point_in_time_universe(universe, as_of=reference_date)
    tickers = accepted["ticker"].astype(str).tolist()
    corp_codes = {
        str(row.ticker): str(row.corp_code)
        for row in accepted.itertuples()
        if hasattr(row, "corp_code") and pd.notna(row.corp_code) and str(row.corp_code).strip()
    }
    market = asyncio.run(collect_kis_history(tickers, end=reference_date.date()))
    financials = asyncio.run(collect_dart_financials(corp_codes, year=reference_date.year - 1)) if corp_codes else pd.DataFrame()
    raw = _merge_numeric_financials(market, financials)
    root = artifact_root or default_artifact_root()
    run_date = reference_date.date().isoformat()
    snapshot = snapshot_id(run_date, tuple(raw.columns), len(raw))
    raw_manifest = write_parquet(raw, root / f"raw_{run_date}.parquet", artifact_type="raw_numeric", as_of=run_date, code_version="forecast-0.1.0")
    feature_frame = build_numeric_features(raw)
    label_frame = add_return_labels(feature_frame)
    feature_manifest = write_parquet(feature_frame, root / f"features_{run_date}.parquet", artifact_type="features", as_of=run_date, code_version="forecast-0.1.0")
    label_manifest = write_parquet(label_frame, root / f"labels_{run_date}.parquet", artifact_type="labels", as_of=run_date, code_version="forecast-0.1.0")
    write_parquet(rejected, root / f"universe_rejected_{run_date}.parquet", artifact_type="universe_rejected", as_of=run_date, code_version="forecast-0.1.0")
    write_markdown(root / f"collection_{run_date}.md", "Forecast online collection", {
        "Run": f"- as_of: `{run_date}`\n- snapshot_id: `{snapshot}`\n- accepted_tickers: `{len(accepted)}`\n- rejected_tickers: `{len(rejected)}`",
        "Artifacts": f"- raw: `{raw_manifest.path}`\n- features: `{feature_manifest.path}`\n- labels: `{label_manifest.path}`",
        "Source policy": "KIS market data and structured DART numbers only; no order, news, Telegram, or LLM calls.",
    })
    return {"as_of": run_date, "snapshot_id": snapshot, "tickers": len(accepted), "rows": len(raw)}


def _merge_numeric_financials(market: pd.DataFrame, financials: pd.DataFrame) -> pd.DataFrame:
    if market.empty or financials.empty:
        return market
    pivot = financials.pivot_table(index=["ticker", "public_at"], columns="account_nm", values="current_amount", aggfunc="last").reset_index()
    rename = {
        "매출액": "revenue",
        "수익(매출액)": "revenue",
        "영업이익": "operating_income",
        "당기순이익": "net_income",
        "자산총계": "assets",
        "자본총계": "equity",
    }
    pivot = pivot.rename(columns=rename)
    numeric_columns = [column for column in rename.values() if column in pivot]
    for column in numeric_columns:
        pivot[column] = pd.to_numeric(pivot[column], errors="coerce")
    result = market.copy()
    result["public_at"] = pd.NaT
    for ticker, rows in pivot.groupby("ticker", sort=False):
        mask = result["ticker"].eq(ticker)
        if not mask.any():
            continue
        latest = rows.sort_values("public_at").iloc[-1]
        public_at = pd.Timestamp(latest["public_at"])
        eligible = mask & (pd.to_datetime(result["date"]) >= public_at)
        result.loc[eligible, "public_at"] = public_at
        for column in numeric_columns:
            result.loc[eligible, column] = latest[column]
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect KIS/DART numeric data for the forecast pipeline")
    parser.add_argument("--universe", type=Path, required=True)
    parser.add_argument("--as-of")
    parser.add_argument("--artifact-root", type=Path)
    args = parser.parse_args()
    print(collect_online(args.universe, as_of=args.as_of, artifact_root=args.artifact_root))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
