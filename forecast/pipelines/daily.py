from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from forecast.data.schemas import validate_feature_frame, validate_label_frame
from forecast.data.storage import default_artifact_root, snapshot_id, write_markdown, write_parquet
from forecast.features.builder import build_numeric_features
from forecast.labels.returns import add_return_labels


def run_daily_pipeline(input_path: Path | None = None, *, as_of: str | None = None, artifact_root: Path | None = None, dry_run: bool = False) -> dict[str, object]:
    if dry_run:
        frame = _dry_run_frame()
    elif input_path is None:
        raise ValueError("input_path is required unless --dry-run is used")
    else:
        frame = pd.read_parquet(input_path) if input_path.suffix.lower() == ".parquet" else pd.read_csv(input_path)
    features = build_numeric_features(frame)
    validate_feature_frame(features)
    labelled = add_return_labels(features)
    validate_label_frame(labelled)
    root = artifact_root or default_artifact_root()
    run_date = as_of or str(pd.to_datetime(labelled["date"]).max().date())
    snapshot = snapshot_id(run_date, tuple(labelled.columns), len(labelled))
    if dry_run:
        report_path = root / "dry_run_summary.md"
        write_markdown(report_path, "Forecast daily dry-run", {
            "Result": f"Validated {len(labelled)} rows for {labelled['ticker'].nunique()} tickers.",
            "Snapshot": f"as_of={run_date}; snapshot_id={snapshot}",
            "Labels": "T+1/T+5/T+20 columns generated; immature future rows remain unavailable.",
        })
        return {"rows": len(labelled), "tickers": int(labelled["ticker"].nunique()), "report": str(report_path)}
    feature_manifest = write_parquet(features, root / f"features_{run_date}.parquet", artifact_type="features", as_of=run_date, code_version="forecast-0.1.0")
    label_manifest = write_parquet(labelled, root / f"labels_{run_date}.parquet", artifact_type="labels", as_of=run_date, code_version="forecast-0.1.0")
    return {"rows": len(labelled), "features": feature_manifest, "labels": label_manifest}


def _dry_run_frame() -> pd.DataFrame:
    dates = pd.date_range(end=date.today(), periods=280, freq="B")
    rows = []
    for ticker, base in (("000001", 10_000), ("000002", 20_000)):
        for index, current_date in enumerate(dates):
            close = base + index * 10 + (index % 7) * 3
            rows.append({"ticker": ticker, "date": current_date, "open": close - 5, "high": close + 20, "low": close - 20, "close": close, "volume": 1_000_000 + index * 10, "foreign_net": (index % 5) * 100, "institution_net": (index % 3) * 80, "individual_net": -(index % 4) * 120})
    return pd.DataFrame(rows)
