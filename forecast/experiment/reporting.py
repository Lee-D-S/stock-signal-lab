from __future__ import annotations

from pathlib import Path

import pandas as pd

from forecast.evaluation.metrics import classification_metrics, regression_metrics

from .daily import summarise_predictions


def build_weekly_tables(predictions: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Build human-readable model, company, and horizon comparison tables."""
    model_summary = summarise_predictions(predictions)
    company_rows: list[dict[str, object]] = []
    for (horizon, ticker), frame in predictions.groupby(["horizon", "ticker"], sort=True):
        class_metrics = classification_metrics(frame["actual_direction"], frame["probability_up"])
        reg_metrics = regression_metrics(frame["actual_return"], frame["predicted_return"])
        company_rows.append({
            "horizon": horizon,
            "ticker": ticker,
            "rows": len(frame),
            **class_metrics,
            **reg_metrics,
        })
    company_summary = pd.DataFrame(company_rows)
    horizon_summary = (
        predictions.groupby(["horizon", "candidate"], as_index=False)
        .agg(
            prediction_rows=("ticker", "size"),
            average_probability_up=("probability_up", "mean"),
            average_predicted_return=("predicted_return", "mean"),
            available_rows=("prediction_available", "sum"),
            mature_rows=("maturity_status", lambda values: int(values.eq("mature").sum())),
        )
    )
    return {
        "model_summary": model_summary,
        "company_summary": company_summary,
        "horizon_summary": horizon_summary,
    }


def write_weekly_report(predictions: pd.DataFrame, path: Path) -> Path:
    """Write the weekly comparison workbook when an Excel engine is installed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tables = build_weekly_tables(predictions)
    try:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            for sheet_name, table in tables.items():
                table.to_excel(writer, sheet_name=sheet_name[:31], index=False)
    except ImportError as exc:
        raise RuntimeError(
            "Weekly Excel output requires openpyxl. Install the optional experiment reporting dependency first."
        ) from exc
    return path
