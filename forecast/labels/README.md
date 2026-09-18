# Forecast label collection

`daily_ground_truth.py` collects adjusted KIS daily closes for the fixed 50-company universe and builds the completed 2025 daily direction ground truth.

The target label compares each target trading date's adjusted close with the previous available trading date's adjusted close. The first 2025 target uses the final available 2024 close as `feature_asof`.

```powershell
rtk python -m forecast.labels.daily_ground_truth --universe data/forecast_experiment/universe_2024-12-31.csv --output-dir data/forecast_experiment
```

The read-only run writes `raw_prices_2025.parquet`, `labels_2025.parquet`, `labels_2025.csv`, and `labels_2025_collection.md`. API window failures are written to `labels_2025_collection_errors.csv`; the command exits non-zero unless `--allow-partial` is explicitly supplied.
