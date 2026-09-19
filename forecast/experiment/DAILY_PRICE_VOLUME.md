# Daily v2 price-volume prediction

The active daily runner uses the v2 price-volume roster selected from the 2020-2024 walk-forward evaluation. It runs every roster candidate separately and also writes an equal-weight ensemble. The baseline candidate is retained as a comparison model.

## Information cutoff

`feature_asof` is the last date whose OHLCV data may be used. `prediction_date` must be later than `feature_asof`; for a prediction made on 2026-09-17, pass `feature_asof=2026-09-16`. The raw input must include enough warm-up history to calculate the 252-day features, but rows after `feature_asof` are ignored while building prediction features.

## Create a daily prediction

```powershell
rtk python -m forecast.experiment.daily_price_volume_cli predict `
  --raw data/forecast_experiment/raw_prices_through_2026-09-16.parquet `
  --train data/forecast_experiment/train_price_volume_1d_2015_2024.parquet `
  --model-root data/forecast_experiment/models_price_volume_2025 `
  --roster data/forecast_experiment/price_volume_model_roster.json `
  --prediction-date 2026-09-17 `
  --feature-asof 2026-09-16 `
  --artifact-root data/forecast_experiment/daily_price_volume
```

The output contains 50 rows per roster candidate plus 50 ensemble rows. If the raw input ends at `feature_asof`, realized labels remain `pending`.

## Score after the next trading day

When the raw history contains the next trading day, attach realized labels and refresh the cumulative scorecard:

```powershell
rtk python -m forecast.experiment.daily_price_volume_cli score `
  --predictions data/forecast_experiment/daily_price_volume/daily_price_volume_predictions_2026-09-16.parquet `
  --raw data/forecast_experiment/raw_prices_through_2026-09-17.parquet `
  --artifact-root data/forecast_experiment/daily_price_volume
```

The runner writes model-level predictions, a per-day summary, and `daily_price_volume_scorecard.csv`. The scorecard reports balanced accuracy, ROC-AUC, Brier score, and log loss separately for each candidate and the ensemble.
