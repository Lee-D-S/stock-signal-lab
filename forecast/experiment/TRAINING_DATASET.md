# 2015-2024 training dataset

Build the first price-volume training dataset from the collected KIS raw prices:

```powershell
rtk python -m forecast.experiment.training_dataset --raw data/forecast_experiment/raw_prices_train_2013-12-01_2024-12-31.parquet --output-dir data/forecast_experiment
```

The builder calculates features from the full warm-up history, then filters feature rows to 2015-2024. Labels use the next available trading date and are kept only when `target_end <= 2024-12-31`. It joins features and labels on `ticker + feature_asof` and rejects rows whose target is not after the feature cutoff.

Outputs:

- `features_price_volume_2015_2024.parquet`: price-volume features, including readiness flags
- `labels_1d_2015_2024.parquet`: mature next-trading-day labels
- `train_price_volume_1d_2015_2024.parquet`: canonical joined training data
- `train_price_volume_1d_2015_2024.csv`: inspection copy
- `training_dataset_2015_2024.md`: schema, counts, and feature list

The v2 price-volume schema adds nine local OHLCV-derived features without new API calls: gap_return, open_close_return, candle_body_pct, upper_wick_pct, lower_wick_pct, close_position_20d, close_position_60d, distance_to_high_20d, and distance_from_low_20d.

The first model dataset intentionally excludes investor flows, DART fundamentals, valuation, news, and text-derived fields. Those will be added as separate feature groups after this baseline is validated.
