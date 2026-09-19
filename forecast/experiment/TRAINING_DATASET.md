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

EXP-000003 evaluated a v3 price-volume schema that adds eight local price-volume interaction features without new API calls: volume_zscore_20d, volume_zscore_60d, turnover_zscore_20d, turnover_zscore_60d, up_volume_share_20d, down_volume_share_20d, return_volume_interaction_1d, and return_volume_interaction_5d. The v3 schema is preserved as a held-out experiment and is not the active training baseline.

The v4 price-volume schema adds eight local market-relative features without new API calls: market_return_1d, market_return_5d, market_return_20d, market_return_60d, market_relative_return_1d, market_relative_return_5d, market_relative_return_20d, and market_relative_return_60d. Market aggregates are equal-weight means within KOSPI or KOSDAQ across the fixed roster. Sector-relative features require a dated sector classification source and are not included in this experiment.

The first model dataset intentionally excludes investor flows, DART fundamentals, valuation, news, and text-derived fields. Those will be added as separate feature groups after this baseline is validated.
