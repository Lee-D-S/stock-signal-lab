# Historical training prices

The fixed 50-company training-price collector uses the read-only KIS daily chart endpoint. It requests the 2015-01-01 through 2024-12-31 training cutoff and includes a 2013-12-01 warm-up period for 252-trading-day features.

```powershell
rtk python -m forecast.experiment.historical_prices --universe data/forecast_experiment/universe_2024-12-31.csv --output-dir data/forecast_experiment
```

Outputs:

- `raw_prices_train_2013-12-01_2024-12-31.parquet`: canonical raw numeric data
- `raw_prices_train_2013-12-01_2024-12-31.csv`: inspection copy
- `raw_prices_train_coverage.csv`: one coverage row per fixed ticker
- `raw_prices_train_collection.md`: provenance and validation summary
- `raw_prices_train_collection_errors.csv`: only when an API window fails

The collector does not generate features or labels. Those remain separate stages so that point-in-time joins and leakage checks can be tested independently.
