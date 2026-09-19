# Fixed-universe experiment package

`forecast/experiment/` is the additive active path for the fixed-company stock forecasting experiment. It does not import `legacy/` and it does not call order, Telegram, news, or LLM code.

## Data contract

- `universe.py` selects and validates a dated fixed cohort. The default contract is 50 companies selected on `2024-12-31`.
- `contracts.py` builds point-in-time numeric features and a separate long-panel label table.
- `core.py` evaluates candidate classifier/regressor bundles with walk-forward splits and horizon-specific rosters.
- `daily.py` persists trained bundles and creates model-level plus equal-weight ensemble predictions.

The production-shaped artifacts are Parquet for raw/features/labels/predictions, CSV for the daily summary, JSON for roster/evaluation manifests, and optional Excel for weekly comparison tables.

## Example flow

```powershell
rtk python -m forecast.experiment.universe_cli --input data/forecast/candidates.parquet --output data/forecast_experiment/universe_2024-12-31.csv --size 50
rtk python -m forecast.experiment.train_cli --input data/forecast/raw.parquet --model-root data/forecast_experiment/models --top-k 5
rtk python -m forecast.experiment.daily_cli --input data/forecast/raw.parquet --model-root data/forecast_experiment/models --prediction-date 2026-09-17 --feature-asof 2026-09-16
```

### Price-volume baseline experiment

The first fixed-universe benchmark uses the 50-company 2024-12-31 roster, 41 price-volume features, and next-trading-day direction labels. It evaluates four candidates with 2020-2024 expanding folds and writes 2025 model-level plus equal-weight ensemble predictions.

    rtk python -m forecast.experiment.price_volume_models --output-dir data/forecast_experiment

The detailed contract is documented in forecast/experiment/PRICE_VOLUME_MODELS.md; generated evaluation and prediction artifacts are written under data/forecast_experiment/.
The daily runner treats the requested `feature_asof` as the information cutoff. Current-year labels remain `pending` until the year is explicitly included in `complete_years`.

Weekly Excel output is implemented by `reporting.write_weekly_report`; the optional `openpyxl` package must be installed in the runtime used for report generation.
