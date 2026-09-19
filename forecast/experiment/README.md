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

The fixed-universe experiment uses the 50-company 2024-12-31 roster, 50 active price-volume features, and next-trading-day direction labels. The active feature schema is price-volume-v2, including local OHLC price-shape features. EXP-000003 and EXP-000004 evaluated additional interaction features separately and held them out because they did not improve the selection baseline. It evaluates four candidates with 2020-2024 expanding folds and writes 2025 model-level plus equal-weight ensemble predictions.

    rtk python -m forecast.experiment.price_volume_models --output-dir data/forecast_experiment

The detailed contract is documented in forecast/experiment/PRICE_VOLUME_MODELS.md; generated evaluation and prediction artifacts are written under data/forecast_experiment/.
The canonical experiment history is maintained in forecast/experiment/EXPERIMENT_LOG.md; register every completed or rejected experiment there with its snapshot, feature schema, model policy, results, conclusion, and Git commit.
The log is an index table only; detailed records live under forecast/experiment/experiments/EXP-000001-style folders, while generated data artifacts are preserved under data/forecast_experiment/experiments/.
Model selection keeps each validation year separate: rank by worst-year balanced accuracy, then the number of years beating the majority baseline, worst-year ROC-AUC, worst-year Brier score, and worst-year log loss; the 2025 output is held out for final reporting.
The daily runner treats the requested `feature_asof` as the information cutoff. Current-year labels remain `pending` until the year is explicitly included in `complete_years`.

The active v2 price-volume daily runner is documented in DAILY_PRICE_VOLUME.md. It predicts each roster candidate plus an ensemble using only raw rows on or before feature_asof, then scores saved predictions after the next trading day is available.

Weekly Excel output is implemented by `reporting.write_weekly_report`; the optional `openpyxl` package must be installed in the runtime used for report generation.
