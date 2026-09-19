# Price-volume model benchmark

Run the current price-volume experiment with the 50 price-volume features:

```powershell
rtk python -m forecast.experiment.price_volume_models --output-dir data/forecast_experiment
```

The runner evaluates a majority baseline, Logistic Regression, Random Forest, and HistGradientBoosting with expanding year-based validation folds from 2020 through 2024. The active feature schema is price-volume-v2, including the local OHLC price-shape features. EXP-000003 and EXP-000004 evaluated additional price-volume and market-relative interaction features separately, but they remain held out because neither experiment improved the active baseline. It then fits the selected roster on all 2015-2024 training rows, builds 2025 features from the combined historical and 2025 raw prices, and compares daily predictions with `labels_2025.parquet`.

The output includes candidate evaluations, a selected model roster, saved model bundles, model-level 2025 predictions, an equal-weight ensemble, and summary metrics.

### Evaluation policy

Candidate selection keeps one row per model and validation year; it does not average the folds. The primary score is each model's worst-year balanced accuracy. Tie-breakers are the number of validation years beating the majority baseline, worst-year ROC-AUC, worst-year Brier score, and worst-year log loss. The report also keeps per-year PR-AUC, calibration error, hit rate, and all per-fold metrics for diagnosis.

Balanced accuracy is the directional selection metric; probability quality is evaluated separately with Brier score and log loss. The 2025 replay remains a final out-of-sample report and is not used to choose the roster or policy. No DART, investor-flow, news, or text-derived data is used at this stage.
