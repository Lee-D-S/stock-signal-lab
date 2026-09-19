# Price-volume model benchmark

Run the current baseline experiment with only the 41 price-volume features:

```powershell
rtk python -m forecast.experiment.price_volume_models --output-dir data/forecast_experiment
```

The runner evaluates a majority baseline, Logistic Regression, Random Forest, and HistGradientBoosting with expanding year-based validation folds from 2020 through 2024. It then fits the selected roster on all 2015-2024 training rows, builds 2025 features from the combined historical and 2025 raw prices, and compares daily predictions with `labels_2025.parquet`.

The output includes candidate evaluations, a selected model roster, saved model bundles, model-level 2025 predictions, an equal-weight ensemble, and summary metrics. No DART, investor-flow, news, or text-derived data is used at this stage.
