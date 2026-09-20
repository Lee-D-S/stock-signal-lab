# Numeric Forecasting Research Lab

> A reproducible, point-in-time stock-direction forecasting lab for a fixed Korean equity universe.

## Portfolio summary

This branch is the active forecasting experiment environment. It answers a focused question:

> Given only information that was available before a trading day, can a model predict whether each stock will rise or fall on the next trading day?

The project deliberately separates research from execution. It collects read-only market and corporate data, builds point-in-time features, evaluates multiple candidate models with walk-forward validation, and runs the selected roster every day. It does not place orders.

| Area | Current design |
| --- | --- |
| Universe | 50 Korean listed companies fixed as of 2024-12-31 |
| Historical input | KIS OHLCV data, with structured OpenDART data where applicable |
| Target | Next-trading-day up/down direction |
| Holdout | 2025 calendar-year labels, kept separate from model selection |
| Daily operation | Predict from the latest permitted cutoff and score after the next trading day is known |
| Active baseline | Price-volume v2 feature set with Random Forest as the operational baseline |
| Execution policy | Read-only research; no broker/order endpoint is imported by active forecast code |

## Why this is a useful experiment

The main engineering challenge is not simply fitting a classifier. It is preserving the information boundary so that an evaluation result means what it appears to mean.

- The company roster is fixed before the 2025 evaluation period.
- Features for a prediction date are generated only from data available by that date.
- Rolling features use an explicit warm-up period and do not use future rows.
- Labels are defined separately from features and are joined by stock and trading date.
- Model selection uses expanding walk-forward folds rather than a random train/test split.
- Experiments have stable IDs, compact log rows, detailed records, and feature snapshots so that a result can be reproduced and compared later.
- The daily runner records predictions first and evaluates them only after the following trading-day label becomes available.

## System flow

```text
KIS / OpenDART (read-only)
        |
        v
Fixed 50-company universe
        |
        v
Raw OHLCV -> point-in-time features -> next-day labels
        |
        v
Walk-forward validation + 2025 holdout evaluation
        |
        v
Selected model roster + ensemble
        |
        v
Daily prediction -> next-day scorecard
```

The active package is intentionally numeric. Historical trading, news, Telegram, LLM, and broad research artifacts are preserved under `legacy/` and are not active model inputs.

## Current experiment evidence

Balanced accuracy is reported per validation year; the selection policy prioritizes the worst validation year rather than hiding variation behind a single average. The table below records the current tracked experiments.

| Experiment | Change | 2025 Random Forest balanced accuracy | Decision |
| --- | --- | ---: | --- |
| EXP-000001 | Initial price-volume baseline, 41 features | 0.521464 | Superseded |
| EXP-000002 | OHLC price-shape features, 50 features | 0.524145 | Active baseline |
| EXP-000003 | Volume-price interaction features, 58 features | 0.521943 | Held out |
| EXP-000004 | Market and market-relative return features, 58 features | 0.519979 | Held out |

These figures are experiment evidence, not a claim of investable performance. The goal is to create a disciplined, repeatable comparison framework and then measure whether each new feature or model adds out-of-sample value.

## Repository map

| Path | Purpose |
| --- | --- |
| `forecast/` | Active numeric forecasting package and CLI |
| `forecast/experiment/` | Fixed-universe dataset, feature, model, experiment, and daily-runner code |
| `forecast/experiment/EXPERIMENT_LOG.md` | One-row-per-experiment index |
| `forecast/experiment/experiments/` | Detailed records and artifacts grouped by experiment ID |
| `forecast/labels/` | Label-generation workflow and contracts |
| `core/api/` | Read-only KIS client used by the active universe/data collectors |
| `legacy/` | Preserved historical auto-trading, research, LLM, and raw artifacts; not imported by active forecast code |
| `data/forecast/` | Active generated forecast artifacts; local/runtime data is not source code |

Useful entry-point documents:

- [Forecast package guide](forecast/README.md)
- [Fixed-company experiment guide](forecast/experiment/README.md)
- [Experiment index](forecast/experiment/EXPERIMENT_LOG.md)
- [Daily prediction and cutoff contract](forecast/experiment/DAILY_PRICE_VOLUME.md)
- [Active baseline record](forecast/experiment/experiments/EXP-000002/README.md)

## Reproduce the active workflow

Run commands from the repository root. The project uses the `rtk` command wrapper in this workspace; plain `python` is equivalent when running outside the configured environment.

```powershell
rtk python -m unittest discover -s forecast/tests -p "test_*.py"
rtk python -m unittest discover -s forecast/experiment/tests -p "test_*.py"
rtk python -m forecast.cli --help
rtk python -m forecast.cli daily --dry-run
```

With the required local credentials and data configuration, the read-only collection and weekly evaluation entry points are:

```powershell
rtk python -m forecast.online_auto --artifact-root forecast_artifacts
rtk python -m forecast.weekly --input <labelled-parquet-path>
```

Do not enable order endpoints for this branch. Generated Parquet, CSV, JSON, and Excel artifacts belong in the configured local artifact location rather than in Git.

## Limitations and next research questions

- The fixed roster is intentionally controlled for this experiment, but it does not represent a historical index membership universe and can contain survivorship bias.
- The first evaluation year is 2025; future work should preserve the holdout and add later rolling evaluation periods without tuning on them prematurely.
- The current feature family is price and volume based. Dated sector, fundamentals, macro, and other data should be added only with an explicit as-of-date contract.
- The signal is binary direction, so balanced accuracy alone does not describe economic value. Future reports should add calibration, turnover, transaction-cost assumptions, and portfolio-level analysis.
- A model that wins one holdout period is not automatically a production or investment recommendation.

## Portfolio takeaway

This project demonstrates a complete experimental loop: define a fixed universe, collect reproducible data, enforce a point-in-time boundary, generate features and labels, compare models with walk-forward validation, preserve rejected ideas, and operate the selected roster with delayed scoring. The emphasis is on avoiding leakage and making every result auditable.
