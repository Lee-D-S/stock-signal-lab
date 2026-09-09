# Numeric Forecasting Research Lab

`forecast/` is the active, read-only research path for point-in-time numeric stock forecasting.

## Data contract

- Sources: KIS market data and structured OpenDART data.
- Universe: KOSPI/KOSDAQ common-stock candidates with price, liquidity, history, listing, and suspension filters.
- Features: OHLCV, investor flows, financial/valuation numbers, and numeric derivatives only.
- Horizons: T+1, T+5, T+20 trading days.
- Evaluation: date-based walk-forward with horizon purge/embargo.
- Storage: Parquet artifacts plus Markdown summaries. Large artifacts belong in GitHub Actions Artifacts or Release assets.

No order, broker, Telegram, news, keyword, or LLM module is imported by this package.

## Safe smoke run

```powershell
rtk python -m forecast.cli daily --dry-run
rtk python -m unittest discover -s forecast/tests -p "test_*.py"
```

## Online and weekly runs

```powershell
rtk python -m forecast.online_auto --artifact-root forecast_artifacts
rtk python -m forecast.weekly --input data/forecast/labels_YYYY-MM-DD.parquet --artifact-root forecast_weekly_artifacts
```

The online runner calls only read-only KIS market-data endpoints and structured DART endpoints. It requires `KIS_REAL_APP_KEY`, `KIS_REAL_APP_SECRET`, and `DART_API_KEY`. The weekly runner produces candidate model bundles and never updates the production manifest automatically.
