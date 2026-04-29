# Repository Guidelines

## Project Structure & Module Organization
`main.py` starts the app, initializes SQLite, and launches the scheduler and dashboard. Core trading and broker integration live in `core/` and `core/api/`; persistent models are in `models/`; scheduled jobs in `scheduler/`; strategies in `strategies/`; notifications in `notifier/`; and the FastAPI dashboard in `dashboard/`. One-off research scripts live in `scripts/`. The analysis workspace is `ai 주가 변동 원인 분석/`, with `00_기업별분석/` for company reports and numbered folders for shared outputs.

## Build, Test, and Development Commands
Always prefix commands with `rtk`.
- `rtk python main.py`: run the app, scheduler, and dashboard.
- `rtk python -m uvicorn dashboard.main:app --reload`: run API/dashboard only.
- `rtk python scripts/run_backtest.py --help`: inspect backtest options.
- `rtk python scripts/run_discovery.py --help`: inspect discovery options.
- `rtk python -u scripts/run_signal_research_pipeline.py --mode daily --recheck`: run the daily signal pipeline.

## Coding Style & Naming Conventions
Use standard Python style: 4-space indentation, `snake_case` for functions/modules, `PascalCase` for classes, and type hints where practical. Keep changes aligned with nearby code and reuse `config.py`’s `settings` instead of reading environment variables directly. For analysis assets, keep folder names numeric and stable, e.g. `03_원천데이터`, `07_전략신호`, `08_관찰기록`.

## Testing Guidelines
This repo does not have a dedicated `tests/` package yet. Validate changes by running the affected script or endpoint, for example `rtk python scripts/test_gemini.py` or `GET /health` against the dashboard. For research changes, rerun the narrowest relevant pipeline step first, then the full pipeline if outputs changed.

## Commit & Pull Request Guidelines
Use short, imperative commit messages such as `Add signal pipeline path fix`. Keep one logical change per commit. PRs should include a brief summary, the commands used to verify the change, and screenshots or sample output when dashboard or report paths change.

## Security & Configuration Tips
Copy `.env.example` to `.env` locally and do not commit secrets. Use `KIS_IS_MOCK=true` unless you are explicitly testing live trading. Treat `auto_invest.db`, `data/`, and token cache files as local artifacts, not source files.
