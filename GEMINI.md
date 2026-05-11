# GEMINI.md

## Project Overview
This project is an **Automated Trading System** for the Korean stock market, utilizing the **Korea Investment & Securities (KIS) Open Trading API**. It is designed with a modular "Strategy Plugin" architecture, allowing for easy addition and testing of various trading algorithms.

The system includes:
- **Core Trading Engine**: Handles OAuth2 authentication, order execution, market data retrieval, and safety features like `SpendLimitGuard` (daily spending caps) and `CircuitBreaker` (stops trading after consecutive losses).
- **Strategy Layer**: Implements specific trading logics (e.g., MA Cross, News Sentiment analysis via Gemini AI). Strategies are registered in `scheduler/runner.py`.
- **Scheduler**: Automates trading ticks during market hours (09:00–15:30 KST) and runs nightly maintenance tasks (Daily Summary, Sector Validation).
- **Dashboard**: A FastAPI-based web interface for monitoring positions, trades, and strategy status.
- **Research Workspace**: An extensive suite of scripts for screening, backtesting, and a specialized "Stock Price Cause Analysis" pipeline (`ai 주가 변동 원인 분석/`).

### Technologies
- **Language**: Python 3.10+ (Async-first)
- **APIs**: KIS Open Trading API, Google Gemini AI (for sentiment), DART (for disclosures).
- **Database**: SQLite (via SQLAlchemy + aiosqlite).
- **Web/UI**: FastAPI, Uvicorn.
- **Data Analysis**: Pandas, Pandas-ta (technical indicators).
- **Tools**: APScheduler (scheduling), Pydantic-settings (config), python-telegram-bot (alerts).

---

## Building and Running

### Prerequisites
1.  Python 3.10 or higher.
2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
3.  Configure Environment:
    - Copy `.env.example` to `.env`.
    - Fill in `KIS_APP_KEY`, `KIS_APP_SECRET`, `KIS_ACCOUNT_NO`, and `GEMINI_API_KEY`.
    - Set `KIS_IS_MOCK=true` for mock trading (highly recommended for initial setup).

### Key Commands
- **Start Full System** (Scheduler + Dashboard):
  ```bash
  python main.py
  ```
- **Run Dashboard Only**:
  ```bash
  python -m uvicorn dashboard.main:app --reload
  ```
- **Daily Signal Pipeline** (Research/Analysis):
  ```bash
  python scripts/run_signal_research_pipeline.py --mode daily
  ```
- **Run Screener**:
  ```bash
  python scripts/screener.py --by marcap --to 300 --ma-align 60,120,240
  ```
- **Backtest a Strategy**:
  ```bash
  python scripts/run_backtest.py --start 2020-01-01 --end 2022-12-31 --hold-days 20
  ```

---

## Project Structure

- `core/`: KIS API clients, broker logic, and market data utilities.
- `strategies/`: Trading strategies implementing the `BaseStrategy` interface.
- `models/`: SQLAlchemy database models (`TradeLog`, `Position`, `NewsCache`).
- `scheduler/`: `APScheduler` job definitions and runner.
- `dashboard/`: FastAPI routers and application for the monitoring UI.
- `scripts/`: Extensive library of research, screening, and backtesting scripts.
- `ai 주가 변동 원인 분석/`: Dedicated research folder for quarterly analysis and pattern discovery.
- `notifier/`: Telegram bot integration for real-time alerts.

---

## Development Conventions

### Coding Style
- **Standard Python**: 4-space indentation, `snake_case` for functions/variables, `PascalCase` for classes.
- **Type Hinting**: Use type hints for function signatures and class members.
- **Asynchrony**: Use `async`/`await` for all I/O bound operations (API calls, DB access).
- **Configuration**: Always use `settings` from `config.py` instead of accessing `os.environ` directly.

### Architecture Guidelines
- **Strategy Plugins**: To add a new strategy, inherit from `BaseStrategy` in `strategies/base.py` and register it in `scheduler/runner.py`.
- **API Limits**: Respect KIS API rate limits. Use the built-in throttling and caching in `core/api/client.py`.
- **Data Persistence**: Trade logs and positions are stored in `auto_invest.db`. Use migrations or direct SQLAlchemy model updates for schema changes.

### Research Workflow
- Research scripts should output results to their respective `results/` subdirectories within `scripts/`.
- Use the `rtk` command prefix if available in your environment (as noted in local documentation).
- **Pipeline Stages**:
    1.  **Universe Refresh**: Update top trading volume tickers (`run_daily_universe_refresh.py`).
    2.  **Report Generation**: Analyze new companies (`run_new_company_reports.py`).
    3.  **Event Extraction**: Collect price change drivers into `이벤트.csv`.
    4.  **Pattern Analysis**: Identify recurring patterns (`collect_event_patterns.py`).
    5.  **Backtesting**: Validate hypotheses (`realistic_backtest_hypotheses.py`).
    6.  **Observation**: Track daily signals in `08_관찰기록` for performance review.
- When modifying analysis paths, run `python scripts/sync_analysis_paths.py` to keep path definitions synchronized.

---

## Automation (GitHub Actions)
The project uses GitHub Actions (`.github/workflows/daily_auto.yml`) for automated daily routines:
- **08:50 KST**: Morning DART disclosure check (`auto_morning_dart_check.py`).
- **16:10 KST**: Daily candidate scanning, observation tracking, and performance summary.

---

## Important Files
- `main.py`: The application entry point.
- `config.py`: Centralized configuration and environment variable management.
- `CLAUDE.md`: Detailed developer guide and command reference (highly recommended).
- `AGENTS.md`: High-level system design and agent-based roles.
- `PLAN/PLAN.md`: Strategic roadmap and implementation phases.
