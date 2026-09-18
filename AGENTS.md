# Repository Guidelines

## Project Structure & Module Organization

`forecast/` is the active numeric forecasting package. It collects read-only KIS/DART data, builds point-in-time numeric features, creates T+1/T+5/T+20 labels, trains pooled scikit-learn candidates, evaluates them with walk-forward splits, and writes prediction artifacts. `core/api/` contains the read-only KIS client used by the forecast universe collector. Historical auto-trading, LLM, and research code is preserved under `legacy/` and must not be imported by active code. `legacy/research_data/ai 주가 변동 원인 분석/` and `data/` are preserved as historical/raw artifacts; they are not direct model inputs.

## Build, Test, and Development Commands

Always prefix commands with `rtk`.

- `rtk python -m forecast.cli --help`: inspect the active forecast CLI.
- `rtk python -m forecast.shadow`: run the offline numeric shadow fixture.
- `rtk python -m forecast.online_auto`: collect read-only KIS/DART data when credentials are available.
- `rtk python -m forecast.weekly`: train and evaluate weekly candidate models from a labelled Parquet input.
- `rtk python scripts/check_docs_sync.py --all --scan-md`: review documentation impact.
- `rtk python legacy/research/scripts/sync_analysis_paths.py --check`: verify preserved historical analysis paths.

## Coding Style & Naming Conventions

Use standard Python style: 4-space indentation, `snake_case` for functions/modules, `PascalCase` for classes, and type hints where practical. Keep active changes inside `forecast/`, `core/api/`, `scripts/`, or the active workflows. Do not import from `legacy/`.

When adding or removing stable historical analysis documents under `legacy/research_data/ai 주가 변동 원인 분석/`, sync `legacy/research/scripts/analysis_paths.py` with `rtk python legacy/research/scripts/sync_analysis_paths.py`. Do not add historical text, news, Gemini output, Telegram output, or broker/order fields to active `forecast/` schemas.

## Testing Guidelines

The active tests are under `forecast/tests/`. Validate changes with the narrowest relevant test first, then run the full forecast test suite. For data-contract changes, also run AST/import checks and the shadow fixture. No dashboard health check or broker/order test is part of the active system.

## Commit & Pull Request Guidelines

Use short, imperative commit messages such as `Add forecast schema guard`. Keep one logical change per commit. PRs should include a brief summary and the commands used to verify the change.

## Git Sync Guard

Before making edits, run `rtk git fetch origin` and `rtk git status --short --branch`. If the current branch is behind `origin/main` and the worktree is clean, run `rtk git pull --rebase origin main` before editing. If the worktree is dirty, do not auto-pull; report the behind/dirty state.

Before pushing, run `rtk git fetch origin` and `rtk git status --short --branch`. Do not push while behind `origin/main`; rebase first when the worktree is clean, or stop and report the required sync when local changes are present.

## Security & Configuration Tips

Copy `.env.example` to `.env` locally and do not commit secrets. Active workflows use read-only market-data access; never invoke KIS order endpoints. Treat `auto_invest.db`, `data/`, `.local/`, token caches, and dependency caches as local artifacts, not source files. Raw and Parquet forecast artifacts belong in GitHub Actions Artifacts or Releases, not Git.
