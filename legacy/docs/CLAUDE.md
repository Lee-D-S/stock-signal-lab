# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

한국투자증권 Open Trading API를 활용한 국내 주식/ETF 자동 매매 시스템. 전략 플러그인 구조로 설계되어 있으며, FastAPI 대시보드와 텔레그램 알림을 제공한다.

## Running the Project

```bash
# Install dependencies
pip install -r requirements.txt

# Run (scheduler + dashboard simultaneously)
python main.py

# Dashboard Swagger UI
# http://localhost:8000/docs

# One-shot utility scripts (run from project root)
python scripts/volume_top10_per.py
```

## Environment Setup

Copy `.env.example` to `.env` and fill in credentials:

```
KIS_APP_KEY=
KIS_APP_SECRET=
KIS_ACCOUNT_NO=       # 8자리 계좌번호 + 상품코드 연결 (예: 5012345601)
KIS_IS_MOCK=true      # Start with mock trading
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
GEMINI_API_KEY=
DASHBOARD_PORT=8000
```

`KIS_IS_MOCK=true` routes all API calls to `openapivts.koreainvestment.com:29443` (mock). Set `false` for live trading at `openapi.koreainvestment.com:9443`.

`KIS_ACCOUNT_NO` format: first 8 digits → `CANO`, remaining digits → `ACNT_PRDT_CD`. Split occurs in `core/broker.py` as `kis_account_no[:8]` / `kis_account_no[8:]`.

## Architecture

`main.py` bootstraps three concurrent components via `asyncio`:
1. **SQLite DB init** (`models/database.py`) — auto-creates `auto_invest.db` in project root
2. **APScheduler** (`scheduler/runner.py`) — runs strategies on intervals during market hours (09:00–15:30 KST weekdays)
3. **FastAPI server** (`dashboard/main.py`) — monitoring API

### Core Flow

```
scheduler/runner.py
  └─ run_strategy(strategy)
       ├─ market_data.get_current_price() / get_ohlcv()   ← core/market_data.py
       ├─ strategy.should_buy() / should_sell()            ← strategies/
       ├─ broker.buy() / broker.sell()                     ← core/broker.py
       ├─ DB write (TradeLog, Position)                    ← models/
       └─ telegram.notify_*()                              ← notifier/telegram.py
```

### Authentication

`core/api/auth.py` maintains a module-level token cache (`_token_cache`). `get_access_token()` auto-refreshes the OAuth2 token 60 seconds before expiry. All API calls go through `core/api/client.py` which injects the auth header.

### KIS API TR ID Pattern

Mock trading uses `V` prefix, live trading uses `T` prefix:
- 매수: `VTTC0011U` (mock) / `TTTC0011U` (live)
- 매도: `VTTC0012U` (mock) / `TTTC0012U` (live)
- 잔고: `VTTC8434R` (mock) / `TTTC8434R` (live)

`core/api/client.py` raises `RuntimeError` when the KIS API returns `rt_cd != "0"`.

### Strategy Plugin Pattern

All strategies implement `strategies/base.py:BaseStrategy`:

```python
class BaseStrategy(ABC):
    name: str
    tickers: list[str]
    enabled: bool = True

    async def should_buy(self, ticker: str, df: pd.DataFrame) -> tuple[bool, str]: ...
    async def should_sell(self, ticker: str, df: pd.DataFrame) -> tuple[bool, str]: ...
    def get_order_quantity(self, price: int, max_amount: int) -> int: ...
```

The `df` passed to strategies is a daily OHLCV DataFrame (60 bars by default, sorted ascending by date) from `market_data.get_ohlcv()`. Columns: `date`, `open`, `high`, `low`, `close`, `volume` — all numeric except `date` (datetime). Uses `pandas_ta` for indicator calculation (e.g., `ta.sma()`).

**To add a new strategy:**
1. Create `strategies/<name>.py` implementing `BaseStrategy`
2. Instantiate and append to `STRATEGIES` list in `scheduler/runner.py`

### Existing Strategies

- `MACrossStrategy` — golden/dead cross on configurable short/long SMA periods via `pandas_ta`
- `NewsSentimentStrategy` — crawls Naver Finance, 한경, MK RSS feeds → Gemini 1.5 Flash for sentiment analysis → stores pending signals in `_pending_signals` class-level dict until the next strategy tick consumes them. **`_pending_signals` is a class variable (shared state)**, not instance-level — signals persist across ticks until consumed by `should_buy`/`should_sell`.

### Database Models

SQLite via SQLAlchemy async (`aiosqlite`):
- `TradeLog` — immutable record of every executed trade (includes `reason` field for AI rationale)
- `Position` — current holdings (upserted on buy with weighted avg price, deleted on sell)
- `NewsCache` — SHA-256 URL hashes to deduplicate crawled articles

### Dashboard API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/trades` | Trade history with date filter |
| GET | `/positions` | Current holdings with avg price |
| GET | `/strategies` | Active strategy list |
| POST | `/strategies/{name}/toggle` | Enable/disable a strategy |
| GET | `/health` | Health check |

## Scheduler Jobs

| Job ID | Schedule | Purpose |
|--------|----------|---------|
| `ma_cross` | Every 5 min | MA Cross strategy tick |
| `news_crawl` | Every N min (config) | Crawl + Gemini analysis |
| `news_sentiment` | Every 5 min | Consume pending AI signals |
| `daily_summary` | Mon–Fri 15:35 | Telegram daily P&L summary |

## Screening Scripts

프로젝트 루트에서 실행. 장 마감 후 또는 장 시작 전 종목 탐색용. 전체 옵션은 `--help` 또는 `CLAUDE.md` 상단 주석 참고.

```bash
# screener.py — 기술적/펀더멘털/밸류에이션 조건 조합 (AND 로직)
# 결과는 scripts/screener/results/screener_YYYY-MM-DD.csv 에 항상 저장 (0건이면 헤더만)
python scripts/screener.py --by marcap --to 300 --ma-align 60,120,240 --rsi-max 50 --obv-rising
python scripts/screener.py --by marcap --per-max 15 --roe-min 10 --ma-align 60,120,240

# ma_alignment.py — 단기 정배열 (MA5>MA20>MA60>MA120)
# 결과는 scripts/screener/results/단기정배열_YYYY-MM-DD.csv 에 항상 저장 (0건이면 matched_count=0 행)
python scripts/ma_alignment.py --by marcap --to 300

# ma_alignment_240.py — 장기 정배열 (MA60>MA120>MA240)
# 결과는 scripts/screener/results/장기정배열_YYYY-MM-DD.csv 에 항상 저장 (0건이면 matched_count=0 행)
python scripts/ma_alignment_240.py --by marcap --to 300

# run_discovery.py — 팩터 리서치: 지표 IC 분석 → 유망 조건 후보 발굴 (FACTOR_RESEARCH_PLAN.md)
# 첫 실행 시 KIS API로 OHLCV 다운로드 후 data/ohlcv_cache/ 에 parquet 캐시 저장
python scripts/run_discovery.py --start 2020-01-01 --end 2022-12-31
python scripts/run_discovery.py --load-records scripts/discovery/results/records.parquet --hold-days 10

# run_scoring.py — 팩터 스코어링: 5개 군 충족률 합산 → 임계값 결정 + 스크리닝 (SCORING_PLAN.md)
# threshold 모드: Train 구간 스코어-수익률 통계 → 권장 임계값 도출
python scripts/run_scoring.py --mode threshold --start 2020-01-01 --end 2022-12-31
python scripts/run_scoring.py --mode threshold --load-raw scripts/scoring/results/raw.parquet
# screen 모드: 유니버스 스코어링 → 임계값 이상 종목 출력. CSV는 결과 없어도 항상 저장
# (OHLCV를 --date 지정 시 그 날짜까지 잘라 계산 → 과거 재현 가능, 기본값=오늘)
python scripts/run_scoring.py --mode screen --threshold 0.70
python scripts/run_scoring.py --mode screen --threshold 0.70 --ic-weights scripts/discovery/results/2020_2022_hold20_ic_ranking.csv
python scripts/run_scoring.py --mode screen --threshold 0.70 --date 2026-05-01

# run_backtest.py — 조건 조합 백테스트: T일 스크리닝 → T+1일 시가 매수 시뮬레이션 (BACKTEST_PLAN.md)
# 첫 실행 시 KIS API로 OHLCV 다운로드 후 data/ohlcv_cache/ 에 parquet 캐시 저장 (discovery 캐시 공유)
# 결과는 scripts/backtest/results/ 에 CSV 저장
python scripts/run_backtest.py --ma-align 60,120,240 --start 2020-01-01 --end 2022-12-31 --hold-days 20
python scripts/run_backtest.py --ma-align 60,120,240 --rsi-max 40 --start 2020-01-01 --end 2022-12-31 --hold-days 10 --stop-loss -0.05 --take-profit 0.10
python scripts/run_backtest.py --macd-cross-up --obv-rising --start 2021-01-01 --end 2023-12-31 --hold-days 15

# run_walkforward.py — Walk-forward 분석: 슬라이딩 윈도우로 전략 시간 일관성 검증 (BACKTEST_PLAN.md 6단계)
# 결과는 scripts/backtest/results/wf_*_{summary,w1_trades,...}.csv 에 저장
python scripts/run_walkforward.py --ma-align 60,120,240 --rsi-max 40 --start 2020-01-01 --end 2024-12-31 --train-years 3 --test-years 1 --hold-days 10
# 생존 편향 제거 (DART 공시 기반 역사적 유니버스, BACKTEST_PLAN.md 8단계)
# 첫 실행 시 DART API + KIS API로 1,500~2,000개 종목 조회 (30분~1시간, 이후 캐시)
# data/dart_universe_cache/{start}_{end}.json 에 캐시됨
python scripts/run_walkforward.py --ma-align 60,120,240 --start 2020-01-01 --end 2024-12-31 --train-years 3 --test-years 1 --historical-universe
python scripts/run_walkforward.py --ma-align 60,120,240 --start 2020-01-01 --end 2024-12-31 --train-years 3 --test-years 1 --historical-universe --max-tickers 500

# run_dca_backtest.py — ETF 시간 기반 DCA 백테스트 (주간 25만원 vs 월간 100만원 vs Buy-and-Hold)
# 결과는 scripts/dca/results/ 에 저장 (매수 이력 CSV + 전략별 요약 dca_summary.csv)
# 기본 기간: KODEX200=2019~2024, TIGER미국S&P500=2021~2024 (티커별 자동 설정)
python scripts/run_dca_backtest.py                                              # 두 ETF 모두 기본 기간
python scripts/run_dca_backtest.py --ticker 069500                              # KODEX 200만
python scripts/run_dca_backtest.py --ticker 360750                              # TIGER 미국S&P500만
python scripts/run_dca_backtest.py --ticker 069500 --start 2020-01-01 --end 2024-12-31  # 기간 직접 지정

# run_dual_momentum_backtest.py — 듀얼 모멘텀 백테스트 (Antonacci 방식, KODEX200 vs TIGER S&P500 vs 현금)
# 매월 12개월 수익률 비교 → 상대 모멘텀 + 절대 모멘텀 (둘 다 음수면 현금)
# 결과는 scripts/dca/results/ 에 저장
python scripts/run_dual_momentum_backtest.py
python scripts/run_dual_momentum_backtest.py --capital 20000000
python scripts/run_dual_momentum_backtest.py --lookback 6

# run_asset_allocation_backtest.py — 정적 자산배분 백테스트 (고정 비중 + 주기적 리밸런싱)
# 기본: KODEX200 50% / TIGER S&P500 50%, 분기 리밸런싱 vs BaH(리밸런싱 없음) 비교
# 결과는 scripts/dca/results/ 에 저장
python scripts/run_asset_allocation_backtest.py
python scripts/run_asset_allocation_backtest.py --capital 20000000
python scripts/run_asset_allocation_backtest.py --rebalance-freq monthly
python scripts/run_asset_allocation_backtest.py --weights 069500=0.6,360750=0.4
```

### screener.py 지표 추가 방법

1. `scripts/screener_lib/indicators/<이름>.py` 생성 — `calculate`, `add_args`, `check`, `condition_labels` 4개 함수 구현
2. `scripts/screener_lib/indicators/__init__.py`의 `INDICATORS` 리스트에 추가
3. 외부 데이터 주입이 필요한 경우(DART처럼) `screener.py`의 주입 로직도 추가

## Key Configuration (`config.py`)

- `max_order_amount` — max KRW per single order (default: 100,000)
- `max_positions` — max concurrent positions (default: 10)
- `news_crawl_interval_min` — news crawl frequency in minutes
- `stop_loss_pct` — 손절 기준 수익률 (default: -0.05, 즉 -5%)
- `take_profit_pct` — 익절 기준 수익률 (default: 0.10, 즉 +10%)

## 주가 변동 원인 분석 프로젝트 (`ai 주가 변동 원인 분석/`)

개별 종목의 주가 변동 원인을 분기별로 분석해 반복 패턴을 찾고, 실전 매매 시그널로 연결하는 리서치 프로젝트. 자동매매 시스템과는 별개로 동작한다. 전체 현황은 `프로젝트_현황_총정리.md` 참고.

### 디렉토리 구조

```
ai 주가 변동 원인 분석/
  00_기업별분석/   # 종목별 분기 보고서 + events.jsonl
  01_기획/        # 종합기획.md, 전략세트 정리
  02_기준/        # 원인후보 분석기준.md
  03_원천데이터/  # 이벤트.csv, 이벤트_분포_요약.md
  04_패턴분석/    # 패턴_분석_*.csv, 패턴_가설_후보.csv
  05_가설검토/    # 가설_이벤트_검토.*, 가설_이벤트_요약.csv
  06_백테스트/    # 대리/실전 백테스트 결과, OHLCV 캐시 보강 결과
  07_전략신호/    # 전략_조건_초안.*, 관심종목_시그널_후보.*, 확정.*, 거래대금_상위_유니버스.*
  08_관찰기록/    # 관찰_로그.*, 관찰_성과_요약.*
  09_조건스냅샷/  # 날짜별 디렉토리 — 가설 검토·백테스트·이벤트 전체 스냅샷
  10_일일요약/    # 일별 운영 요약 리포트 (일일_운영_요약_YYYY-MM-DD.md)
  프로젝트_현황_총정리.md  # 목적·흐름·스크립트 전체 설명
```

### 일일 운영 파이프라인

**통합 실행 (권장):**

```bash
# daily 모드 — 유니버스 갱신 → 시그널 스캔 → 관찰 로그 추가 → 팩터 스코어 스크린/추적 → 정배열 추적
python scripts/run_signal_research_pipeline.py --mode daily

# daily 모드에서 과거 특정 날짜 재실행 (--date 는 scoring/alignment 세 단계 모두에 일관되게 전달됨)
python scripts/run_signal_research_pipeline.py --mode daily --date 2026-05-01

# daily 모드에서 특정 단계 스킵
python scripts/run_signal_research_pipeline.py --mode daily --skip-scoring-observation --skip-alignment-observation

# daily 모드에서 팩터 스코어 임계값 변경 (기본 0.60)
python scripts/run_signal_research_pipeline.py --mode daily --scoring-threshold 0.70

# daily 모드에서 팩터 스코어링 풀 크기 변경 (기본 300)
python scripts/run_signal_research_pipeline.py --mode daily --scoring-pool-size 500

# daily 모드에서 정배열 스캔 풀 크기 변경 (기본 300)
python scripts/run_signal_research_pipeline.py --mode daily --alignment-pool-size 500

# backtest 모드 — 가설 백테스트 → 스냅샷 저장
python scripts/run_signal_research_pipeline.py --mode backtest

# full 모드 — backtest 후 daily까지 한 번에 실행
python scripts/run_signal_research_pipeline.py --mode full
```

**단계별 개별 실행:**

```bash
# 거래대금 상위 유니버스 갱신 (07_전략신호/거래대금_상위_유니버스.*)
python scripts/run_daily_universe_refresh.py

# 신규 기업 보고서 생성 (거래대금 상위 중 보고서 없는 종목 → 00_기업별분석/)
python scripts/run_new_company_reports.py

# 확정 후보 관찰 로그 추가 (07_전략신호 → 08_관찰기록)
python scripts/run_observation_update.py

# 관찰 로그 D+1/D+5/D+10/D+20 수익률 추적 업데이트
python scripts/run_observation_tracking_update.py

# 관찰 성과 집계 (08_관찰기록/관찰_성과_요약.*)
python scripts/run_observation_performance_summary.py

# 신규 스냅샷 조건 관찰 로그 추가 (09_조건스냅샷 → 08_관찰기록)
python scripts/run_new_condition_observation.py

# 외국인 연속 순매수 후보 관찰 기록 및 D+ 추적 (기본 min-streak=2)
python scripts/run_foreign_flow_observation.py --min-streak 2

# 팩터 스코어링 스크린 (시총 상위 N개 유니버스)
python scripts/run_scoring.py --mode screen --by marcap --to 300
python scripts/run_scoring.py --mode screen --by marcap --to 300 --date 2026-05-01

# 팩터 스코어 관찰 기록/추적 — run_scoring.py --mode screen 의 결과를 임계값 이상 누적 (08_관찰기록/)
# screen CSV가 비어 있으면 D+ 추적만 실행하고 정상 종료 (에러 없음)
# --date 를 지정하면 해당 날짜의 screen_{date}.csv 를 읽음
python scripts/run_scoring_observation.py --threshold 0.60
python scripts/run_scoring_observation.py --threshold 0.60 --date 2026-05-01

# 단기(MA5>20>60>120) + 장기(MA60>120>240) 정배열 종목 관찰 기록/추적 (08_관찰기록/)
# --date 를 지정하면 OHLCV를 그 날짜까지 잘라 계산
python scripts/run_alignment_observation.py --pool-size 300
python scripts/run_alignment_observation.py --pool-size 300 --date 2026-05-01

# 외국인 연속 순매수/순매도 빠른 조회 (기본 10일, --days로 변경)
python scripts/foreign_consec_buy.py --days 3

# 일일 리서치 요약 생성 — Gemini 활용 (10_일일요약/)
python scripts/run_daily_research_summary.py

# 신규 스냅샷 조건 검토 (09_조건스냅샷 내 최신 날짜 기준)
python scripts/review_new_snapshot_conditions.py
```

### 조건 임계값 탐색 (`run_condition_search.py`)

```bash
# 단기 조건 검증 (hold_days=1, 5, 10, 20)
python scripts/run_condition_search.py \
    --load-records scripts/discovery/results/records_hold1.parquet \
    --train-end 2022-12-31 --val-end 2024-12-31 --horizon short

# 장기 조건 검증 (hold_days=60 이상)
python scripts/run_condition_search.py \
    --load-records scripts/discovery/results/records_hold60.parquet \
    --train-end 2022-12-31 --val-end 2024-12-31 --horizon long
```

### 초기 분석 파이프라인 (`scripts/*.py`)

전략 조건을 처음 발굴할 때 순서대로 실행한다. `quarterly_stock_analysis.py`는 KIS API 공통 라이브러리로 직접 실행하지 않는다.

```bash
# 1. 기업별 분기 보고서 생성 (00_기업별분석/ → events.jsonl 생성)
python scripts/regenerate_all_quarterly_reports.py

# 2. 이벤트 집계 + 패턴 분석 (03, 04 생성)
python scripts/collect_event_patterns.py

# 3. 가설별 이벤트 리뷰 (05 생성)
python scripts/review_hypothesis_events.py

# 4. OHLCV 캐시 보강 (백테스트 전처리)
python scripts/refresh_hypothesis_ohlcv_cache.py
python scripts/fill_hypothesis_ohlcv_internal_gaps.py

# 5. 백테스트 (06 생성)
python scripts/proxy_backtest_hypotheses.py
python scripts/realistic_backtest_hypotheses.py
python scripts/batch_realistic_backtest_hypotheses.py

# 6. 갭 분류 + 전략 조건 초안 (06, 07 생성)
python scripts/classify_gaps_and_draft_strategy.py

# 7. 관심종목 시그널 생성 (07 생성)
python scripts/generate_watchlist_signals.py

# 8. 수급 재조회 — 15:40 이후에만 실행 가능 (07→08 생성)
python scripts/recheck_watchlist_flows.py
```

### 자동화 스크립트

```bash
# 평일 08:50 자동 실행 — 관찰 종목 DART 공시 확인 후 텔레그램 알림
python scripts/auto_morning_dart_check.py
```

### Windows 주의사항

`aiohttp`를 사용하는 스크립트(`recheck_watchlist_flows.py`, `run_foreign_flow_observation.py` 등)는 Windows에서 `asyncio.WindowsProactorEventLoopPolicy()`를 사용해야 한다. `WindowsSelectorEventLoopPolicy`는 `socket.socketpair()` 오류를 유발한다.

KIS 수급 API(`FHPTJ04160001`)는 `TIME LIMIT 00:00~15:40` 제한이 있어 **15:40 이후에만** 호출 가능하다. 장 중에는 `pending_api_error`가 반환된다.