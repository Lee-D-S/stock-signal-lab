# 모듈 지도

## 디렉터리별 책임

| 경로 | 책임 | 주요 진입점·파일 |
| --- | --- | --- |
| `main.py` | 운영 앱 부트스트랩 | `main`, `sync_positions`, `run_dashboard` |
| `config.py` | 환경 변수와 매매·LLM 설정 | `Settings`, `settings` |
| `core/api/` | KIS 인증·HTTP 공통 클라이언트 | `auth.py`, `client.py` |
| `core/broker.py` | 매수·매도·잔고 어댑터 | `buy`, `sell`, `get_balance` |
| `core/market_data.py` | 현재가·거래량·업종·OHLCV 조회 | `get_current_price`, `get_ohlcv` |
| `core/guardrails/` | Agent 리스크·준법·주문 초안 검증 | `free_rules.py` |
| `core/agents/` | 규칙 기반 투자 Agent pipeline | `free_base.py`, `free_pipeline.py` |
| `core/llm/` | 로컬 LLM 검토·최종 gate·보고서 | `committee.py`, `local_client.py`, `schemas.py` |
| `models/` | SQLAlchemy async 모델과 세션 | `database.py`, `Position`, `TradeLog`, `NewsCache`, `SectorSignal` |
| `scheduler/` | 시장 시간 판단, 전략 실행, 예약 작업 | `runner.py` |
| `strategies/` | 런타임 전략 플러그인 | `BaseStrategy`, `MACrossStrategy`, 뉴스 전략 |
| `dashboard/` | FastAPI monitoring API | `dashboard/main.py`, 4개 router |
| `notifier/` | Telegram 알림 | `telegram.py` |
| `scripts/screener_lib/` | 지표 계산·DART·유니버스·출력 공통 라이브러리 | `indicators/`, `dart.py`, `data.py` |
| `scripts/discovery/` | 지표 IC와 조건 후보 탐색 | `collector.py`, `analyzer.py`, loaders |
| `scripts/scoring/` | 조건 점수·임계값·가중치 | `scorer.py`, `threshold.py`, `weight_tuner.py` |
| `scripts/backtest/` | OHLCV 기반 백테스트 엔진 | `engine.py`, `portfolio.py`, `metrics.py` |
| `scripts/` 루트 | 리서치 파이프라인과 64개 CLI 진입점의 대부분 | `run_signal_research_pipeline.py` 등 |
| `legacy/research_data/ai 주가 변동 원인 분석/` | 기업 보고서, 이벤트, 패턴, 전략, 관찰 산출물 | `00_기업별분석`~`10_일일요약` |
| `.github/workflows/` | 원격 오전·일일 자동화 | `daily_auto.yml` |

## 운영 애플리케이션 의존 흐름

```text
main.py
 ├─ config.settings
 ├─ models.database.init_db
 ├─ core.broker.get_balance → core.api.client → core.api.auth
 ├─ scheduler.runner.create_scheduler
 └─ dashboard.main.app

scheduler.runner.run_strategy
 ├─ core.market_data
 ├─ strategies.BaseStrategy
 ├─ core.broker.buy/sell
 ├─ models.Position/TradeLog
 └─ notifier.telegram
```

## 리서치 스크립트 계층

### 입력·수집

- `run_daily_universe_refresh.py`: 거래대금 상위 유니버스
- `run_new_company_reports.py`, `quarterly_stock_analysis.py`: 기업별 분기 보고서와 이벤트
- `auto_morning_dart_check.py`: 오전 공시·근일 이벤트
- `screener_lib/dart.py`, `screener_lib/data.py`: DART/KIS 데이터 공통화

### 분석·검증

- `collect_event_patterns.py`: 이벤트 집계와 패턴 축 분석
- `review_hypothesis_events.py`: 가설별 이벤트 검토
- `proxy_backtest_hypotheses.py`, `realistic_backtest_hypotheses.py`: 가설 백테스트
- `run_event_condition_discovery.py`: 이벤트 당일 조건 조합의 train/validation 탐색
- `run_discovery.py`, `run_condition_search.py`, `run_scoring.py`: 지표·조건·점수 탐색

### 운영·관찰

- `generate_watchlist_signals.py`: active 조건 후보 생성
- `recheck_watchlist_flows.py`: 수급 재조회와 후보 확정
- `run_observation_update.py`, `run_observation_tracking_update.py`: 관찰 로그와 D+ 추적
- `run_observation_performance_summary.py`, `run_daily_research_summary.py`: 성과·일일 요약
- `run_foreign_flow_observation.py`, `run_alignment_observation.py`, `run_scoring_observation.py`, `run_ntm_per_observation.py`: 별도 관찰 축

## 전체 Python 파일 커버리지

Trailmark와 AST 스캔에서 146개 Python 파일을 모두 발견했다. `__init__.py` 파일은 대부분 패키지 표식 또는 re-export이며, 실질적인 구현은 다음 그룹에 포함된다.

- 루트 6개: `check_samsung_real.py`, `config.py`, `main.py`, `validate_portfolios_script.py`, `verify_tickers.py` 등
- `core/` 15개
- `dashboard/` 7개
- `models/` 6개
- `notifier/` 2개
- `scheduler/` 2개
- `strategies/` 5개
- `scripts/` 126개: backtest, discovery, scoring, screener_lib와 일회성·운영 CLI 포함

## 설정과 상태 파일

- `.env`, `.env.example`: 자격증명과 운영 설정
- `.token_cache.json`, `.token_cache_real.json`: KIS OAuth 토큰 캐시
- `auto_invest.db`: SQLite 운영 상태
- `data/`: OHLCV/DART 캐시, 포트폴리오 입력·snapshot, Agent 실행 결과
- `legacy/research_data/ai 주가 변동 원인 분석/`: 대규모 연구 문서와 CSV/JSONL 산출물

