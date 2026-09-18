# 데이터 모델과 산출물 계보

## 운영 SQLite 모델

| 테이블 | 주요 필드 | 의미 |
| --- | --- | --- |
| `positions` | `ticker`, `quantity`, `avg_price`, `strategy` | 현재 보유 포지션. 매수 시 weighted average upsert, 매도 시 삭제 |
| `trade_logs` | `ticker`, `side`, `quantity`, `price`, `amount`, `strategy`, `reason`, `order_id` | 체결·주문 결과의 append-only 기록 |
| `news_cache` | `url_hash`, `title`, `url`, `analysis` | 뉴스 중복 수집 방지와 분석 캐시 |
| `sector_signals` | `sector_name`, `sentiment`, `confidence`, `actual_change_rate`, `hit` | 뉴스 섹터 예측과 장 마감 후 검증 결과 |

DB는 `models/database.py`의 `create_async_engine`와 `Base.metadata.create_all`로 초기화된다. 마이그레이션 체계는 확인되지 않았고, 스키마 변화는 `create_all`에 의존한다.

## 리서치 파일 계보

```text
KIS/DART/기업 보고서
  → 00_기업별분석/{기업명}/*_events.jsonl
  → 03_원천데이터/이벤트.csv
  → 04_패턴분석/패턴_*.csv, *.md
  → 05_가설검토/가설_*.csv, *.md
  → 06_백테스트/가설_*.csv, *.md
  → 07_전략신호/전략·관심종목·수급·캔들
  → 08_관찰기록/관찰 로그·성과 요약
  → 09_조건스냅샷/YYYY-MM-DD/
  → 10_일일요약/
```

`legacy/research/scripts/analysis_paths.py`가 이 경로를 중앙 관리하며, `sync_analysis_paths.py --check`는 2026-09-09 기준 통과했다.

## 백테스트 데이터 모델

`scripts/backtest/`는 다음을 사용한다.

- `DataFrame`: ticker별 OHLCV와 date 컬럼
- `precompute_indicators`: MA·기술 지표를 T일까지 미리 계산
- `Portfolio`: 현금, 포지션, 거래, 일별 평가액
- `Trade`: 진입·청산 가격, 수량, 청산 사유, gross/net return
- `metrics`: 승률, 평균 수익률, profit factor, total return, CAGR, MDD, Sharpe

백테스트 엔진은 T일 종가에서 조건을 계산하고 T+1일 시가에서 매수하며, 손절·익절·보유기간 만료를 시가 기준으로 처리해 look-ahead bias를 줄이는 구조다.

## Agent 데이터 모델

`core/agents/free_base.py`의 핵심 객체는 다음과 같다.

- `Candidate`: 종목·시그널·점수·현재가·거래대금
- `PortfolioPosition`: 수량·평균가·현재가·섹터와 market value
- `AgentContext`: 기준일, 후보, 포트폴리오, 현금, 입력 경로와 설정
- `AgentResult`: Agent별 상태, 신호, 경고, human checks, artifacts
- `OrderProposal`: buy/sell/hold, 금액·수량·리스크·준법 상태, 실행 허용 여부

기본 실행 결과는 `data/agent_runs/YYYY-MM-DD/<run_id>/`에 Agent별 JSON, 최종 Markdown, Telegram 요약, `pipeline_manifest.json`으로 저장된다.

