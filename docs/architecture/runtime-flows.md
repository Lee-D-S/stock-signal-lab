# 실행 흐름

## 1. 운영 앱 시작

```mermaid
sequenceDiagram
  participant O as 운영자
  participant M as main.py
  participant DB as SQLite/SQLAlchemy
  participant B as KIS Broker
  participant S as APScheduler
  participant D as FastAPI

  O->>M: python main.py
  M->>DB: init_db()
  M->>B: get_balance()
  B-->>M: 현재 보유 잔고
  M->>DB: positions 삭제 후 재생성
  M->>S: create_scheduler(); start()
  M->>D: uvicorn Server serve()
```

## 2. 스케줄러의 매매 판단

```mermaid
flowchart TD
  tick[주기적 run_strategy] --> open{시장 운영 시간인가?}
  open -- 아니오 --> skip[실행 건너뜀]
  open -- 예 --> halt{CircuitBreaker 차단?}
  halt -- 예 --> skip
  halt -- 아니오 --> price[현재가 조회]
  price --> ohlcv[OHLCV 60봉 조회]
  ohlcv --> held{보유 포지션인가?}
  held -- 예 --> sell_rules[손절·익절 또는 should_sell]
  sell_rules --> sell{매도 신호?}
  sell -- 예 --> sell_order[broker.sell → TradeLog → Position 삭제 → Telegram]
  sell -- 아니오 --> wait_sell[대기]
  held -- 아니오 --> buy_rules[should_buy]
  buy_rules --> buy{매수 신호?}
  buy -- 아니오 --> wait_buy[대기]
  buy -- 예 --> limits[금액·일별 한도·최대 포지션 검사]
  limits --> order[broker.buy → TradeLog → Position upsert → Telegram]
```

## 3. 스케줄 작업

| Job ID | 주기 | 역할 |
| --- | --- | --- |
| `ma_cross` | 5분 | `MACrossStrategy` 실행 |
| `news_crawl` | 설정값 분 | 뉴스 수집·Gemini 감성 분석 |
| `news_sentiment` | 5분 | 대기 중 뉴스 신호 소비 |
| `sector_crawl` | 10분 | 업종 뉴스 감성 분석 |
| `daily_summary` | 평일 15:35 | 당일 매매 손익 Telegram |
| `sector_validation` | 평일 15:40 | 섹터 예측 적중률 검증 |
| `foreign_flow_observation` | 평일 16:20 | 외국인 수급 관찰 스크립트 실행 |

## 4. 일일 리서치 파이프라인

```mermaid
flowchart LR
  U[거래대금 상위 유니버스] --> R[신규 기업 보고서]
  R --> E[events.jsonl / 이벤트.csv]
  E --> P[패턴 분석]
  P --> H[가설 검토·백테스트]
  H --> A[active 조건·스냅샷]
  A --> W[일별 후보 스캔]
  W --> F[KIS 수급 재조회]
  F --> O[관찰 로그]
  O --> T[D+1/D+5/D+10/D+20 추적]
  T --> G[조건별 성과 요약]
```

`run_signal_research_pipeline.py --mode daily`는 현재 다음 종류의 관찰을 조합한다.

- active 전략 후보
- 외국인 연속 순매수·순매도
- 팩터 스코어
- 단기·장기 이동평균 정배열
- NTM PER/PEG
- 필요 시 캔들·신규 조건 관찰

## 5. Free Agent Committee

```mermaid
flowchart TD
  csv[최근 전략 CSV 또는 CLI 후보] --> quant[QuantSignalAgent]
  quant --> analyst[EquityResearchAnalystAgent]
  analyst --> research[ResearchFileAgent]
  research --> pm[PortfolioManagerAgent]
  pm --> risk[RiskManagerAgent]
  risk --> compliance[ComplianceOfficerAgent]
  compliance --> trader[TraderAgent]
  trader --> ops[OperationsReportAgent]
  ops --> artifacts[JSON·Markdown·manifest]
  trader -.-> blocked[execution_allowed=false]
```

Agent는 종목 코드·리서치 파일·포트폴리오·현금·유동성·섹터 집중도를 검사하고, `buy/sell/hold` 제안과 human checklist를 만든다. 실제 주문은 이 흐름에 포함되지 않는다.

