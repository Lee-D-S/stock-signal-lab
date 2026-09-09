# C4 컨테이너 구조

```mermaid
C4Container
  title Auto Invest & Stock Research Platform - Containers

  Person(operator, "운영자/연구자", "전략·리서치 결과를 확인")

  System_Boundary(platform, "Auto Invest & Stock Research Platform") {
    Container(runtime, "Trading Runtime", "Python asyncio", "앱 시작, 포지션 동기화, 스케줄러·대시보드 실행")
    Container(api_adapter, "KIS API Adapter", "httpx + OAuth2", "인증, 토큰 캐시, 시세·잔고·주문 요청")
    Container(strategy_engine, "Strategy Engine", "Python strategy plugins", "OHLCV 기반 MA 전략과 뉴스·섹터 전략 실행")
    Container(scheduler, "Scheduler", "APScheduler", "시장 시간·주기·장 마감 작업 관리")
    Container(dashboard, "Monitoring API", "FastAPI", "health, trades, positions, strategies, market API")
    ContainerDb(db, "Operational Database", "SQLite + SQLAlchemy async", "positions, trade_logs, news_cache, sector_signals")
    Container(research_cli, "Research CLI Pipeline", "Python scripts + pandas", "유니버스, 이벤트, 패턴, 조건, 백테스트, 관찰")
    Container(agent, "Free Agent Committee", "Python rules + optional Ollama", "후보·리서치·포트폴리오·리스크·준법·주문 초안 검토")
    ContainerDb(artifacts, "Research Artifacts", "CSV/JSONL/Markdown/Parquet", "기업 보고서와 분석·관찰 산출물")
  }

  System_Ext(kis, "KIS API", "시장 데이터·잔고·현금 주문")
  System_Ext(dart, "DART API", "공시·기업·재무 데이터")
  System_Ext(gemini, "Gemini", "뉴스·기업 분석")
  System_Ext(telegram, "Telegram", "운영 알림")
  System_Ext(actions, "GitHub Actions", "예약 CLI 실행")

  Rel(operator, runtime, "앱 실행·운영")
  Rel(operator, dashboard, "상태·거래·전략 조회")
  Rel(runtime, db, "초기화·포지션 동기화")
  Rel(runtime, scheduler, "생성·시작")
  Rel(runtime, dashboard, "FastAPI 서버 시작")
  Rel(scheduler, strategy_engine, "주기적 전략 실행")
  Rel(strategy_engine, api_adapter, "시세·OHLCV 조회")
  Rel(strategy_engine, db, "뉴스·섹터 신호 저장")
  Rel(strategy_engine, api_adapter, "매수·매도·잔고 요청")
  Rel(api_adapter, kis, "HTTPS API 호출")
  Rel(scheduler, db, "거래·포지션·검증 결과 기록")
  Rel(scheduler, telegram, "체결·오류·일일 요약")
  Rel(dashboard, db, "조회·전략 상태 변경")
  Rel(dashboard, api_adapter, "실시간 잔고·시장 조회")
  Rel(actions, research_cli, "예약 실행")
  Rel(research_cli, dart, "공시·재무 수집")
  Rel(research_cli, kis, "OHLCV·수급·현재가 수집")
  Rel(research_cli, gemini, "선택적 분석 보조")
  Rel(research_cli, artifacts, "리서치 산출물 생성")
  Rel(agent, artifacts, "후보·리서치 읽기·실행 결과 저장")
  Rel(agent, db, "운영 포트폴리오 선택 시 조회")
```

## 컨테이너 경계의 의미

- `Trading Runtime`과 `Research CLI Pipeline`은 모두 Python이지만 실행 목적과 상태 경계가 다르다.
- 운영 DB는 소수의 최신 운영 상태와 거래 기록을 보관한다.
- 리서치 결과는 파일 중심이며, 날짜별 스냅샷과 관찰 로그가 분석의 장기 상태다.
- `Free Agent Committee`는 실제 broker API를 호출하지 않는 안전한 주문 초안 단계다.

