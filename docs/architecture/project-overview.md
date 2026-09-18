# 프로젝트 개요

## 한 문장 요약

이 저장소는 한국투자증권(KIS) API를 이용한 국내 주식 자동매매 런타임과, 기업별 주가 변동 원인을 이벤트로 구조화해 조건·백테스트·관찰 성과로 연결하는 리서치 플랫폼이 결합된 Python 프로젝트다.

## 분석 기준과 범위

| 항목 | 결과 |
| --- | ---: |
| 기준 커밋 | `66a2d78` |
| 추적 파일 | 4,388개 |
| 런타임 디렉터리를 제외한 물리 파일 | 4,815개 |
| Python 파일 | 146개 |
| Markdown 파일 | 2,197개 |
| JSONL 파일 | 1,977개 |
| Trailmark 노드 | 3,992개 |
| Trailmark 함수 | 1,024개 |
| Trailmark 클래스 | 47개 |
| Trailmark 호출 엣지 | 11,088개 |
| 감지된 진입점 | 64개 |

`node_modules`, `.git`, `__pycache__`, `.ruff_cache`, `.local`은 런타임·도구 생성 디렉터리로 분리했다. `.env`, 토큰 캐시, SQLite DB는 비밀값·상태 데이터이므로 내용을 문서에 복사하지 않았다.

## 시스템의 세 축

### 1. 실시간 자동매매 앱

`main.py`가 다음 세 요소를 초기화한다.

1. SQLAlchemy async 기반 SQLite DB
2. Asia/Seoul 기준 APScheduler
3. FastAPI 모니터링 대시보드

스케줄러는 시장 데이터와 OHLCV를 조회하고, 등록된 전략을 실행한 뒤 KIS 주문·포지션·거래 기록·Telegram 알림을 연결한다. 현재 전략 등록은 `scheduler/runner.py`의 `STRATEGIES` 목록에서 관리한다.

### 2. 주가 변동 원인 리서치 플랫폼

`legacy/research_data/ai 주가 변동 원인 분석/`은 본 앱과 별도로 동작하는 파일 기반 연구 공간이다.

```text
거래대금 상위 유니버스
→ 신규 기업·분기 보고서
→ events.jsonl / 이벤트.csv
→ 공통 패턴·가설
→ 프록시/실전형 백테스트
→ active 조건 기반 일별 후보
→ 수급 재조회
→ 관찰 로그
→ D+1/D+5/D+10/D+20 성과 집계
```

이 흐름은 `scripts/run_signal_research_pipeline.py`가 `daily`, `backtest`, `event-discovery`, `full` 모드로 조합한다.

### 3. free-agent 투자위원회

`core/agents/free_pipeline.py`와 `scripts/run_free_agent_pipeline.py`는 후보 종목을 규칙 기반 Agent 체인으로 검토한다.

```text
QuantSignalAgent
→ EquityResearchAnalystAgent
→ ResearchFileAgent
→ PortfolioManagerAgent
→ RiskManagerAgent
→ ComplianceOfficerAgent
→ TraderAgent
→ OperationsReportAgent
```

이 파이프라인은 주문 초안을 만들지만 `execution_allowed=False`를 강제하며 실제 주문 실행을 하지 않는다. 선택적으로 `core/llm/committee.py`가 로컬 Ollama 검토와 최종 gate를 추가한다.

## 외부 시스템

- KIS mock/live API: OAuth2, 시세·OHLCV·잔고·현금 주문
- KIS 실전 서버: 모의투자 중에도 일부 시장 데이터와 실전 토큰이 필요할 수 있음
- DART OpenAPI: 기업 코드, 공시, 재무 정보
- Google Gemini: 뉴스·기업 분석 및 일부 리서치 보조
- Ollama: 선택적 로컬 LLM 위원회
- Naver Finance 및 RSS: 뉴스 수집
- Telegram: 체결·오류·일일 요약 알림
- GitHub Actions: 오전 공시 확인과 일일 리서치 루틴

## 가장 중요한 경계

- `core/`, `models/`, `scheduler/`, `strategies/`, `dashboard/`, `notifier/`: 운영 자동매매 런타임
- `scripts/`: 리서치·스크리닝·백테스트·관찰·보고서 생성 CLI
- `legacy/research_data/ai 주가 변동 원인 분석/`: 대규모 연구 산출물과 문서 저장소
- `data/`: 캐시, 테스트 포트폴리오, 리서치 중간 산출물
- `.github/workflows/`: 원격 일일 자동화와 결과 commit/push

## 안전 모델

매매 런타임에는 주문 금액 제한, 일별 매수 한도, 최대 보유 종목 수, 손절·익절, 연속 손실 Circuit Breaker가 있다. 그러나 `KIS_IS_MOCK=false`이면 실제 주문 경로가 열리므로 분석·검증 단계에서는 `KIS_IS_MOCK=true`를 유지해야 한다.

