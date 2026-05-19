# Connect AI 기반 1인 투자기업 Agent 연동 PLAN

작성일: 2026-05-19

## 1. 목적

이 문서는 `C:\projects\auto-invest`의 Python 기반 투자 Agent 시스템을 `C:\projects\connect-ai`의 AI 에이전트 운영 UI와 연결하기 위한 계획이다.

핵심 방향은 기존 투자 로직을 LLM으로 대체하는 것이 아니다. `auto-invest`는 투자 엔진으로 유지하고, `connect-ai`는 에이전트 캐릭터, 회의 UI, 실행 버튼, 리포트 표시, 로컬/클라우드 LLM 요약 계층을 담당한다.

목표 구조:

```text
Connect AI
  -> auto-invest CLI 실행
  -> data/agent_runs 결과 수집
  -> 로컬 LLM 또는 Gemini가 요약/반론/브리핑
  -> 사람 승인 전까지 주문 실행 금지
```

## 2. 현재 상태

`auto-invest`에는 이미 코드 기반 투자 Agent 파이프라인이 있다.

| 구성요소 | 역할 |
|---|---|
| `core/agents/free_pipeline.py` | 투자 Agent 실행 순서와 결과 산출 |
| `core/agents/free_base.py` | `AgentContext`, `AgentResult`, `Candidate`, `OrderProposal` 공통 구조 |
| `scripts/run_free_agent_pipeline.py` | 수동 후보/발견 후보 기반 파이프라인 실행 |
| `scripts/run_agent_committee.py` | 기본 discovery를 포함한 투자위원회 실행 |
| `data/agent_runs/` | Agent별 JSON, Markdown 보고서 산출 위치 |

현재 Agent는 `QuantSignalAgent`, `EquityResearchAnalystAgent`, `ResearchFileAgent`, `PortfolioManagerAgent`, `RiskManagerAgent`, `ComplianceOfficerAgent`, `TraderAgent`, `OperationsReportAgent` 중심으로 구성되어 있다.

## 3. 연동 원칙

- `RiskManagerAgent`, `ComplianceOfficerAgent`, `TraderAgent`의 코드 기반 차단 규칙은 유지한다.
- LLM은 설명, 요약, 반론, 리서치 보강, 회의록 생성에만 사용한다.
- LLM이 직접 매수/매도 실행 여부를 최종 결정하지 않는다.
- 실제 주문 실행은 기본 범위에서 제외한다.
- 사람 승인 전까지 `execution_allowed`는 보수적으로 다룬다.
- 기본 운영은 `KIS_IS_MOCK=true`를 권장한다.

## 3.1 무료 로컬 LLM 운영 원칙

기존 Agent를 코드 기반으로 만든 이유는 API 비용 없이 무료로 운영하기 위해서였다. 이 목표는 유지한다. 기본 설계는 Ollama 또는 LM Studio에서 실행되는 로컬 LLM을 사용해 토큰 과금 없이 LLM-backed Agent를 만드는 것이다.

무료로 처리할 수 있는 영역:

- 기존 `AgentResult` JSON/Markdown 요약
- 투자회의 발언 생성
- 리스크 반론과 체크리스트 작성
- 리서치 파일의 누락/충돌/오래된 근거 설명
- 사람에게 보낼 일일 브리핑과 텔레그램 요약

로컬 LLM만으로 약한 영역:

- 최신 뉴스 검색
- 최신 공시/실적 외부 검색
- 실시간 이슈 확인
- 웹 기반 출처 검증

따라서 기본값은 로컬 LLM 무료 운영으로 둔다. Gemini 같은 외부 API는 최신 외부 정보 검색이 꼭 필요한 경우에만 선택적으로 사용하며, API 키가 없으면 해당 보강 단계만 건너뛰고 나머지 Agent 운영은 계속한다.

## 4. Connect AI Agent 매핑

`connect-ai`에는 투자기업 전용 Agent 프로필을 추가할 수 있다.

| Connect AI Agent | auto-invest 대응 | 책임 |
|---|---|---|
| CEO | InvestmentCommittee 역할 | 전체 투자회의 요약, 충돌 의견 정리 |
| Quant | `QuantSignalAgent` | 가격/거래대금/신호 후보 설명 |
| Analyst | `EquityResearchAnalystAgent`, `ResearchFileAgent` | 기업 리서치, 반증 조건, 누락 자료 정리 |
| Portfolio Manager | `PortfolioManagerAgent` | 포트폴리오 비중과 현금 상태 해석 |
| Risk Manager | `RiskManagerAgent` | 리스크 차단 사유 설명 |
| Compliance | `ComplianceOfficerAgent` | 규칙 위반, 근거 부족, 필수 기록 확인 |
| Trader | `TraderAgent` | 주문 제안 JSON을 사람이 이해할 수 있게 설명 |
| Secretary | `OperationsReportAgent` | 일일 브리핑, 텔레그램/대시보드 요약 |

## 5. 1차 구현 범위

1차 목표는 실행 가능한 얇은 연결 계층을 만드는 것이다.

### 5.1 실행 명령 연결

Connect AI에서 다음 명령을 실행할 수 있게 한다.

```powershell
rtk python scripts/run_agent_committee.py --discover-limit 10 --test-portfolio balanced
```

일일 신호 파이프라인은 별도 명령으로 연결한다.

```powershell
rtk python -u scripts/run_signal_research_pipeline.py --mode daily --recheck
```

### 5.2 최신 결과 읽기

Connect AI는 `data/agent_runs/YYYY-MM-DD/<run_id>/` 아래 최신 실행 결과를 읽는다.

주요 입력 파일:

```text
quant_signal.json
equity_research_analyst.json
research_file.json
portfolio_manager.json
risk_manager.json
compliance_officer.json
trader_order_proposal.json
operations_report.json
final_committee_report.md
pipeline_manifest.json
```

`pipeline_manifest.json`이 있으면 우선 사용하고, 없으면 최신 수정 시각 기준으로 run 디렉터리를 찾는다.

### 5.3 LLM 요약 계층

로컬 LLM 또는 Gemini는 다음 출력만 담당한다.

- 오늘의 후보 종목 요약
- Agent별 approve/block/needs_review 사유 설명
- Risk/Compliance 차단 사유를 사람이 읽기 쉬운 언어로 변환
- Trader 주문 제안의 전제와 보류 조건 요약
- 다음 사람이 확인해야 할 체크리스트 생성

## 6. 후속 구현 단계

### Phase 1: 문서/설정 기반 연동

- Connect AI 설정에 `autoInvestPath`를 추가한다.
- 기본값은 비워두고, 사용자가 `C:\projects\auto-invest`를 지정한다.
- 지정 경로에 `scripts/run_agent_committee.py`가 있는지 확인한다.

### Phase 2: 실행 버튼과 결과 패널

- Connect AI 대시보드에 `투자위원회 실행` 버튼을 추가한다.
- 실행 중 상태, 완료 상태, 실패 로그를 표시한다.
- 완료 후 최신 `agent_runs` 결과를 읽어 Agent별 카드로 표시한다.

### Phase 3: 회의형 요약

- 각 JSON 결과를 Connect AI Agent 발언으로 변환한다.
- CEO가 최종 회의록을 생성한다.
- Secretary가 텔레그램 또는 일일 브리핑 형식으로 짧게 요약한다.

### Phase 4: 선택적 LLM 고도화

- 로컬 LLM은 기본 엔진으로 사용하며, 내부 산출물 요약과 반론 생성을 토큰 과금 없이 처리한다.
- Gemini 등 외부 모델은 최신 뉴스/외부 요인 검색이 필요한 리서치 보강에만 선택적으로 사용한다.
- 외부 API 키가 없거나 호출에 실패해도 로컬 LLM 기반 Agent 회의는 계속 진행한다.
- 모든 LLM 출력은 투자 판단의 근거가 아니라 검토 보조 자료로 표시한다.

### Phase 5: Python Agent를 LLM-backed Agent로 확장

이 단계부터는 단순히 `auto-invest`를 Connect AI에 붙이는 수준을 넘어서, `connect-ai` 코드를 수정해 투자기업 전용 LLM Agent 런타임으로 사용한다. 기존 Python Agent는 데이터 수집, 계산, 검증, 차단 규칙을 담당하고, 각 역할별 LLM Agent는 해당 결과를 해석해 의견을 낸다.

기본 실행 모델은 무료 로컬 LLM이다. 각 LLM-backed Agent는 Connect AI의 Ollama/LM Studio 연결을 통해 호출한다. 모델이 없거나 로컬 서버가 꺼져 있으면 Python Agent 결과와 기존 Markdown 보고서만 표시하고, LLM 의견 생성은 `skipped` 상태로 기록한다.

목표 구조:

```text
Python Agent
  -> 숫자 계산, 파일 검사, 리스크/준법 hard block
  -> AgentResult JSON 생성

LLM-backed Agent
  -> AgentResult + 리서치 문서 + 포트폴리오 상태 입력
  -> 역할별 투자 의견, 반론, 질문, 사람 확인사항 생성

Final Gate
  -> Python guardrail 결과를 최종 우선
  -> LLM 의견은 execution_allowed를 직접 true로 바꾸지 못함
```

역할별 확장:

| Python Agent | LLM-backed Agent | LLM의 책임 |
|---|---|---|
| `QuantSignalAgent` | Quant LLM Agent | 신호 품질, 과최적화 위험, 추가 확인 지표 설명 |
| `EquityResearchAnalystAgent` | Analyst LLM Agent | 투자 thesis, catalyst, risk, disconfirmation 작성 |
| `ResearchFileAgent` | Evidence LLM Agent | 리서치 누락, 오래된 근거, 근거 충돌 해석 |
| `PortfolioManagerAgent` | PM LLM Agent | 비중 조정 의견, 현금 사용 우선순위, 집중도 코멘트 |
| `RiskManagerAgent` | Risk LLM Agent | 손실 시나리오, 변동성, 리스크 완화 질문 생성 |
| `ComplianceOfficerAgent` | Compliance LLM Agent | 준법/기록 누락을 사람이 이해할 문장으로 설명 |
| `TraderAgent` | Trader LLM Agent | 주문 방식, 보류 조건, 체결 전 체크리스트 작성 |
| `OperationsReportAgent` | Secretary LLM Agent | 투자회의록, 일일 브리핑, 텔레그램 요약 작성 |

### Phase 6: Connect AI 코드 수정 범위

`connect-ai`는 범용 AI 회사 UI에서 투자기업 운영 UI로 확장한다.

- `src/agents.ts`에 투자기업 전용 Agent 정의를 추가하거나 별도 `investmentAgents.ts`로 분리한다.
- 설정에 `autoInvestPath`, `autoInvestPythonCommand`, `autoInvestDefaultMode`를 추가한다.
- VS Code command를 추가한다: `Connect AI: Run Investment Committee`, `Connect AI: Open Latest Investment Report`.
- auto-invest CLI 실행기는 child process로 구현하되, 작업 디렉터리는 `autoInvestPath`로 고정한다.
- 결과 로더는 `pipeline_manifest.json`을 우선 읽고, 없으면 최신 `data/agent_runs/YYYY-MM-DD/<run_id>`를 탐색한다.
- Webview에는 Agent별 Python 결과와 LLM 의견을 분리해서 표시한다.
- LLM 호출은 Connect AI의 기존 Ollama/LM Studio 감지 로직을 재사용한다.
- Gemini 같은 외부 모델은 리서치 보강용 선택 옵션으로 남긴다.
- UI에는 현재 사용 중인 엔진을 명확히 표시한다: `Local LLM`, `External API`, `No LLM`.
- `No LLM` 상태에서도 Python Agent 실행과 결과 조회는 가능해야 한다.

Connect AI에서 생성할 LLM 출력은 `auto-invest` 산출물과 분리한다. 초기에는 `data/agent_runs/YYYY-MM-DD/<run_id>/connect_ai_llm_review.json` 또는 같은 내용의 Markdown 요약으로 저장한다.

## 7. 제외 범위

- 기존 `auto-invest` Agent의 계산, 검증, 리스크/준법 hard guardrail을 LLM으로 대체하지 않는다.
- 각 Agent의 해석/토론/질문/보고서 작성 기능은 LLM-backed Agent로 확장할 수 있다.
- LLM이 직접 주문 API를 호출하지 않는다.
- 실거래 자동 주문을 이번 연동 범위에 넣지 않는다.
- `auto-invest`의 리스크/준법 규칙을 Connect AI 쪽 UI 판단으로 우회하지 않는다.

## 8. 검증 기준

- Connect AI에서 투자위원회 실행 명령을 호출할 수 있다.
- `data/agent_runs` 최신 결과를 찾아 Agent별 상태를 표시할 수 있다.
- Ollama/LM Studio가 켜져 있으면 로컬 LLM으로 Agent별 의견을 생성할 수 있다.
- 로컬 LLM이 꺼져 있어도 Python Agent 결과 표시는 실패하지 않는다.
- Gemini API 키가 없어도 외부 리서치 보강만 건너뛰고 무료 로컬 흐름은 유지된다.
- Risk/Compliance가 `block` 또는 `needs_review`일 때 UI와 요약에 명확히 드러난다.
- Trader 제안이 있어도 사람 승인 전에는 실행 가능하다고 표현하지 않는다.
- 명령 실패, `.env` 누락, KIS mock/live 상태를 사용자에게 명확히 보여준다.

## 9. 운영 원칙

이 연동의 최종 형태는 다음 역할 분담을 유지한다.

```text
auto-invest = 투자 데이터, 규칙, 파이프라인, 산출물, 안전장치
connect-ai  = 운영 UI, Agent 캐릭터, 회의 진행, LLM 요약, 알림
사람        = 최종 승인, 실거래 책임, 전략 변경 결정
```

따라서 Connect AI는 투자기업의 “사장실/회의실” 역할을 하고, auto-invest는 실제 투자 검토를 수행하는 “백오피스 엔진” 역할을 한다.
