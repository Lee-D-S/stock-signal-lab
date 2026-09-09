# 무료 1인 투자기업 Agent 시스템 PLAN

작성일: 2026-05-07

## 1. 목적

이 문서는 OpenAI API 같은 유료 LLM API 없이 시작할 수 있는 1인 투자기업 Agent 시스템 계획이다. 기존 장기 PLAN은 AI Agent 런타임까지 고려하지만, 이 문서는 우선 무료로 가능한 범위에 집중한다.

초기 목표는 LLM이 독립적으로 판단하는 Agent가 아니라, 다음 조합으로 투자 운영 프로세스를 만드는 것이다.

```text
규칙 기반 Agent
+ 기존 Python 리서치/스크리닝/백테스트 파이프라인
+ 사람이 작성한 투자 리포트
+ Codex CLI를 통한 개발/리서치 보조
```

초기 자동화 흐름은 다음과 같다.

```text
데이터 수집
→ 조건/신호 산출
→ 포트폴리오 검토
→ 리스크/준법 Gate
→ 주문안 생성
→ 사람 승인
→ 기록/보고
```

## 2. 기본 원칙

- OpenAI API는 사용하지 않는다.
- Agent는 유료 모델 호출이 아니라 Python 모듈/클래스 형태로 구현한다.
- 투자 판단의 핵심 차단 규칙은 LLM이 아니라 코드로 처리한다.
- 실제 주문 자동화는 초기 범위에서 제외한다.
- Codex CLI는 운영 파이프라인에 넣지 않고, 개발/리서치 보조로만 사용한다.
- 사람이 최종 승인하기 전까지 주문은 실행하지 않는다.

## 3. 무료 버전 Agent 구조

초기 Agent는 9개로 단순화한다.

| Agent | 무료 버전 구현 방식 |
|---|---|
| `QuantSignalAgent` | 기존 스크리너, 가격/거래량 조건, 백테스트 결과를 후보 신호로 표준화 |
| `StrategyDecisionAgent` | 조건별 기대수익, hit rate, 진입 규칙, 보유기간, 매수/반등감시/회피/청산감시 방향을 해석 |
| `EquityResearchAnalystAgent` | 기업의 사업, 실적, 밸류에이션, 촉매, 리스크, 반증 조건을 체크리스트화 |
| `ResearchFileAgent` | 사람이 작성한 기업 리포트, DART/뉴스 수집 결과, 기존 분석 문서를 읽어 체크리스트화 |
| `PortfolioManagerAgent` | 보유 비중, 현금 비중, 후보 종목 점수 기반으로 매수/매도/보류 제안 |
| `RiskManagerAgent` | 종목/섹터 비중, 손실한도, 유동성, 데이터 누락을 코드로 차단 |
| `ComplianceOfficerAgent` | 투자근거 존재 여부, 필수 기록, 금지 조건을 코드로 검사 |
| `TraderAgent` | 실제 주문 없이 주문안 JSON/Markdown 생성 |
| `OperationsReportAgent` | 실행 결과, 주문안, 승인 여부, 일간/주간 보고서 생성 |

`InvestmentCommitteeAgent`는 별도 AI 모델이 아니라 orchestrator 역할로 구현한다. 각 Agent 결과를 모아 최종 `approve`, `block`, `needs_review` 상태와 요약 보고서를 만든다.

## 4. 무료 버전 실행 흐름

```text
1. QuantSignalAgent
   기존 scripts 기반으로 후보군, 점수, 신호 출처 생성

2. StrategyDecisionAgent
   신호의 전략 방향, 기대수익, hit rate, 진입 규칙, 보유기간 해석

3. EquityResearchAnalystAgent
   후보 기업의 사업, 실적, 밸류에이션, 촉매, 리스크, 반증 조건 확인

4. ResearchFileAgent
   기업별 리포트/공시 점검 결과/뉴스 수집 결과에서 투자근거 존재 여부 확인

5. PortfolioManagerAgent
   후보 종목을 현재 포트폴리오와 비교해 액션 제안

6. RiskManagerAgent
   코드 기반 한도 검사로 approve/block/needs_review 산출

7. ComplianceOfficerAgent
   투자근거/기록/금지조건 체크

8. TraderAgent
   실제 주문이 아닌 주문안 생성

9. OperationsReportAgent
   결과 저장 및 사람이 읽을 보고서 생성
```

## 5. 기술 방향

| 필요 기능 | 무료 버전 방향 |
|---|---|
| Agent 실행 | Python 클래스/함수 |
| Agent 조율 | 명시적 orchestrator |
| 신호 산출 | 기존 `scripts/` 재사용 |
| 리스크 차단 | Python guardrail |
| 준법 차단 | 체크리스트/필수 파일 검사 |
| 결과 저장 | JSON + Markdown |
| 리포트 작성 | 템플릿 기반 Markdown |
| 자연어 리서치 | 사람이 작성한 문서 + Codex CLI 보조 |

선택 사항으로, 나중에 무료 로컬 LLM이 필요하면 Ollama 같은 로컬 모델을 `ResearchFileAgent` 요약 단계에만 붙일 수 있다. 초기 PLAN에서는 필수로 두지 않는다.

## 6. 권장 프로젝트 구조

```text
core/agents/
  무료 Agent 인터페이스와 orchestrator

core/agent_tools/
  기존 스크립트 기능을 호출하기 쉬운 함수로 래핑

core/guardrails/
  리스크/준법/주문안 차단 규칙

data/agent_runs/
  Agent 실행 결과 JSON, 주문안, 보고서 저장

scripts/run_free_agent_pipeline.py
  무료 Agent 파이프라인 실행 CLI

scripts/run_agent_committee.py
  agent 회의/토론 전체 실행 표준 CLI. 후보 미지정 시 최근 전략 신호 CSV에서 자동 발견

docs/
  투자 정책, 리스크 정책, Agent별 상세 PLAN
```

기존 `PLAN/1인_투자기업_AI_Agent_시스템_PLAN.md`는 장기 방향 문서로 유지하고, 이 문서는 API 없이 시작하는 현실적인 v1 계획으로 사용한다.

## 7. 공통 Agent 입출력

각 Agent는 같은 형태로 동작한다.

입력:

```text
run_date
ticker 또는 universe
portfolio_snapshot
research_context
config
```

출력:

```text
status: approve | block | needs_review | info
summary
signals
warnings
required_human_checks
artifacts
```

실행 결과는 날짜별 폴더에 저장한다.

```text
data/agent_runs/YYYY-MM-DD/
  quant_signal.json
  strategy_decision.json
  research_file.json
  portfolio_manager.json
  risk_manager.json
  compliance_officer.json
  trader_order_proposal.json
  final_committee_report.md
```

## 8. 기존 스크립트 재사용 계획

우선 새 알고리즘을 만들지 않고 기존 자산을 연결한다.

| 기존 기능 | 활용 방향 |
|---|---|
| `scripts/run_signal_research_pipeline.py` | 일간 신호/관찰 흐름 |
| `scripts/run_daily_universe_refresh.py` | 후보군 갱신 |
| `scripts/run_scoring.py` | 점수/조건 평가 |
| `scripts/run_backtest.py` | 필요한 경우 전략 검증 |
| `scripts/classify_gaps_and_draft_strategy.py` | 조건별 action_hint, preferred_hold_days, 기대수익, hit rate 산출 |
| `scripts/screener.py` | 스크리닝 실행 |
| `scripts/screener_lib/` | DART, 가격 데이터, 유니버스, 출력 유틸 |

무료 Agent는 이 스크립트들을 직접 대체하지 않는다. 기존 결과를 읽거나, 얇은 wrapper로 호출 가능한 형태를 만든다.

## 9. Guardrail 우선 구현

가장 먼저 코드로 막을 규칙을 만든다.

기본 차단 규칙:

- 종목 최대 비중 초과
- 섹터 최대 비중 초과
- 일간 손실한도 초과
- 거래대금 대비 주문금액 과다
- 데이터 누락 또는 오래된 데이터
- 투자근거 파일 없음
- 주문안 필수 필드 누락

판정 기준:

- 치명적 위반: `block`
- 판단 애매함 또는 사람 확인 필요: `needs_review`
- 문제 없음: `approve`
- 단순 정보 제공: `info`

## 10. 주문안 생성

초기 주문안은 실제 API 주문이 아니라 검토용 문서다.

주문안 필수 필드:

```text
ticker
name
side: buy | sell | hold
suggested_amount
suggested_quantity
order_type_hint
reason
risk_status
compliance_status
human_checklist
```

주문안은 JSON으로 저장하고, 사람이 읽을 수 있는 Markdown 섹션도 최종 보고서에 포함한다.

## 11. 보고서 형식

최종 보고서는 사람이 바로 볼 수 있게 Markdown으로 만든다.

```text
# Daily Investment Committee Report

- 오늘의 후보
- 매수/매도/보류 제안
- Risk 차단 항목
- Compliance 차단 항목
- 사람이 확인할 항목
- 다음 액션
```

보고서는 투자 판단을 대체하는 문서가 아니라, 사람이 승인하기 위한 검토 자료다.

## 12. 구축 단계

### Phase 1. 무료 Agent 정책 문서화

목표:

- OpenAI API 없이 가능한 범위와 불가능한 범위를 명확히 한다.
- AI 판단이 아니라 규칙 기반 자동화와 사람이 보완하는 리서치임을 명시한다.
- 실제 주문 자동화는 제외한다.

산출물:

- 이 PLAN 문서
- Agent별 상세 PLAN 작성 순서
- 리스크/준법 기본 체크리스트

### Phase 2. Agent 공통 인터페이스 설계

목표:

- 모든 Agent가 같은 입력/출력 구조를 사용하게 한다.
- 실행 결과를 날짜별 JSON으로 저장한다.

산출물:

- Agent result schema
- 날짜별 실행 폴더 구조
- orchestrator 실행 순서

### Phase 3. 기존 스크립트 재사용

목표:

- 기존 스크리닝, 신호, 백테스트, scoring 기능을 Agent 파이프라인에 연결한다.
- 새 알고리즘보다 기존 결과의 표준화에 집중한다.

산출물:

- QuantSignalAgent 결과 JSON
- 후보군/점수/관찰 결과 요약

### Phase 4. Guardrail 구현

목표:

- 투자 판단보다 먼저 차단 규칙을 신뢰 가능하게 만든다.

산출물:

- RiskManagerAgent
- ComplianceOfficerAgent
- `approve`, `block`, `needs_review`, `info` 표준 상태

### Phase 5. 주문안/보고서 생성

목표:

- 실제 주문 없이 사람이 검토할 수 있는 주문안과 최종 보고서를 만든다.

산출물:

- 주문안 JSON
- 최종 투자위원회 Markdown 보고서
- 일간 실행 로그

## 13. 초기 테스트 계획

- 후보 종목 1개를 넣었을 때 Agent 실행 폴더가 생성되는지 확인한다.
- 기존 스크리닝/신호 결과를 `QuantSignalAgent`가 읽어 표준 JSON으로 변환하는지 확인한다.
- `StrategyDecisionAgent`가 조건별 action_hint, 기대수익, hit rate, 보유기간을 후보별 전략 결정으로 변환하는지 확인한다.
- 투자근거 파일이 없으면 `ComplianceOfficerAgent`가 `needs_review` 또는 `block`을 반환하는지 확인한다.
- 종목 비중 한도를 넘기면 `RiskManagerAgent`가 `block`을 반환하는지 확인한다.
- `TraderAgent`가 실제 주문 없이 주문안 파일만 생성하는지 확인한다.
- 모든 Agent 결과가 최종 Markdown 보고서로 합쳐지는지 확인한다.
- OpenAI API 키가 없어도 전체 파이프라인이 실행되는지 확인한다.

## 14. 가정과 제외 범위

가정:

- 무료 버전은 외부 유료 LLM API를 사용하지 않는다는 뜻으로 정의한다.
- ChatGPT/Codex CLI는 사람이 직접 쓰는 개발/리서치 보조 도구로만 사용한다.
- 운영 파이프라인은 Python 코드와 기존 데이터/스크립트만으로 실행한다.
- 주식만 대상으로 한다.
- 자연어 리서치 품질은 초기에는 사람이 작성한 기업 리포트와 기존 분석 문서 품질에 의존한다.

제외 범위:

- 실제 주문 자동화
- 타인 자금 운용
- 투자자문/투자일임/펀드 설정
- 부동산, 채권, 보험 등 주식 외 자산군
- OpenAI API, Gemini API 등 외부 유료 LLM 호출
- 인허가/법무/세무 세부 검토

## 15. 다음 작업

다음 단계는 `StrategyDecisionAgent`를 Quant와 Portfolio 사이에 연결하는 것이다. 무료 버전에서는 Quant가 후보를 만들고, StrategyDecision이 그 후보의 전략적 의미를 해석한 뒤, Portfolio가 계좌 적합성을 판단해야 한다.
