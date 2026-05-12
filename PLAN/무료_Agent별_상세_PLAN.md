# 무료 Agent별 상세 PLAN

작성일: 2026-05-08

> 2026-05-08 업데이트: 이 문서는 전체 목차/초기 초안으로만 유지한다.
> 실제 운영 PLAN은 Agent별 개별 문서로 분리했다.
>
> - `PLAN/free_agent_plans/00_투자팀_구성_검증.md`
> - `PLAN/free_agent_plans/01_QuantSignalAgent_PLAN.md`
> - `PLAN/free_agent_plans/02_EquityResearchAnalystAgent_PLAN.md`
> - `PLAN/free_agent_plans/03_ResearchFileAgent_PLAN.md`
> - `PLAN/free_agent_plans/04_PortfolioManagerAgent_PLAN.md`
> - `PLAN/free_agent_plans/05_RiskManagerAgent_PLAN.md`
> - `PLAN/free_agent_plans/06_ComplianceOfficerAgent_PLAN.md`
> - `PLAN/free_agent_plans/07_TraderAgent_PLAN.md`
> - `PLAN/free_agent_plans/08_OperationsReportAgent_PLAN.md`
> - `PLAN/free_agent_plans/09_FreeAgentPipeline_PLAN.md`
> - `PLAN/free_agent_plans/12_StrategyDecisionAgent_PLAN.md`

## 1. 목적

이 문서는 `PLAN/무료_1인_투자기업_Agent_시스템_PLAN.md`를 바탕으로, 무료 버전 Agent를 각각 어떻게 발전시킬지 정리한 상세 계획이다.

현재 구현은 OpenAI API 없이 동작하는 v1 골격이다. 각 Agent는 `core/agents/free_pipeline.py` 안에 Python 클래스로 구현되어 있으며, `FreeAgentPipeline`이 전체 실행 순서를 조율한다. 실제 주문은 하지 않고, 날짜별 JSON 결과와 최종 Markdown 보고서를 생성한다.

초기 원칙:

- 유료 LLM API를 사용하지 않는다.
- 실제 주문 API를 호출하지 않는다.
- 리스크/준법 차단은 코드 규칙으로 처리한다.
- 사람이 최종 승인하기 전까지 주문은 실행하지 않는다.
- 각 Agent의 지능은 작은 규칙부터 점진적으로 추가한다.

## 2. 공통 설계

모든 Agent는 같은 공통 결과 구조를 사용한다.

```text
status: approve | block | needs_review | info
summary
signals
warnings
required_human_checks
artifacts
```

상태 의미:

| Status | 의미 |
|---|---|
| `approve` | 규칙상 통과 |
| `block` | 진행 금지 |
| `needs_review` | 사람 확인 필요 |
| `info` | 정보 제공 |

날짜별 산출물:

```text
data/agent_runs/YYYY-MM-DD/
  quant_signal.json
  strategy_decision.json
  equity_research_analyst.json
  research_file.json
  portfolio_manager.json
  risk_manager.json
  compliance_officer.json
  trader_order_proposal.json
  operations_report.json
  final_committee_report.md
```

## 3. QuantSignalAgent PLAN

### 역할

후보 종목과 전략 신호를 표준화한다. 무료 버전에서는 기존 `scripts/`의 신호, 스크리닝, scoring, observation 결과를 읽어 투자위원회 파이프라인의 입력으로 변환한다.

### 현재 상태

- CLI 후보 또는 CSV 자동 발견 후보를 `signals`로 정리한다.
- 후보가 없으면 `needs_review`를 반환한다.
- 아직 기존 신호 CSV의 점수, 조건명, 관찰 상태를 깊게 해석하지 않는다.

### 입력

- 수동 후보: `--candidate TICKER[:NAME[:AMOUNT]]`
- 자동 발견 후보: `--discover`
- 기존 전략신호/관찰 CSV
- 실행일

### 출력

- 후보 종목 리스트
- 후보 출처
- 점수 또는 조건명
- 데이터 누락 경고

### v1 보강 과제

- 기존 `07_전략신호` CSV에서 종목코드, 종목명, 조건명, 점수를 안정적으로 추출한다.
- 후보별 `source_type`을 구분한다: manual, watchlist, foreign_flow, new_condition, candlestick, scoring.
- 같은 종목이 여러 신호에 잡히면 중복 제거하고 신호 목록을 합친다.
- 오래된 CSV 또는 빈 CSV는 `needs_review` 경고로 표시한다.

### 성공 기준

- `--discover --discover-limit N` 실행 시 후보 N개 이하가 표준 JSON으로 생성된다.
- 후보마다 최소 `ticker`, `name`, `source`, `signal_count`가 기록된다.
- 기존 CSV 형식이 조금 달라도 파이프라인이 실패하지 않고 경고를 남긴다.

## 4. StrategyDecisionAgent PLAN

### 역할

Quant가 만든 후보 신호를 전략 행동 언어로 해석한다. 이 Agent는 매수 승인자가 아니라, 조건별 기대수익, hit rate, 진입 규칙, 계획 보유기간, 회피 또는 청산 감시 방향을 표준화한다.

### 현재 상태

- 전담 Agent는 아직 없다.
- `전략_조건_초안.csv`와 후보 CSV에는 `action_hint`, `preferred_entry_mode`, `preferred_hold_days`, `avg_score_return_pct`, `hit_rate`가 존재한다.
- 현재 free pipeline은 이 값을 Portfolio 판단에 충분히 반영하지 못한다.

### 입력

- Quant 후보와 `signal_details`
- `07_전략신호/05_전략/전략_조건_초안.csv`
- `07_전략신호/02_신규조건/신규조건_전략_조건.csv`
- 관찰 성과 요약 CSV

### 출력

- 전략 방향: `buy_candidate`, `rebound_watch`, `avoid`, `exit_watch`, `hold`
- 진입 규칙
- 계획 보유기간
- 기대수익과 hit rate
- 청산/회피 감시 조건
- 전략 해석 신뢰도와 경고

### v1 보강 과제

- `hypothesis_id` 또는 조건명을 기준으로 후보와 전략 조건을 매칭한다.
- `action_hint`를 표준 `decision_type`으로 변환한다.
- `preferred_entry_mode`, `preferred_hold_days`, `avg_score_return_pct`, `hit_rate`를 후보별 결과에 남긴다.
- 회피 조건은 매수 후보로 넘기지 않고 `avoid` 또는 `exit_watch`로 표시한다.
- Portfolio가 `strategy_decision.json`을 사용해 신규 매수 초안을 만들도록 연결한다.

### 성공 기준

- Quant 후보만 보고 매수/회피 방향이 섞이지 않는다.
- 조건별 기대수익과 보유기간이 Portfolio와 최종 보고서까지 전달된다.
- 매도/청산 감시 후보는 신규 매수 후보와 분리된다.

## 5. ResearchFileAgent PLAN

### 역할

후보 종목에 대해 투자근거 문서가 존재하는지 확인하고, 사람이 리서치를 보완해야 할 항목을 표시한다.

### 현재 상태

- `ai 주가 변동 원인 분석/00_기업별분석` 아래에서 종목코드 또는 종목명이 포함된 파일명을 찾는다.
- 파일이 없으면 `needs_review` 경고를 낸다.
- 파일 내용을 분석하거나 품질을 평가하지는 않는다.

### 입력

- 후보 종목
- 기업별 분석 폴더
- 기존 리서치 Markdown/CSV/TXT

### 출력

- 매칭된 리서치 파일 경로
- 리서치 파일 존재 여부
- 사람이 확인할 체크리스트

### v1 보강 과제

- 파일명뿐 아니라 상위 폴더명까지 검색 대상에 포함한다.
- 리서치 파일의 마지막 수정일을 기록한다.
- 너무 오래된 리포트는 `needs_review`로 표시한다.
- 필수 섹션 존재 여부를 검사한다: 사업모델, 투자 가설, 리스크, 반증 조건.
- 신규 기업 리포트가 필요한 경우 `scripts/run_new_company_reports.py --include-existing-missing` 실행 안내를 보고서에 포함한다.

### 성공 기준

- 리서치 파일이 없으면 Compliance 단계에서 확인 가능한 경고가 전달된다.
- 리서치 파일이 있어도 오래됐거나 필수 섹션이 없으면 `needs_review`가 유지된다.
- Agent가 리포트를 새로 작성하지는 않고, 필요한 작업만 명확히 표시한다.

## 6. PortfolioManagerAgent PLAN

### 역할

개별 후보가 아니라 포트폴리오 전체 관점에서 매수, 보유, 매도, 보류 초안을 만든다.

### 현재 상태

- 현재 보유 중인 종목이면 `hold`, 신규 후보면 `buy` 초안을 만든다.
- 포트폴리오 JSON이 없으면 현금/비중 판단이 제한된다.
- 목표 비중, 섹터 비중, 현금 정책은 아직 단순하다.

### 입력

- 후보 종목
- 포트폴리오 스냅샷 JSON
- 현금
- 기본 주문 금액
- 보유 종목 정보

### 출력

- 후보별 액션 초안: buy, sell, hold
- 제안 금액
- 간단한 포트폴리오 판단 사유
- 현재 포트폴리오 가치

### v1 보강 과제

- 포트폴리오 JSON 예시 파일을 문서화한다.
- 현금이 부족하면 buy를 `needs_review`로 낮춘다.
- 이미 보유 중인 종목은 추가매수, 유지, 축소 검토를 분리한다.
- 후보 점수와 리서치 상태를 반영해 액션 우선순위를 만든다.
- 포트폴리오 스냅샷이 없으면 명확히 `needs_review`로 표시한다.

### 성공 기준

- 포트폴리오 파일이 있을 때 종목별 보유 비중과 현금 상태가 보고서에 표시된다.
- 신규 후보와 기존 보유 종목의 액션이 구분된다.
- 실제 주문 결정을 하지 않고 사람 검토용 제안만 만든다.

## 7. RiskManagerAgent PLAN

### 역할

손실 가능성과 계좌 훼손 가능성을 코드로 차단한다. 무료 Agent 시스템에서 가장 먼저 신뢰 가능해야 하는 Gate다.

### 현재 상태

- 주문금액 상한을 검사한다.
- 포트폴리오 스냅샷이 없으면 비중 검사를 `needs_review`로 표시한다.
- 포트폴리오 가치가 있으면 종목 비중 초과를 `block`한다.
- 유동성, 섹터, 일간 손실, 이벤트 리스크는 아직 미구현이다.

### 입력

- 후보 종목
- 제안 금액
- 포트폴리오 스냅샷
- 리스크 설정값

### 출력

- 후보별 `approve`, `block`, `needs_review`
- 차단 사유
- 사람이 확인할 리스크 항목

### v1 보강 과제

- 섹터 최대 비중 규칙을 추가한다.
- 현금 부족 규칙을 추가한다.
- 일간 최대 신규매수 금액 규칙을 추가한다.
- 거래대금/유동성 데이터가 없으면 `needs_review`로 표시한다.
- 리스크 설정을 코드 상수보다 설정 파일 또는 CLI 옵션으로 분리한다.

### 성공 기준

- 리스크 규칙 위반 후보는 TraderAgent에서 `hold`로 내려간다.
- 차단 사유가 최종 보고서에 후보별로 표시된다.
- 데이터가 없어서 판단 못 하는 상황은 통과가 아니라 `needs_review`로 남는다.

## 8. ComplianceOfficerAgent PLAN

### 역할

투자 판단의 절차와 기록 요건을 검사한다. 투자근거가 없거나 종목코드가 부정확하면 주문안으로 넘어가지 않도록 막는다.

### 현재 상태

- 종목코드가 6자리 숫자인지 검사한다.
- 리서치 파일 존재 여부를 검사한다.
- 리서치 파일 필수 여부는 CLI 옵션으로 끌 수 있다.
- 미공개정보, 루머, 기록 누락 같은 항목은 체크리스트 수준이다.

### 입력

- 후보 종목
- 리서치 파일 매칭 결과
- 준법 설정값

### 출력

- 후보별 준법 상태
- 투자근거 파일 개수
- 경고 및 사람 확인 항목

### v1 보강 과제

- 투자근거 필수 섹션 누락을 검사한다.
- “루머/확인필요/미확인” 같은 금지 키워드가 리서치 파일에 있는지 점검한다.
- 리서치 파일이 너무 오래됐으면 `needs_review`로 표시한다.
- 수동 후보는 반드시 출처를 남기도록 강제한다.
- 주문안에 준법 상태가 누락되면 TraderAgent가 `needs_review`로 처리한다.

### 성공 기준

- 투자근거가 없는 후보는 자동 승인되지 않는다.
- 준법 경고가 최종 보고서와 주문안 JSON에 모두 남는다.
- Compliance가 `block`한 후보는 주문안에서 `hold`로 바뀐다.

## 9. TraderAgent PLAN

### 역할

실제 주문 없이 검토용 주문안을 생성한다. 주문 API 호출은 하지 않는다.

### 현재 상태

- Risk/Compliance 상태를 반영해 주문안 JSON을 만든다.
- Risk 또는 Compliance가 `block`이면 `hold`로 바꾼다.
- 현재가를 모르므로 수량은 0으로 두고 사람 확인 항목을 남긴다.
- 주문 API 호출 여부를 `broker_api_called: false`로 기록한다.

### 입력

- 후보 종목
- PortfolioManager 제안
- Risk 결과
- Compliance 결과
- 기본 주문 금액

### 출력

- 주문안 JSON
- 매수/매도/보류 방향
- 제안 금액
- 주문 방식 힌트
- 사람 확인 체크리스트

### v1 보강 과제

- PortfolioManagerAgent의 액션을 직접 입력으로 사용한다.
- 현재가 CSV 또는 수동 가격 입력이 있으면 제안 수량을 계산한다.
- Risk/Compliance가 `needs_review`이면 주문안도 `needs_review`로 표시한다.
- 주문안 필수 필드 검사를 강화한다.
- 최종 보고서에 “실제 주문 금지” 문구를 항상 포함한다.

### 성공 기준

- 주문안은 생성되지만 브로커 API는 절대 호출하지 않는다.
- 차단 후보는 주문안에서 `hold`로 표시된다.
- 수량 계산이 불가능하면 실패하지 않고 사람 확인 항목으로 남긴다.

## 10. OperationsReportAgent PLAN

### 역할

모든 Agent 결과를 날짜별로 저장하고 사람이 읽을 최종 보고서를 만든다.

### 현재 상태

- `final_committee_report.md`를 생성한다.
- Agent별 상태, 주문안, 경고, 사람 확인 항목을 모은다.
- 보고서 형식은 최소 Markdown이다.

### 입력

- 모든 Agent 결과
- 실행일
- 후보 종목
- 포트폴리오 요약

### 출력

- 최종 Markdown 보고서
- 운영 결과 JSON
- 실행 폴더

### v1 보강 과제

- 보고서에 최종 상태 요약을 추가한다: block, needs_review, approve 개수.
- 후보별 표를 만든다.
- 주문안 JSON 경로를 보고서에 표시한다.
- 이전 실행 결과와 비교해 신규/반복 후보를 구분한다.
- 향후 Telegram 알림용 짧은 요약을 별도 텍스트로 생성한다.

### 성공 기준

- 사람이 보고 바로 확인할 수 있는 보고서가 생성된다.
- 보고서만 봐도 어떤 후보가 왜 막혔는지 알 수 있다.
- JSON 산출물과 Markdown 보고서가 같은 내용을 가리킨다.

## 11. FreeAgentPipeline / InvestmentCommittee 역할 PLAN

### 역할

별도 AI Agent가 아니라 전체 실행 순서를 조율하는 투자위원회 orchestrator다.

### 현재 상태

- 9개 Agent를 순서대로 실행한다.
- 각 결과를 날짜별 JSON 파일로 저장한다.
- 마지막에 최종 보고서를 생성한다.

### v1 보강 과제

- Agent 간 의존성을 명확히 한다: Portfolio 결과를 Trader가 사용하도록 연결한다.
- 중간 Agent가 `block`을 반환해도 전체 파이프라인은 끝까지 실행하되, 주문안은 보류로 만든다.
- 실행 설정을 CLI 옵션과 config 파일로 분리한다.
- 동일 날짜 재실행 시 덮어쓰기/새 run id 생성 정책을 정한다.
- 최종 상태 계산 기준을 문서화한다.

### 성공 기준

- OpenAI API 키 없이 실행된다.
- 실제 주문 없이 모든 산출물이 생성된다.
- 실패 가능한 입력에서도 파이프라인 전체가 중간 결과와 경고를 남긴다.

## 12. 구현 우선순위

1. `QuantSignalAgent`: 기존 신호 CSV 해석 강화
2. `ResearchFileAgent`: 리서치 파일 최신성/필수 섹션 검사
3. `RiskManagerAgent`: 현금, 섹터, 일간 매수 한도 추가
4. `ComplianceOfficerAgent`: 투자근거 품질 체크 강화
5. `TraderAgent`: Portfolio 결과 반영 및 수량 계산 옵션 추가
6. `OperationsReportAgent`: 후보별 표와 최종 상태 요약 추가
7. `FreeAgentPipeline`: run id, 설정 파일, 재실행 정책 정리

## 13. 테스트 계획

공통 테스트:

- 후보 1개 수동 입력 시 모든 JSON과 최종 보고서가 생성된다.
- 후보가 없으면 실패하지 않고 `needs_review` 보고서가 생성된다.
- OpenAI API 키가 없어도 실행된다.
- 브로커 주문 API가 호출되지 않는다.

Agent별 테스트:

- Quant: 중복 종목이 있는 CSV를 넣으면 하나로 병합된다.
- Research: 리서치 파일이 없으면 `needs_review`가 된다.
- Portfolio: 보유 종목과 신규 종목의 액션이 다르게 나온다.
- Risk: 종목 비중 초과 시 `block`이 된다.
- Compliance: 잘못된 종목코드는 `block`이 된다.
- Trader: 차단 후보는 `hold` 주문안으로 바뀐다.
- Operations: 보고서에 경고와 사람 확인 항목이 모두 포함된다.

## 14. 제외 범위

- 실제 주문 자동화
- OpenAI API, Gemini API 등 유료 LLM 호출
- 투자자문/일임/타인자금 운용
- 세금, 법무, 인허가 세부 검토
- 부동산, 채권, 보험 등 주식 외 자산군
