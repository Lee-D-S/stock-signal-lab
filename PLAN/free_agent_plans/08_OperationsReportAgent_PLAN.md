# OperationsReportAgent PLAN

작성일: 2026-05-08

## 1. 전문가 역할

`OperationsReportAgent`는 투자팀의 **투자 운영 COO / 회의록 책임자 / 감사 추적 관리자**다.

역할은 투자 아이디어를 새로 만들거나 주문을 결정하는 것이 아니라, 모든 Agent의 판단을 사람이 검토 가능한 형태로 보존하는 것이다. 이 Agent가 만든 보고서는 “투자 지시서”가 아니라 “투자위원회 검토 기록”이다.

핵심 책임:

- 모든 Agent 결과를 날짜별 실행 폴더에 보존한다.
- 최종 Markdown 보고서를 생성한다.
- Telegram 또는 짧은 알림에 사용할 요약 텍스트를 생성한다.
- Agent별 status를 요약한다.
- 후보별 최종 주문 제안과 보류 사유를 표로 만든다.
- Risk/Compliance/Trader의 차단 사유를 숨기지 않는다.
- 사람이 다음에 무엇을 확인해야 하는지 체크리스트로 남긴다.
- 실제 주문이 실행되지 않았다는 문구를 항상 남긴다.

하지 말아야 할 일:

- 보고서를 투자 권유나 주문 지시처럼 표현하지 않는다.
- `block`, `needs_review` 경고를 요약에서 누락하지 않는다.
- Trader 결과가 `buy`여도 자동 실행 가능하다고 쓰지 않는다.
- 실패한 Agent를 보고서에서 제외하지 않는다.
- 원본 JSON을 임의로 수정하지 않는다.
- 보고서 생성을 위해 기존 리서치 파일을 삭제하거나 수정하지 않는다.

중요 원칙:

```text
OperationsReportAgent의 목표는 좋은 결론이 아니라 완전한 기록이다.
```

## 2. 입력 계약

### 2.1 Context 입력

`AgentContext`에서 사용하는 정보:

| 필드 | 용도 |
|---|---|
| `run_date` | 보고서 기준일 |
| `candidates` | 후보 수와 후보 목록 요약 |
| `portfolio_value` | 계좌 총 평가 기준 표시 |
| `cash` | 현금 요약 표시 |
| `run_dir` | 보고서와 요약 파일 저장 위치 |
| `config` | 주요 정책 값 표시 |

### 2.2 AgentResult 입력

모든 Agent의 `AgentResult`를 입력으로 받는다.

| 필드 | 용도 |
|---|---|
| `agent` | Agent 이름 |
| `status` | 최종 상태 요약 |
| `summary` | Agent별 한 줄 요약 |
| `signals` | 후보별 상세 결과 |
| `warnings` | 경고와 차단 사유 |
| `required_human_checks` | 사람 확인 항목 |
| `artifacts` | 산출물 경로와 정책 값 |

Operations는 각 Agent 결과를 재판정하지 않는다. 다만 누락되거나 서로 충돌하는 결과가 있으면 보고서에 경고로 남긴다.

### 2.3 필수 Agent 결과

최종 보고서에는 다음 Agent 결과가 포함되어야 한다.

```text
QuantSignalAgent
EquityResearchAnalystAgent
ResearchFileAgent
PortfolioManagerAgent
RiskManagerAgent
ComplianceOfficerAgent
TraderAgent
OperationsReportAgent
```

현재 v1 구현에는 `EquityResearchAnalystAgent`가 연결되어 있다. 향후 실행 중 Analyst 결과가 누락되면 보고서에는 “Analyst result missing”을 숨기지 않고 보완 항목으로 표시한다.

## 3. 처리 프로세스

처리 순서:

```text
1. 실행 폴더 run_dir 확인
2. AgentResult 목록 수집
3. status별 개수 계산
4. 필수 Agent 결과 누락 여부 확인
5. Trader 결과에서 후보별 최종 side/amount/quantity 추출
6. Risk/Compliance status를 후보별 표에 병합
7. 모든 warnings를 Agent별로 모음
8. required_human_checks를 Agent별로 모음
9. 최종 Markdown 보고서 생성
10. Telegram 요약 텍스트 생성
11. operations_report.json 생성
12. 보고서 경로를 artifacts에 기록
```

보고서 작성 원칙:

- 먼저 전체 상태와 후보 수를 보여준다.
- 다음으로 Agent별 status를 보여준다.
- 그 다음 후보별 주문 제안 표를 보여준다.
- warnings와 human checks는 별도 섹션으로 남긴다.
- 실제 주문 금지 문구는 항상 포함한다.

## 4. 판정 기준

Operations의 status는 입력 Agent들의 최악 상태를 따른다.

| Status | 조건 |
|---|---|
| `approve` | 모든 필수 Agent가 실행되고 심각한 경고 없이 보고서 생성 완료 |
| `needs_review` | 하나 이상의 Agent가 `needs_review`이거나 필수 확인 항목이 있음 |
| `block` | 하나 이상의 Agent가 `block`이거나 보고서 생성에 필요한 핵심 결과가 구조적으로 깨짐 |
| `info` | 후보 없음 등 정보성 보고서만 생성된 경우 |

status 계산 원칙:

```text
block > needs_review > approve > info
```

보고서 생성 자체가 성공해도 앞 단계에 `block`이 있으면 Operations의 최종 status도 `block`이어야 한다. 이는 보고서 품질 문제가 아니라 투자위원회 전체 상태를 보존하기 위한 것이다.

## 5. 출력 계약

Operations는 세 가지 파일을 생성한다.

| 파일 | 목적 |
|---|---|
| `final_committee_report.md` | 사람이 읽는 최종 투자위원회 보고서 |
| `telegram_summary.txt` | 짧은 알림용 요약 |
| `operations_report.json` | Operations 자체 실행 결과 |

### 5.1 Markdown 보고서 필수 섹션

`final_committee_report.md`는 다음 섹션을 포함해야 한다.

```text
# Daily Investment Committee Report
## Agent Status
## Candidate Table
## Draft Order Proposals
## Warnings
## Required Human Checks
## Notes
```

필수 메타 정보:

| 항목 | 의미 |
|---|---|
| Run date | 실행일 |
| Candidate count | 후보 수 |
| Portfolio value | 계좌 평가 기준 |
| Status counts | status별 Agent 수 |
| Trader JSON | 주문 제안 JSON 경로 |

후보별 표 필드:

| 필드 | 의미 |
|---|---|
| Ticker | 종목코드 |
| Name | 종목명 |
| Side | Trader 최종 side |
| Amount | Trader 최종 제안 금액 |
| Quantity | Trader 최종 제안 수량 |
| Risk | RiskManager 상태 |
| Compliance | ComplianceOfficer 상태 |
| Main Reason | 핵심 사유 |

현재 구현의 후보별 표는 Ticker, Name, Side, Amount, Risk, Compliance 중심이다. 향후 Quantity와 Main Reason을 추가한다.

### 5.2 Telegram 요약 필수 정보

`telegram_summary.txt`에는 다음이 포함되어야 한다.

| 항목 | 의미 |
|---|---|
| 날짜 | 실행일 |
| 최종 상태 | 전체 투자위원회 status |
| 후보 수 | 검토 후보 개수 |
| draft_buys | Trader 최종 buy 초안 수 |
| holds | Trader 최종 hold 수 |
| 보고서 경로 | Markdown 보고서 경로 |
| 주문 미실행 문구 | 실제 주문이 없었음을 표시 |

예상 형식:

```text
2026-05-08 investment committee: needs_review
candidates=3, draft_buys=0, holds=3
No broker API call was made. Manual review is required.
data/agent_runs/2026-05-08/final_committee_report.md
```

### 5.3 Operations JSON

`operations_report.json` 구조:

```json
{
  "agent": "OperationsReportAgent",
  "status": "needs_review",
  "summary": "Final report written to ...",
  "signals": [],
  "warnings": [],
  "required_human_checks": [],
  "artifacts": {
    "report_path": "...",
    "telegram_summary_path": "..."
  }
}
```

향후 확장 필드:

| 필드 | 의미 |
|---|---|
| `missing_agents` | 필수 Agent 결과 누락 목록 |
| `status_counts` | status별 개수 |
| `candidate_counts` | buy/hold/block 요약 |
| `blocked_by_risk` | Risk로 보류된 후보 수 |
| `blocked_by_compliance` | Compliance로 보류된 후보 수 |
| `report_integrity_status` | JSON과 Markdown 결론 일치 여부 |

## 6. 다른 Agent와의 계약

Operations는 모든 Agent의 결과를 소비하지만 투자 판단을 바꾸지 않는다.

| Agent | 사용하는 정보 | 보고서 반영 |
|---|---|---|
| `QuantSignalAgent` | 후보 수, source, signal_details | 후보 출처와 신호 요약 |
| `EquityResearchAnalystAgent` | analysis_status, missing_items, key_risks | 기업 분석 보완 항목 |
| `ResearchFileAgent` | research_files, quality_status, warnings | 리서치 파일 품질 |
| `PortfolioManagerAgent` | side, suggested_amount, reason | 포트폴리오 관점 초안 |
| `RiskManagerAgent` | status, warnings, artifacts | 리스크 한도와 차단 사유 |
| `ComplianceOfficerAgent` | status, warnings | 준법/기록 차단 사유 |
| `TraderAgent` | OrderProposal signals | 최종 주문 제안 표 |

Operations가 보장해야 하는 것:

- Agent 결과를 숨기지 않는다.
- status와 warnings를 사람이 찾기 쉽게 표시한다.
- JSON과 Markdown이 같은 결론을 가리키게 한다.
- 실제 주문 금지 문구를 항상 포함한다.
- 보고서 경로를 artifacts에 남긴다.

Operations가 보장하지 않는 것:

- 투자 판단의 정확성
- 주문 실행
- 리스크 한도 계산
- 준법 판단의 법률적 확정
- 리서치 내용의 사실 검증
- Telegram 실제 발송

## 7. 사람 확인 항목

Operations 보고서를 읽는 사람이 확인해야 할 항목:

- 최종 status가 `block`, `needs_review`, `approve` 중 무엇인지 확인한다.
- `Candidate Table`에서 `hold` 후보의 Risk/Compliance 사유를 확인한다.
- `Warnings` 섹션에 숨겨진 핵심 차단 사유가 없는지 확인한다.
- `Required Human Checks`를 순서대로 처리한다.
- Analyst 결과가 누락되어 있으면 구현 또는 실행 순서를 보완한다.
- Trader JSON과 Markdown 표가 같은 후보/side/금액을 표시하는지 확인한다.
- 실제 주문은 이 보고서만 보고 실행하지 않는다.

보고서에 반드시 포함될 문구:

```text
No broker API call was made.
Actual order execution is prohibited by this pipeline.
This report is a review aid, not an investment instruction.
```

## 8. 실패/예외 처리

| 상황 | 처리 |
|---|---|
| 후보 없음 | 보고서 생성, 후보 없음 표시, status는 `info` 또는 상위 Agent 상태 따름 |
| Trader 결과 없음 | Candidate Table 대신 Trader missing 경고 |
| AgentResult 일부 없음 | `missing_agents`에 기록, `needs_review` |
| AgentResult JSON 저장 실패 | 파일 경로와 오류를 warnings에 기록 |
| Markdown 생성 실패 | Operations status `block` |
| Telegram 요약 생성 실패 | Markdown은 유지, Operations status `needs_review` |
| warnings 없음 | Warnings 섹션 생략 가능하나 status counts는 유지 |
| human checks 없음 | Required Human Checks 섹션 생략 가능 |
| JSON과 Markdown 불일치 | `report_integrity_status="mismatch"`, `needs_review` |

실패 처리 원칙:

- 보고서 생성 실패 외에는 파이프라인 전체 기록을 최대한 남긴다.
- 실패와 누락은 숨기지 않는다.
- 보고서를 만들기 위해 원본 Agent 결과를 수정하지 않는다.
- 기존 파일 삭제가 필요해 보여도 사용자 승인 없이 삭제하지 않는다.

## 9. 구현 작업 목록

현재 구현 기준에서 Operations 관련 보강 작업은 다음 순서로 진행한다.

1. 필수 Agent 목록을 정의하고 누락 Agent를 보고서에 표시한다.
2. Analyst 결과가 없으면 “not implemented/missing”으로 명확히 표시한다.
3. Candidate Table에 `Quantity`, `Main Reason` 컬럼을 추가한다.
4. Trader 결과에서 `blocked_by_risk`, `blocked_by_compliance` 수를 계산한다.
5. Telegram 요약에 `blocked_by_risk`, `blocked_by_compliance`를 추가한다.
6. `operations_report.json` artifacts에 `status_counts`, `candidate_counts`, `missing_agents`를 추가한다.
7. JSON과 Markdown의 후보 수, buy/hold 수가 일치하는지 integrity check를 추가한다.
8. warnings를 Agent별 섹션으로 그룹화한다.
9. required_human_checks를 중복 제거해 보고서 가독성을 높인다.
10. 최종 보고서 상단에 “자동 주문 미실행” 문구를 더 잘 보이게 배치한다.

v1에서 하지 않는 구현:

- Telegram 실제 발송
- 이메일 발송
- Notion/Slack 자동 업로드
- PDF 변환
- 보고서 기반 자동 주문
- 보고서 내용 자동 수정

## 10. 테스트 시나리오

### 10.1 정상 보고서 생성

조건:

- 모든 AgentResult 존재
- Trader 결과 존재

기대 결과:

- `final_committee_report.md` 생성
- `telegram_summary.txt` 생성
- `operations_report.json` 생성
- artifacts에 두 파일 경로 기록

### 10.2 Risk 차단 후보 포함

조건:

- Risk status가 `block`인 후보 존재
- Trader side는 `hold`

기대 결과:

- 보고서 Candidate Table에 `hold` 표시
- Warnings에 Risk 차단 사유 포함
- 전체 status는 `block`

### 10.3 Compliance 검토 필요 후보 포함

조건:

- Compliance status가 `needs_review`

기대 결과:

- Trader side는 `hold`
- Warnings에 Compliance 사유 포함
- Required Human Checks에 준법/기록 확인 항목 포함

### 10.4 후보 없음

조건:

- candidates 빈 배열

기대 결과:

- 보고서는 생성된다.
- 후보 수 0 표시
- Trader JSON 경로는 있거나 없음이 명확히 표시된다.

### 10.5 Analyst 결과 누락 회귀 방지

조건:

- Analyst Agent가 오류 또는 회귀로 pipeline 결과에서 빠짐

기대 결과:

- 보고서에 Analyst missing 경고 표시
- `missing_agents`에 `EquityResearchAnalystAgent` 기록
- status는 `needs_review`

### 10.6 Telegram 요약 생성

조건:

- Trader 결과에 buy 1개, hold 2개

기대 결과:

- `draft_buys=1`
- `holds=2`
- 주문 미실행 문구 포함

### 10.7 JSON/Markdown 일치성

조건:

- Trader JSON의 후보 수와 Markdown Candidate Table 후보 수가 다름

기대 결과:

- `report_integrity_status="mismatch"`
- Operations status는 `needs_review`

### 10.8 보고서 생성 실패

조건:

- run_dir 쓰기 실패 또는 경로 오류

기대 결과:

- 가능한 경우 warnings에 오류 기록
- Operations status는 `block`
- 원본 AgentResult는 삭제하지 않음

## 11. 성공 기준

`OperationsReportAgent`는 다음 조건을 만족해야 성공이다.

- 날짜별 실행 폴더에 최종 보고서가 생성된다.
- Telegram 요약 파일이 생성된다.
- Operations JSON에 보고서 경로가 기록된다.
- Agent별 status가 보고서에 표시된다.
- 후보별 최종 side, 금액, Risk, Compliance가 표시된다.
- 모든 warnings가 보고서에 남는다.
- 사람이 확인해야 할 항목이 보고서에 남는다.
- 실제 주문 미실행 문구가 항상 포함된다.
- JSON과 Markdown이 같은 결론을 가리킨다.
- 후보가 없거나 데이터가 부족해도 보고서는 생성된다.

## 12. 향후 고도화

v2 이후 개선 후보:

- Markdown 보고서 템플릿 분리
- Agent별 상세 JSON 링크 추가
- Notion 또는 Slack 업로드 옵션
- Telegram 실제 발송 옵션
- 일별 보고서 인덱스 생성
- 주간 운영 요약 생성
- 후보별 이슈 추적 테이블 생성
- 승인/보류 사유의 누적 통계
- 보고서 무결성 체크 자동화

v2에서도 유지할 원칙:

- 보고서는 투자 지시가 아니다.
- 경고와 실패를 숨기지 않는다.
- 실제 주문 실행은 Operations의 책임이 아니다.
- 기록 완전성이 결론의 보기 좋음보다 우선이다.
