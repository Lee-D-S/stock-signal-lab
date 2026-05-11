# FreeAgentPipeline PLAN

작성일: 2026-05-08

## 1. 전문가 역할

`FreeAgentPipeline`은 투자팀의 **투자위원회 의장 겸 orchestrator**다.

역할은 개별 Agent의 판단을 임의로 바꾸는 것이 아니라, 올바른 순서로 실행하고, 각 단계의 결과를 다음 단계에 전달하고, 중간에 `block`이나 `needs_review`가 있어도 최종 보고서까지 완성하는 것이다.

이 Pipeline의 핵심 목표는 자동 주문이 아니다. 목표는 무료 v1 환경에서 후보 발굴, 기업 분석, 리서치 검수, 포트폴리오 판단, 리스크 차단, 준법 차단, 주문안 생성, 운영 보고까지 끊기지 않는 투자위원회 기록을 만드는 것이다.

핵심 책임:

- Agent 실행 순서를 고정한다.
- 각 Agent 결과를 날짜별 JSON으로 저장한다.
- 앞 단계 결과를 필요한 다음 Agent에 전달한다.
- 중간 차단이 있어도 OperationsReportAgent까지 실행한다.
- Risk/Compliance가 승인하지 않은 후보는 Trader에서 `hold`가 되도록 보장한다.
- 브로커 API 호출이 파이프라인에 들어오지 않게 한다.
- 최종 보고서에 실제 주문 금지 문구가 남도록 한다.

하지 말아야 할 일:

- 한 Agent의 결론만 보고 전체 검토를 생략하지 않는다.
- `block` 또는 `needs_review`를 숨기지 않는다.
- Risk와 Compliance를 우회하지 않는다.
- 실제 주문 API를 연결하지 않는다.
- 데이터가 부족한 후보를 임의로 보완해 승인하지 않는다.
- Agent 결과를 덮어써서 보기 좋은 결론으로 바꾸지 않는다.

중요 원칙:

```text
Pipeline은 결론을 좋게 만드는 장치가 아니라, 판단 누락을 막는 운영 장치다.
```

## 2. 목표 실행 순서

목표 v1 구조는 **8개 Agent + 1개 orchestrator**다.

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

실행 순서의 의미:

| 순서 | Agent | 목적 |
|---|---|---|
| 1 | `QuantSignalAgent` | 후보 종목과 신호 출처 표준화 |
| 2 | `EquityResearchAnalystAgent` | 기업 분석, 촉매, 리스크, 반증 조건 정리 |
| 3 | `ResearchFileAgent` | 분석 파일 존재와 품질 검수 |
| 4 | `PortfolioManagerAgent` | 계좌 관점 액션 초안 작성 |
| 5 | `RiskManagerAgent` | 금액, 비중, 현금, 유동성, 한도 검사 |
| 6 | `ComplianceOfficerAgent` | 출처, 리서치, 금지 정보, 기록 요건 검사 |
| 7 | `TraderAgent` | 실제 주문 없는 주문 제안 JSON 생성 |
| 8 | `OperationsReportAgent` | 최종 보고서와 짧은 요약 생성 |

현재 구현 상태:

- 현재 코드에는 `EquityResearchAnalystAgent`가 연결되어 있다.
- 현재 실행 순서는 Quant → Analyst → ResearchFile → Portfolio → Risk → Compliance → Trader → Operations다.
- `equity_research_analyst.json`을 날짜별 실행 폴더에 저장한다.
- ResearchFile, Portfolio, Compliance, Operations는 Analyst 결과를 입력으로 사용할 수 있다.

## 3. 입력 계약

Pipeline은 `AgentContext`를 중심으로 실행된다.

| 필드 | 의미 |
|---|---|
| `run_date` | 투자 판단 기준일(as-of date). 미래 매매 예측일이 아니며, 오늘보다 미래 날짜는 운영 실행에서 차단한다. |
| `candidates` | 수동 입력 또는 CSV 발견 후보 |
| `portfolio` | 보유 종목 목록 |
| `cash` | 주문 가능 현금 또는 현금 추정값 |
| `output_dir` | 실행 결과 저장 루트 |
| `research_root` | 기업별 리서치 파일 루트 |
| `config` | 리스크/준법/기본 금액 설정 |

Pipeline이 직접 생성하거나 보정하는 입력:

| 입력 | 처리 |
|---|---|
| 수동 후보 | CLI `--candidate TICKER[:NAME[:AMOUNT]]`로 생성 |
| 자동 발견 후보 | `--discover` 사용 시 전략 신호 CSV에서 생성 |
| 포트폴리오 | `--portfolio-json`에서 로드 |
| 기본 주문 검토 금액 | `--default-amount` 또는 settings |
| 리서치 필수 여부 | `--no-require-research-file` 반영 |

Pipeline은 입력이 부족해도 실행을 멈추지 않는다. 부족한 입력은 각 Agent가 `needs_review`로 기록해야 한다.

## 4. 출력 계약

실행 결과는 기준일과 실행 ID별 폴더에 저장된다.

기본 경로:

```text
data/agent_runs/YYYY-MM-DD/HHMMSS_microseconds/
```

`YYYY-MM-DD`는 투자 판단 기준일이고, 하위 실행 ID는 같은 기준일에 여러 번 실행해도 결과를 덮어쓰지 않기 위한 값이다.

목표 산출물:

| 파일 | 생성 Agent | 목적 |
|---|---|---|
| `quant_signal.json` | QuantSignalAgent | 후보와 신호 출처 |
| `equity_research_analyst.json` | EquityResearchAnalystAgent | 기업 분석 상태 |
| `research_file.json` | ResearchFileAgent | 리서치 파일 품질 검수 |
| `portfolio_manager.json` | PortfolioManagerAgent | 포트폴리오 액션 초안 |
| `risk_manager.json` | RiskManagerAgent | 리스크 한도 검사 |
| `compliance_officer.json` | ComplianceOfficerAgent | 준법/기록 검사 |
| `trader_order_proposal.json` | TraderAgent | 주문 제안 초안 |
| `final_committee_report.md` | OperationsReportAgent | 최종 보고서 |
| `telegram_summary.txt` | OperationsReportAgent | 짧은 요약 |
| `operations_report.json` | OperationsReportAgent | 운영 보고 결과 |

현재 구현은 `equity_research_analyst.json`을 생성한다.

## 5. Agent 간 데이터 흐름

목표 데이터 흐름:

```text
Candidate[]
  ↓
QuantSignalAgent
  ↓ quant_signal.json
EquityResearchAnalystAgent
  ↓ equity_research_analyst.json
ResearchFileAgent
  ↓ research_file.json
PortfolioManagerAgent
  ↓ portfolio_manager.json
RiskManagerAgent
  ↓ risk_manager.json
ComplianceOfficerAgent
  ↓ compliance_officer.json
TraderAgent
  ↓ trader_order_proposal.json
OperationsReportAgent
  ↓ final_committee_report.md / telegram_summary.txt / operations_report.json
```

중요 연결 계약:

| 연결 | 계약 |
|---|---|
| Quant → Analyst | 후보별 ticker/name/source/signal_details 전달 |
| Analyst → ResearchFile | source_files, missing_items, forbidden_keyword_hits 전달 |
| Analyst → Portfolio | analysis_status, confidence, key_risks 전달 |
| Analyst → Compliance | source_files, forbidden_keyword_hits, analysis_status 전달 |
| ResearchFile → Compliance | research_files, quality_status, missing sections 전달 |
| Portfolio → Trader | side, suggested_amount, reason 전달 |
| Risk → Trader | 후보별 status가 `approve`인지 전달 |
| Compliance → Trader | 후보별 status가 `approve`인지 전달 |
| 모든 Agent → Operations | status, warnings, human checks, artifacts 전달 |

절대 규칙:

```text
Risk status != approve 또는 Compliance status != approve 이면
Trader side는 hold다.
```

## 6. 상태 전파 규칙

Agent status 우선순위:

```text
block > needs_review > approve > info
```

Pipeline은 최종 상태를 직접 낙관적으로 낮추지 않는다.

| 상황 | Pipeline 처리 |
|---|---|
| 특정 Agent `block` | 다음 Agent도 계속 실행하되 최종 보고서에 `block` 반영 |
| 특정 Agent `needs_review` | 다음 Agent도 계속 실행하되 Trader는 Gate 결과에 따라 hold |
| 후보 없음 | 모든 가능한 Agent 실행, Operations 보고서 생성 |
| 데이터 누락 | Agent별 warnings와 human checks로 남김 |
| 예외 발생 | 가능하면 해당 Agent 실패 결과를 만들고 Operations까지 진행 |

Pipeline이 중단해도 되는 경우:

- 실행 폴더 생성 자체가 불가능한 경우
- Python import 또는 코드 구조 오류로 Agent 객체를 만들 수 없는 경우
- 사용자가 명시적으로 중단을 요청한 경우

그 외에는 보고서 생성까지 가는 것이 기본이다.

## 7. 실제 주문 금지 경계

무료 v1 Pipeline은 주문 실행 시스템이 아니다.

금지:

- KIS 주문 API 호출
- 브로커 주문 엔드포인트 호출
- 자동 시장가/지정가 주문
- 자동 정정/취소 주문
- 사람 승인 없는 주문 실행

허용:

- 주문 제안 JSON 생성
- 제안 수량 계산
- 수동 주문 전 체크리스트 생성
- Risk/Compliance 미승인 후보의 hold 기록
- 최종 보고서 생성

Pipeline 수준에서 항상 보장해야 하는 값:

```json
{
  "broker_api_called": false
}
```

이 값은 Trader artifacts와 최종 보고서에 남아야 한다.

## 8. 실패/예외 처리

| 상황 | 처리 |
|---|---|
| 후보가 없음 | 보고서 생성, 후보 없음 경고 |
| discovery root 없음 | 후보 없음으로 처리, 경고 |
| CSV 파싱 실패 | 해당 파일 건너뛰고 경고 |
| portfolio JSON 없음 | portfolio empty/cash 0, Portfolio/Risk가 `needs_review` |
| portfolio JSON 파싱 실패 | 구조화된 경고, 가능하면 실행 지속 |
| research_root 없음 | Research/Compliance가 `needs_review` |
| Analyst 미구현 | missing agent로 기록, 향후 구현 작업에 표시 |
| Risk `block` | Trader hold, Operations까지 실행 |
| Compliance `block` | Trader hold, Operations까지 실행 |
| Trader 결과 없음 | Operations가 missing warning 표시 |
| 보고서 생성 실패 | Operations `block`, 가능한 오류 기록 |

실패 처리 원칙:

- 부족한 데이터는 승인으로 해석하지 않는다.
- 실패한 Agent 결과는 숨기지 않는다.
- 중간 차단은 파이프라인 종료 사유가 아니다.
- 기존 파일은 자동 삭제하지 않는다.
- 삭제가 필요해 보여도 사용자 승인 없이 삭제하지 않는다.

## 9. 구현 작업 목록

현재 구현 기준에서 Pipeline 보강 작업은 다음 순서로 진행한다.

1. Agent 실행 wrapper를 추가해 예외가 나도 실패 결과를 JSON으로 남긴다.
2. `combine_statuses()` 기준을 모든 Agent와 guardrail에서 일관되게 사용한다.
3. 후보 없음, 리서치 없음, 포트폴리오 없음 케이스를 회귀 테스트로 고정한다.
4. `broker_api_called=false`가 항상 유지되는지 테스트한다.
5. Analyst 체크리스트 키워드를 실제 리서치 문서 템플릿과 맞춰 보강한다.
6. ResearchFile/Compliance의 중복 파일 검색을 공통 결과 재사용 구조로 줄인다.
7. Operations 보고서에 Analyst 핵심 리스크와 반증 조건을 더 자세히 표시한다.
8. README 또는 상위 PLAN 인덱스에 현재 구현 상태를 표시한다.

v1에서 하지 않는 구현:

- 실제 주문 API 연결
- 유료 LLM API 호출
- 실시간 뉴스/공시 크롤링
- 자동 목표가 생성
- 포트폴리오 최적화 엔진
- 사람 승인 토큰 기반 실제 실행
- Slack/Telegram 실제 발송

## 10. CLI 운영 계획

기본 실행:

```powershell
rtk python scripts/run_free_agent_pipeline.py --candidate 005930:삼성전자:100000
```

기준일을 명시하는 실행:

```powershell
rtk python scripts/run_free_agent_pipeline.py --as-of-date 2026-05-08 --candidate 005930:삼성전자:100000
```

CSV 자동 후보 발견:

```powershell
rtk python scripts/run_free_agent_pipeline.py --discover --discover-limit 10
```

포트폴리오 포함 실행:

```powershell
rtk python scripts/run_free_agent_pipeline.py --discover --portfolio-json data/portfolio_snapshot.json
```

리서치 파일 필수 조건 해제:

```powershell
rtk python scripts/run_free_agent_pipeline.py --candidate 005930:삼성전자 --no-require-research-file
```

운영 원칙:

- 기본은 리서치 파일 필수다.
- 포트폴리오 JSON이 없으면 매수 판단은 대부분 `needs_review`가 된다.
- `--no-require-research-file`은 테스트/탐색용이며, 실전 검토에서는 기본값을 유지한다.
- `--date`와 `--as-of-date`는 같은 옵션이며, 투자 판단 기준일을 뜻한다.
- 미래 날짜는 미래 시세나 미래 매매 신호를 의미하지 않으므로 기본 실행에서 차단한다.
- 명령 실행 후 `run_dir` 경로와 Agent별 status를 확인한다.

## 11. 테스트 시나리오

### 11.1 수동 후보 1개

조건:

```powershell
rtk python scripts/run_free_agent_pipeline.py --candidate 005930:삼성전자:100000
```

기대 결과:

- 날짜별 run_dir 생성
- Quant, Research, Portfolio, Risk, Compliance, Trader, Operations JSON 생성
- `equity_research_analyst.json` 생성
- 실제 주문 API 호출 없음

### 11.2 후보 없음

조건:

```powershell
rtk python scripts/run_free_agent_pipeline.py
```

기대 결과:

- 보고서 생성
- 후보 수 0 표시
- Trader proposal 빈 배열
- 최종 status는 `info` 또는 `needs_review`

### 11.3 CSV discovery

조건:

```powershell
rtk python scripts/run_free_agent_pipeline.py --discover --discover-limit 5
```

기대 결과:

- 최근 전략 신호 CSV에서 후보 생성
- 후보별 source_file과 source_type 기록
- 읽을 수 없는 CSV는 경고 또는 건너뛰기

### 11.4 포트폴리오 없음

조건:

- 후보는 있음
- `--portfolio-json` 없음

기대 결과:

- Portfolio 또는 Risk가 `needs_review`
- Trader는 `hold`
- 보고서에 포트폴리오 스냅샷 필요 경고

### 11.5 리서치 파일 없음

조건:

- 후보와 매칭되는 리서치 파일 없음
- `require_research_file=true`

기대 결과:

- ResearchFile `needs_review`
- Compliance `needs_review`
- Trader `hold`

### 11.6 Risk block

조건:

- 후보 suggested_amount가 `max_order_amount` 초과

기대 결과:

- Risk `block`
- Trader `hold`
- Operations 최종 상태 `block`
- 최종 보고서에 차단 사유 표시

### 11.7 Compliance block

조건:

- ticker 형식 오류 또는 명백한 금지 정보 의존

기대 결과:

- Compliance `block`
- Trader `hold`
- Operations 최종 상태 `block`

### 11.8 실제 주문 미호출

조건:

- 모든 Gate 통과 가능 데이터 제공

기대 결과:

- Trader proposal은 생성되지만 실제 주문 없음
- `broker_api_called=false`
- 최종 보고서에 실제 주문 금지 문구 포함

### 11.9 Analyst 추가 이후

조건:

- Analyst 구현 완료
- 후보와 리서치 파일 존재

기대 결과:

- `equity_research_analyst.json` 생성
- Research/Portfolio/Compliance가 Analyst 결과를 사용
- Operations 보고서에 Analyst 섹션 포함

## 12. 성공 기준

`FreeAgentPipeline`은 다음 조건을 만족해야 성공이다.

- 8개 Agent 목표 구조가 명확하다.
- Analyst가 포함된 현재 구현 상태가 명확하다.
- 모든 Agent 결과가 날짜별 JSON으로 저장된다.
- 중간 차단이 있어도 최종 보고서가 생성된다.
- Risk 또는 Compliance가 `approve`가 아니면 Trader가 `hold`를 만든다.
- 후보 없음, 리서치 없음, 포트폴리오 없음 상황에서도 파이프라인이 기록을 남긴다.
- 실제 주문 API를 호출하지 않는다.
- 최종 보고서에 주문 미실행 문구가 포함된다.
- OpenAI API 키 없이 실행된다.
- 다음 구현 작업이 실패 결과 표준화와 회귀 테스트 보강부터 시작된다는 점이 명확하다.

## 13. 향후 고도화

v2 이후 개선 후보:

- Agent 실행 wrapper와 실패 JSON 표준화
- `pipeline_manifest.json` 생성
- Agent별 입력/출력 schema 파일 분리
- `policy/` 폴더에 risk/compliance policy JSON 도입
- DataQualityAgent 추가
- MacroRegimeAgent 추가
- PerformanceReviewAgent 추가
- 보고서 무결성 검사 자동화
- 수동 승인 토큰 기반 별도 실행 파이프라인 검토

v2에서도 유지할 원칙:

- Pipeline은 Agent 판단을 숨기지 않는다.
- 중간 차단이 있어도 기록은 완성한다.
- 실제 주문 실행은 기본 Pipeline에 넣지 않는다.
- 사람의 최종 검토가 투자 판단의 마지막 단계다.
