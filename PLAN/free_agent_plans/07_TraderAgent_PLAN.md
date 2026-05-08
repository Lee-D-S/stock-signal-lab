# TraderAgent PLAN

작성일: 2026-05-08

## 1. 전문가 역할

`TraderAgent`는 투자팀의 **집행 트레이더/주문 제안 책임자**다.

역할은 실제 주문을 실행하는 것이 아니라, 앞 단계 Agent들의 결론을 모아 사람이 검토할 수 있는 주문 제안서를 만드는 것이다. 이 Agent는 브로커 API를 호출하지 않는다. 모든 결과는 `trader_order_proposal.json`과 최종 보고서에 남는 초안이다.

핵심 책임:

- PortfolioManager가 제안한 방향을 기본 주문 방향으로 사용한다.
- RiskManager 상태가 `approve`가 아니면 반드시 `hold`로 낮춘다.
- ComplianceOfficer 상태가 `approve`가 아니면 반드시 `hold`로 낮춘다.
- 현재가가 있으면 제안 수량을 계산한다.
- 현재가가 없거나 0 이하이면 수량을 0으로 둔다.
- 주문 제안의 필수 필드가 비어 있는지 검사한다.
- 사람이 실제 주문 전에 확인해야 할 체크리스트를 남긴다.
- 브로커 API 미호출 사실을 `broker_api_called: false`로 기록한다.

하지 말아야 할 일:

- 실제 주문 API를 호출하지 않는다.
- Risk 또는 Compliance의 `needs_review`를 매수 가능 상태로 해석하지 않는다.
- `block` 후보를 사람이 보기 좋게 숨기지 않는다.
- 가격이 없는데 임의 가격으로 수량을 계산하지 않는다.
- 시장가/지정가 전략을 자동 확정하지 않는다.
- 슬리피지나 호가 상황을 추정해서 주문을 실행하지 않는다.
- 사람 검토 없이 주문 가능 상태를 만들지 않는다.

중요 원칙:

```text
TraderAgent는 마지막 실행 Gate지만, 실제 실행자는 아니다.
```

## 2. 입력 계약

### 2.1 후보 입력

Trader는 후보 자체의 가격과 금액 정보를 사용한다.

| 필드 | 용도 |
|---|---|
| `ticker` | 주문 제안 종목코드 |
| `name` | 주문 제안 종목명 |
| `suggested_amount` | 주문 제안 금액 |
| `current_price` | 수량 계산 |
| `source` | 주문 근거 추적 참고 |

### 2.2 PortfolioManager 입력

Portfolio 결과는 기본 주문 방향과 사유를 제공한다.

| 필드 | 용도 |
|---|---|
| `ticker` | 후보 매칭 |
| `side` | 기본 주문 방향: `buy`, `sell`, `hold` |
| `suggested_amount` | 포트폴리오 관점 금액 |
| `reason` | 주문 제안 사유 |
| `action_detail` | 신규 매수, 기존 보유 검토 등 세부 상태 |
| `priority` | 보고서 정렬 또는 참고 우선순위 |

Portfolio가 `buy`를 제안해도 Risk와 Compliance를 통과하지 못하면 Trader는 `hold`로 낮춘다.

### 2.3 RiskManager 입력

Risk 결과는 주문 가능 여부를 결정하는 필수 Gate다.

| 필드 | 용도 |
|---|---|
| `ticker` | 후보 매칭 |
| `status` | `approve`가 아니면 `hold` |
| `warnings` | hold 사유와 사람 확인 항목 |

Risk status 처리:

| Risk status | Trader 처리 |
|---|---|
| `approve` | Compliance도 approve일 때만 Portfolio side 유지 가능 |
| `needs_review` | `hold` |
| `block` | `hold` |
| `info` | `hold` |

### 2.4 ComplianceOfficer 입력

Compliance 결과도 주문 가능 여부를 결정하는 필수 Gate다.

| 필드 | 용도 |
|---|---|
| `ticker` | 후보 매칭 |
| `status` | `approve`가 아니면 `hold` |
| `warnings` | 근거/기록/출처 문제 표시 |

Compliance status 처리:

| Compliance status | Trader 처리 |
|---|---|
| `approve` | Risk도 approve일 때만 Portfolio side 유지 가능 |
| `needs_review` | `hold` |
| `block` | `hold` |
| `info` | `hold` |

### 2.5 설정 입력

| 설정 | 의미 |
|---|---|
| `default_suggested_amount` | 후보에 금액이 없을 때 사용할 기본 검토 금액 |

Trader는 Risk 한도 설정을 직접 재검사하지 않는다. RiskManager가 이미 계산한 결과를 따른다.

## 3. 처리 프로세스

후보별 처리 순서:

```text
1. 후보 ticker/name 확인
2. PortfolioManager signal 매칭
3. RiskManager status 매칭
4. ComplianceOfficer status 매칭
5. Portfolio side를 기본 side로 설정
6. Risk 또는 Compliance가 approve가 아니면 side를 hold로 변경
7. suggested_amount 결정
8. current_price가 있으면 suggested_quantity 계산
9. hold이면 amount와 quantity를 0으로 설정
10. OrderProposal 생성
11. 주문 제안 필수 필드 검사
12. 후보별 warnings와 human_checklist 기록
13. 전체 Trader status 계산
14. broker_api_called=false 기록
```

수량 계산:

```text
if side != "hold" and current_price > 0:
    suggested_quantity = floor(suggested_amount / current_price)
else:
    suggested_quantity = 0
```

hold 강등 규칙:

```text
if risk_status != "approve":
    side = "hold"

if compliance_status != "approve":
    side = "hold"
```

## 4. 판정 기준

| Status | 조건 |
|---|---|
| `approve` | 생성된 주문 제안이 구조적으로 완전하고 Risk/Compliance를 모두 통과했다. |
| `needs_review` | 하나 이상의 주문안이 `hold`이거나, 필수 확인 항목이 있거나, 수량 계산이 불완전하다. |
| `block` | 주문 제안 구조가 깨졌거나 허용되지 않는 side가 있다. |
| `info` | 후보 없음 등 주문 제안이 없는 정보성 상황. |

Trader의 `approve`는 “자동 주문 가능”이 아니다. 의미는 다음과 같다.

```text
사람이 검토할 수 있는 주문 초안이 구조적으로 완전하다.
```

`needs_review` 조건:

- Risk status가 `approve`가 아니다.
- Compliance status가 `approve`가 아니다.
- Portfolio side가 `hold`다.
- 현재가가 없어 수량을 계산할 수 없다.
- 제안 수량이 0이다.
- 주문 사유가 비어 있다.
- human checklist가 비어 있다.

`block` 조건:

- side가 `buy`, `sell`, `hold` 중 하나가 아니다.
- 필수 주문 필드가 구조적으로 누락되어 JSON 계약이 깨졌다.
- ticker가 비어 있어 주문 제안을 식별할 수 없다.

## 5. 출력 계약

Trader 결과는 `trader_order_proposal.json`으로 저장된다.

공통 구조:

```json
{
  "agent": "TraderAgent",
  "status": "needs_review",
  "summary": "1 draft order proposal(s) generated without execution.",
  "signals": [],
  "warnings": [],
  "required_human_checks": [
    "This pipeline never executes orders. Entering an order is a separate manual action."
  ],
  "artifacts": {
    "broker_api_called": false
  }
}
```

각 `signals[]` 항목은 `OrderProposal`이다.

| 필드 | 의미 |
|---|---|
| `ticker` | 종목코드 |
| `name` | 종목명 |
| `side` | `buy`, `sell`, `hold` |
| `suggested_amount` | 제안 금액. `hold`이면 0 |
| `suggested_quantity` | 제안 수량. `hold`이면 0 |
| `order_type_hint` | 사람이 검토할 주문 방식 힌트 |
| `reason` | 주문 제안 사유 |
| `risk_status` | RiskManager 최종 상태 |
| `compliance_status` | ComplianceOfficer 최종 상태 |
| `human_checklist` | 실제 주문 전 확인 항목 |

현재 주문 방식 힌트:

```text
manual_review_limit_order
```

향후 확장 필드:

| 필드 | 의미 |
|---|---|
| `portfolio_side` | PortfolioManager 원래 제안 방향 |
| `final_side_reason` | hold 또는 buy 최종 결정 사유 |
| `price_used` | 수량 계산에 사용한 가격 |
| `price_source` | 가격 출처 |
| `risk_warnings` | Risk 후보별 경고 |
| `compliance_warnings` | Compliance 후보별 경고 |
| `execution_allowed` | 항상 false 또는 수동 승인 이후 별도 정책 |

## 6. 다른 Agent와의 계약

| Agent | 사용하는 정보 | 목적 |
|---|---|---|
| `PortfolioManagerAgent` | side, suggested_amount, reason | 기본 주문 초안 |
| `RiskManagerAgent` | status, warnings | 리스크 Gate |
| `ComplianceOfficerAgent` | status, warnings | 준법/기록 Gate |
| `OperationsReportAgent` | OrderProposal signals | 최종 보고서와 Telegram 요약 |

Trader가 보장해야 하는 것:

- Risk 또는 Compliance가 `approve`가 아니면 `hold`다.
- `hold` 주문안은 금액과 수량이 0이다.
- 실제 주문 API는 호출하지 않는다.
- 주문 제안은 JSON으로 남는다.
- 사람이 실제 주문 전에 확인할 체크리스트가 남는다.

Trader가 보장하지 않는 것:

- 주문 체결
- 최적 호가
- 슬리피지 최소화
- 실시간 가격 정확성
- 세금/수수료 반영
- 장중 거래 가능 여부
- 자동 주문 실행

## 7. 사람 확인 항목

Trader 단계에서 사람이 확인해야 할 항목:

- Risk status가 `approve`인지 확인한다.
- Compliance status가 `approve`인지 확인한다.
- 종목코드와 종목명이 실제 주문 화면과 일치하는지 확인한다.
- 현재가와 호가가 최신인지 확인한다.
- 제안 수량이 주문 가능 단위와 맞는지 확인한다.
- 지정가를 얼마로 넣을지 직접 판단한다.
- 장중 유동성과 호가 공백을 확인한다.
- 주문 전 최종 리서치와 반증 조건을 확인한다.
- `hold` 후보를 실수로 주문하지 않도록 확인한다.

필수 안내 문구:

```text
This pipeline never executes orders. Entering an order is a separate manual action.
```

## 8. 실패/예외 처리

| 상황 | 처리 |
|---|---|
| 후보 없음 | `info`, 주문 제안 없음 |
| Portfolio signal 없음 | 기본 `hold`, reason에 초안 없음 기록 |
| Risk signal 없음 | `risk_status="needs_review"`, `hold` |
| Compliance signal 없음 | `compliance_status="needs_review"`, `hold` |
| Risk `block` | `hold`, warnings에 차단 사유 기록 |
| Compliance `block` | `hold`, warnings에 차단 사유 기록 |
| 현재가 없음 | `hold`가 아니어도 수량 0, `needs_review` |
| 현재가 <= 0 | 수량 0, `needs_review` |
| suggested_amount 없음 | `default_suggested_amount` 사용 |
| side가 허용값 아님 | `block`, side는 `hold`로 방어 |
| 주문 proposal 필드 누락 | `needs_review` 또는 `block` |

실패 처리 원칙:

- 누락 데이터는 주문 가능으로 해석하지 않는다.
- 보류 사유는 JSON과 최종 보고서에 남긴다.
- 오류가 있어도 브로커 API를 호출하지 않는다.
- 주문 실행은 항상 별도 수동 행동이다.

## 9. 구현 작업 목록

현재 구현 기준에서 Trader 관련 보강 작업은 다음 순서로 진행한다.

1. `OrderProposal`에 `portfolio_side`, `final_side_reason`, `price_used` 필드를 추가한다.
2. Risk와 Compliance의 후보별 warnings를 Trader signals에 함께 포함한다.
3. `check_order_proposal_rules()`가 `hold` 주문안의 빈 필드를 과도하게 경고하지 않도록 기준을 정교화한다.
4. 후보 없음일 때 Trader status를 `info`로 명확히 처리한다.
5. 현재가가 없어서 수량 계산이 안 된 경우 경고를 더 구체화한다.
6. 최종 보고서에 Portfolio 원래 제안과 Trader 최종 side를 함께 표시한다.
7. Telegram 요약에 `draft_buys`, `holds`, `blocked_by_risk`, `blocked_by_compliance`를 표시한다.
8. `broker_api_called`는 항상 false로 유지하고 테스트로 고정한다.

v1에서 하지 않는 구현:

- KIS 주문 API 호출
- 자동 시장가 주문
- 자동 지정가 산출
- 장중 호가 기반 체결 전략
- 자동 재시도 주문
- 분할 주문
- 주문 체결 확인

## 10. 테스트 시나리오

### 10.1 Risk/Compliance 모두 승인

조건:

- Portfolio side: `buy`
- Risk status: `approve`
- Compliance status: `approve`
- current_price 존재

기대 결과:

- Trader side는 `buy`
- suggested_amount는 0보다 큼
- suggested_quantity는 1 이상
- `broker_api_called == false`

### 10.2 Risk needs_review

조건:

- Portfolio side: `buy`
- Risk status: `needs_review`
- Compliance status: `approve`

기대 결과:

- Trader side는 `hold`
- suggested_amount는 0
- suggested_quantity는 0
- warnings에 Risk 미승인 사유 포함

### 10.3 Compliance block

조건:

- Portfolio side: `buy`
- Risk status: `approve`
- Compliance status: `block`

기대 결과:

- Trader side는 `hold`
- warnings에 Compliance 미승인 사유 포함

### 10.4 현재가 없음

조건:

- Risk/Compliance 모두 `approve`
- Portfolio side: `buy`
- current_price 없음

기대 결과:

- side 정책은 구현에 따라 `buy` 초안일 수 있으나 suggested_quantity는 0
- status는 `needs_review`
- 사람이 가격과 수량을 확인해야 한다는 경고 포함

### 10.5 Portfolio signal 없음

조건:

- 후보는 있으나 PortfolioManager 결과에 해당 ticker 없음

기대 결과:

- Trader side는 `hold`
- reason에 Portfolio 초안 없음 또는 rule-based draft 문구 포함
- status는 `needs_review`

### 10.6 허용되지 않는 side

조건:

- Portfolio signal side가 `strong_buy` 등 허용되지 않는 값

기대 결과:

- Trader side는 `hold`
- proposal rule warning 발생

### 10.7 후보 없음

조건:

- candidates 빈 배열

기대 결과:

- proposals 빈 배열
- status는 `info`
- 브로커 API 호출 없음

### 10.8 실제 주문 미호출 고정

조건:

- 모든 Gate 통과

기대 결과:

- `artifacts.broker_api_called == false`
- 주문 관련 외부 API 호출 없음

## 11. 성공 기준

`TraderAgent`는 다음 조건을 만족해야 성공이다.

- Risk 또는 Compliance가 `approve`가 아니면 항상 `hold`다.
- 실제 주문 API를 호출하지 않는다.
- 모든 주문 제안은 JSON에 남는다.
- `hold` 제안은 금액과 수량이 0이다.
- 현재가가 있을 때 수량 계산이 된다.
- 현재가가 없으면 사람이 확인해야 할 경고가 남는다.
- 최종 보고서에 후보별 side, 금액, 수량, Risk, Compliance 상태가 표시된다.
- `broker_api_called: false`가 항상 기록된다.
- 사람이 실제 주문 전에 확인할 체크리스트가 남는다.

## 12. 향후 고도화

v2 이후 개선 후보:

- 가격 출처와 시각 기록
- 호가 단위 기반 지정가 힌트
- 분할 주문 제안 초안
- 장중 유동성/스프레드 경고
- 주문 전 체크리스트 Markdown 생성
- 수동 승인 토큰 없이는 어떤 실행도 불가능한 구조 추가
- 체결 후 사후 기록 Agent와 연결

v2에서도 유지할 원칙:

- 자동 주문 실행은 기본값이 아니다.
- 승인되지 않은 후보는 `hold`다.
- Trader는 앞 단계 Gate를 우회하지 않는다.
- 실제 주문은 별도 수동 행동으로 남긴다.
