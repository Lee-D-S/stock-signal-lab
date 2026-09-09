# StrategyDecisionAgent PLAN

작성일: 2026-05-12

## 1. 전문가 역할

`StrategyDecisionAgent`는 투자팀의 **전략 판단 리서처**다.

역할은 Quant가 표준화한 후보 신호를 실제 투자 검토 언어로 해석하는 것이다. 이 Agent는 “무엇을 주문할지”를 확정하지 않는다. 대신 후보별로 매수 후보인지, 반등 감시 후보인지, 회피 후보인지, 보유 종목 청산 감시 후보인지, 아니면 전략 해석이 불충분한 보류 후보인지를 분리한다.

핵심 책임:

- Quant 후보의 `signal_details`에서 조건명, `hypothesis_id`, source file을 읽는다.
- 전략 조건 CSV와 후보를 매칭한다.
- `action_hint`를 표준 `decision_type`으로 변환한다.
- 기대수익, hit rate, 진입 규칙, 계획 보유기간을 기록한다.
- 회피/청산 감시 후보를 신규 매수 후보와 분리한다.
- 전략 해석의 표본 부족, 오래된 조건, 매칭 실패를 경고한다.

하지 말아야 할 일:

- 매수, 매도, 보유를 최종 승인하지 않는다.
- 포트폴리오 비중이나 현금을 판단하지 않는다.
- Risk/Compliance 통과를 가정하지 않는다.
- 기대수익을 수익 보장처럼 표현하지 않는다.
- 회피 후보를 매수 후보로 바꾸지 않는다.

## 2. 실행 위치

목표 실행 순서:

```text
QuantSignalAgent
→ StrategyDecisionAgent
→ EquityResearchAnalystAgent
→ ResearchFileAgent
→ PortfolioManagerAgent
→ RiskManagerAgent
→ ComplianceOfficerAgent
→ TraderAgent
→ OperationsReportAgent
```

이 Agent가 Quant 다음에 필요한 이유:

- Quant는 후보와 출처를 표준화하지만 전략 방향을 확정하지 않는다.
- `H01` 같은 매수형 조건과 `H04/H06` 같은 회피형 조건이 같은 Candidate로 섞이면 Portfolio가 오해할 수 있다.
- Portfolio는 “전략적으로 살 만한 후보인지”와 “내 계좌에 넣을 수 있는지”를 분리해서 판단해야 한다.

## 3. 입력 계약

### 3.1 Quant 후보

사용 필드:

| 필드 | 용도 |
|---|---|
| `ticker` | 후보 식별 |
| `name` | 보고서 표시 |
| `source` | 대표 신호 파일 |
| `source_type` | watchlist, new_condition, foreign_flow, candlestick 등 구분 |
| `score` | 조건 점수 보조 참고 |
| `signal_details` | 조건명, 신호 날짜, 원천 파일 매칭 |
| `current_price` | 전략 해석 보조 정보 |
| `trade_amount` | 전략 해석 보조 정보 |

### 3.2 전략 조건 파일

우선 입력:

```text
ai 주가 변동 원인 분석/07_전략신호/05_전략/전략_조건_초안.csv
ai 주가 변동 원인 분석/07_전략신호/02_신규조건/신규조건_전략_조건.csv
ai 주가 변동 원인 분석/08_관찰기록/00_공통/관찰_성과_요약.csv
```

주요 컬럼:

| 컬럼 | 의미 |
|---|---|
| `hypothesis_id` | 조건 ID |
| `use_type` | 매수 후보, 반등 감시 후보, 참고 회피 후보 등 |
| `action_hint` | 전략 행동 힌트 |
| `suggested_response` | 사람이 읽을 대응 설명 |
| `preferred_entry_mode` | 진입 규칙 |
| `preferred_hold_days` | 계획 보유기간 |
| `tested_trades` | 백테스트 표본 수 |
| `avg_score_return_pct` | 조건 기반 평균 점수 수익률 |
| `hit_rate` | 조건 기반 hit rate |
| `risk_note` | 조건별 주의점 |

## 4. 처리 프로세스

```text
1. Quant 후보별 signal_details를 읽는다.
2. condition, hypothesis_id, source file에서 조건 ID를 추출한다.
3. 전략 조건 CSV를 최신 파일 우선으로 로드한다.
4. 후보와 전략 조건을 매칭한다.
5. action_hint/use_type을 decision_type으로 표준화한다.
6. 기대수익, hit rate, 진입 규칙, 계획 보유기간을 기록한다.
7. 표본 부족, 오래된 조건, 매칭 실패를 경고한다.
8. strategy_decision.json을 생성한다.
```

매칭 우선순위:

1. 후보 `signal_details[].condition`이 전략 조건의 `hypothesis_id`와 일치
2. 후보 행의 `hypothesis_id` 컬럼과 일치
3. source file 이름과 source_type 기반 보조 분류
4. 매칭 실패 시 `decision_type="hold"`와 `needs_review`

## 5. 판정 기준

표준 `decision_type`:

| decision_type | 의미 | Portfolio 기본 처리 |
|---|---|---|
| `buy_candidate` | 전략상 신규 매수 검토 가능 | 분석/리서치/계좌 조건 충족 시 buy 초안 가능 |
| `rebound_watch` | 반등 확인 후 검토 | 기본 hold, 사람 확인 필요 |
| `avoid` | 신규 매수 회피 | 신규 매수 초안 금지 |
| `exit_watch` | 기존 보유 청산 또는 축소 감시 | 신규 매수 초안 금지, 보유 종목이면 축소 검토 |
| `hold` | 전략 해석 불충분 또는 중립 | 기본 hold |

`action_hint` 변환 예:

| action_hint/use_type | decision_type |
|---|---|
| `진입 후보`, `매수 후보` | `buy_candidate` |
| `반등 관찰 후보`, `반등 감시 후보` | `rebound_watch` |
| `추격매수 회피 후보`, `참고 회피 후보` | `avoid` |
| `청산 감시`, `축소 검토` | `exit_watch` |
| 기타 또는 매칭 실패 | `hold` |

## 6. 출력 계약

StrategyDecision 결과는 `strategy_decision.json`으로 저장한다.

공통 구조:

```json
{
  "agent": "StrategyDecisionAgent",
  "status": "info",
  "summary": "N개 후보의 전략 방향을 해석했습니다.",
  "signals": [],
  "warnings": [],
  "required_human_checks": [],
  "artifacts": {
    "strategy_file_count": 0,
    "decision_type_counts": {}
  }
}
```

각 `signals[]` 항목:

| 필드 | 의미 |
|---|---|
| `ticker` | 종목코드 |
| `name` | 종목명 |
| `decision_type` | 표준 전략 방향 |
| `hypothesis_id` | 매칭된 조건 ID |
| `action_hint` | 원천 전략 행동 힌트 |
| `entry_rule` | 진입 규칙 |
| `planned_hold_days` | 계획 보유기간 |
| `expected_return_pct` | 조건 기반 기대수익 |
| `hit_rate` | 조건 기반 hit rate |
| `tested_trades` | 백테스트 표본 수 |
| `exit_rule` | 청산/무효화 감시 조건 |
| `confidence` | high, medium, low, none |
| `reason` | 사람이 읽을 전략 해석 |
| `source_file` | 매칭에 사용한 전략 조건 파일 |

## 7. 다른 Agent와의 계약

| 다음 Agent | 사용하는 정보 | 목적 |
|---|---|---|
| `EquityResearchAnalystAgent` | decision_type, hypothesis_id, risk_note | 기업 분석에서 볼 전략 맥락 제공 |
| `PortfolioManagerAgent` | decision_type, expected_return_pct, hit_rate, planned_hold_days, entry_rule | 신규 매수/회피/청산 감시 구분 |
| `RiskManagerAgent` | planned_hold_days, decision_type | 단기/중기 리스크 검토 참고 |
| `TraderAgent` | Portfolio가 반영한 side와 reason | 직접 사용보다 Portfolio 결과를 통해 반영 |
| `OperationsReportAgent` | 모든 전략 해석 결과 | 최종 보고서의 후보 사유 표시 |

절대 규칙:

```text
decision_type != buy_candidate 이면
PortfolioManagerAgent는 신규 매수 초안을 만들지 않는다.
```

단, 기존 보유 종목이 `exit_watch`이면 Portfolio는 `trim_review` 또는 `hold` 검토 사유를 남길 수 있다. 실제 매도 주문안은 Risk/Compliance/Trader를 통과해야 한다.

## 8. 사람 확인 항목

- 기대수익과 hit rate가 과거 조건 성과일 뿐 수익 보장이 아님을 확인한다.
- `tested_trades`가 너무 적은 조건은 자동 매수 후보로 보지 않는다.
- `rebound_watch`는 반등 확인 전 신규 매수로 해석하지 않는다.
- `avoid`와 `exit_watch`는 신규 매수 후보와 분리한다.
- 전략 조건 파일이 최신인지 확인한다.
- 후보 source와 매칭된 `hypothesis_id`가 실제로 맞는지 확인한다.

## 9. 실패/예외 처리

| 상황 | 처리 |
|---|---|
| 전략 조건 파일 없음 | 후보별 `decision_type=hold`, status `needs_review` |
| 후보에 조건 ID 없음 | source_type 기반 보조 분류, 실패 시 `hold` |
| 매칭 조건 없음 | `hold`, 매칭 실패 warning |
| `preferred_hold_days` 없음 | `null`, 사람 확인 |
| `avg_score_return_pct` 없음 | `null`, 기대수익 없음 warning |
| `hit_rate` 없음 | `null`, hit rate 없음 warning |
| 표본 수 부족 | confidence `low` |
| 회피 조건 | 신규 매수 후보로 넘기지 않음 |

## 10. 구현 작업 목록

1. `core/agents/free_pipeline.py`에 `StrategyDecisionAgent` 클래스를 추가한다.
2. `FreeAgentPipeline.run()`에서 Quant 다음에 실행하고 `strategy_decision.json`을 저장한다.
3. 전략 조건 CSV 로더를 추가한다.
4. `signal_details`에서 `hypothesis_id`/condition 추출 함수를 만든다.
5. `action_hint`를 `decision_type`으로 변환하는 함수를 만든다.
6. StrategyDecision 결과를 Portfolio 입력으로 전달한다.
7. Portfolio가 `decision_type != buy_candidate`인 신규 후보를 `hold`로 처리하게 한다.
8. Operations 보고서에 전략 방향, 기대수익, 보유기간 컬럼을 추가한다.
9. 후보 매칭 실패, 회피 조건, 표본 부족 테스트를 추가한다.

v1에서 하지 않는 구현:

- 실시간 기대수익 예측 모델을 만들지 않는다.
- 목표가를 자동 생성하지 않는다.
- 자동 매도 주문을 만들지 않는다.
- 백테스트를 매번 새로 실행하지 않는다.
- 전략 조건을 자동 승격하지 않는다.

## 11. 테스트 시나리오

### 11.1 H01 매수 후보

조건:

- 후보 `signal_details.condition == "H01"`
- 전략 조건 파일에 H01이 있고 `action_hint="진입 후보"`

기대 결과:

- `decision_type == "buy_candidate"`
- `planned_hold_days`가 채워진다.
- `expected_return_pct`, `hit_rate`가 기록된다.

### 11.2 H02 반등 감시 후보

조건:

- H02가 `반등 관찰 후보` 또는 `반등 감시 후보`

기대 결과:

- `decision_type == "rebound_watch"`
- Portfolio는 신규 매수 초안을 만들지 않는다.
- 보고서에 반등 확인 필요가 표시된다.

### 11.3 H04/H06 회피 후보

조건:

- H04 또는 H06이 추격매수 회피 후보

기대 결과:

- `decision_type == "avoid"`
- Portfolio 신규 매수 초안 금지
- Trader까지 가더라도 기본 `hold`

### 11.4 조건 매칭 실패

조건:

- 후보에 조건 ID가 없고 source_type만 있음

기대 결과:

- `decision_type == "hold"`
- warnings에 매칭 실패 포함
- status는 `needs_review`

## 12. 성공 기준

`StrategyDecisionAgent`는 다음 조건을 만족해야 성공이다.

- Quant 후보를 매수/반등감시/회피/청산감시/보류로 분리한다.
- 기대수익, hit rate, 보유기간이 후보별로 기록된다.
- 회피 후보가 신규 매수 후보로 승격되지 않는다.
- 매칭 실패는 조용히 통과하지 않고 warning으로 남는다.
- Portfolio가 전략 방향을 입력으로 사용한다.
- 실제 주문 API를 호출하지 않는다.

## 13. 향후 고도화

v2 이후 개선 후보:

- 관찰 성과 요약을 반영한 confidence 재계산
- 조건별 최신 성과 decay 적용
- 시장 국면별 조건 유효성 태그 추가
- 기존 보유 종목 대상 `exit_watch` 강화
- 조건 간 충돌 감지
- `PerformanceReviewAgent`와 연결해 전략 승격/강등 기록 생성

v2에서도 유지할 원칙:

- StrategyDecision은 전략 해석자이지 주문 승인자가 아니다.
- 기대수익은 과거 조건 성과이며 수익 보장이 아니다.
- 회피/청산 감시와 신규 매수 후보는 반드시 분리한다.
