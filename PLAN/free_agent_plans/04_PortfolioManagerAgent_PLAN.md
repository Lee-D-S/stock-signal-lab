# PortfolioManagerAgent PLAN

작성일: 2026-05-08

## 1. 전문가 역할

`PortfolioManagerAgent`는 투자팀의 **포트폴리오 매니저**다.

역할은 개별 후보가 좋아 보이는지 판단하는 것이 아니라, 현재 계좌 전체 구조에서 그 후보를 어떻게 다뤄야 하는지 초안을 만드는 것이다. 이 Agent는 매수/매도 실행 권한이 없다. Quant가 가져온 후보를 현재 보유 종목, 현금, 포트폴리오 가치, 후보 우선순위와 비교해 “검토 방향”을 만든다.

핵심 책임:

- 신규 후보와 기존 보유 종목을 구분한다.
- 후보별 검토 액션 초안을 만든다.
- 현금과 포트폴리오 가치가 충분히 제공됐는지 확인한다.
- 후보 점수와 신호 수를 기반으로 우선순위를 기록한다.
- Analyst 분석 상태와 ResearchFile 품질 상태를 반영해 후보를 보수적으로 낮춘다.
- 기존 보유 종목의 현재 비중을 계산한다.
- Risk와 Compliance가 검사할 수 있는 제안 금액을 명확히 넘긴다.

하지 말아야 할 일:

- Risk 한도 위반을 무시하지 않는다.
- Compliance 경고를 우회하지 않는다.
- 실제 주문을 실행하지 않는다.
- 리서치가 부족한 종목을 확정 매수로 표현하지 않는다.
- Analyst가 `missing`이거나 ResearchFile이 `missing/incomplete`이면 신규 매수 초안을 보수화한다.
- 포트폴리오 데이터가 없는데 매수 가능하다고 단정하지 않는다.

## 2. 입력 계약

### 2.1 후보 입력

`PortfolioManagerAgent`는 Quant에서 표준화된 `Candidate` 목록을 입력으로 받는다.

사용 필드:

| 필드 | 용도 |
|---|---|
| `ticker` | 기존 보유 종목 여부 확인 |
| `name` | 보고서 표시 |
| `score` | 후보 우선순위 산정 |
| `signal_count` | 점수가 없을 때 보조 우선순위 |
| `suggested_amount` | 수동 후보별 제안 금액 |
| `source_type` | 향후 후보 출처별 우선순위 정책 참고 |

### 2.2 Analyst 입력

`EquityResearchAnalystAgent`가 구현된 이후에는 Portfolio가 기업 분석 상태를 함께 입력으로 받는다.

사용 필드:

| 필드 | 용도 |
|---|---|
| `analysis_status` | 분석 완성도에 따라 신규 매수 초안 보수화 |
| `confidence` | 후보 우선순위 조정 |
| `key_risks` | 포트폴리오 판단 사유에 반영 |
| `missing_items` | 사람 검토 항목에 반영 |
| `disconfirmation_conditions` | 기존 보유 종목 유지/축소 검토 참고 |

Portfolio는 Analyst의 분석을 투자 승인으로 해석하지 않는다. Analyst 결과가 좋더라도 Risk와 Compliance를 반드시 통과해야 한다.

### 2.3 ResearchFile 입력

`ResearchFileAgent`가 구현된 이후에는 리서치 기록 품질도 함께 입력으로 받는다.

사용 필드:

| 필드 | 용도 |
|---|---|
| `quality_status` | 리서치 파일 품질에 따라 액션 보수화 |
| `is_stale` | 오래된 리서치면 신규 매수 보류 |
| `missing_required_sections` | 후보 우선순위와 사람 확인 항목 반영 |
| `analyst_source_file_mismatch` | 분석 근거 불일치 시 신규 매수 보류 |

### 2.4 포트폴리오 입력

포트폴리오는 `--portfolio-json`으로 입력한다. 현재 구현의 기본 스키마는 다음과 같다.

```json
{
  "cash": 1000000,
  "positions": [
    {
      "ticker": "005930",
      "name": "삼성전자",
      "quantity": 10,
      "avg_price": 70000,
      "current_price": 80000,
      "sector": "반도체"
    }
  ]
}
```

`PortfolioPosition` 필드:

| 필드 | 필수 | 의미 |
|---|---:|---|
| `ticker` | 예 | 6자리 종목코드 |
| `name` | 아니오 | 종목명 |
| `quantity` | 예 | 보유 수량 |
| `avg_price` | 아니오 | 평균 매입 단가 |
| `current_price` | 아니오 | 현재가 |
| `sector` | 아니오 | 섹터 |

시장가치 계산:

```text
market_value = quantity * (current_price if current_price > 0 else avg_price)
portfolio_value = cash + sum(position.market_value)
```

### 2.5 설정 입력

| 설정 | 의미 |
|---|---|
| `default_suggested_amount` | 후보별 금액이 없을 때 사용할 기본 검토 금액 |
| `max_order_amount` | Risk 단계에서 다시 검사할 주문금액 상한 |
| `max_position_pct` | Risk 단계에서 검사할 종목 비중 한도 |
| `max_sector_pct` | Risk 단계에서 검사할 섹터 비중 한도 |

Portfolio는 위 한도를 직접 최종 판정하지 않는다. 단, 제안 금액과 현재 비중을 명확히 기록해 Risk가 검사할 수 있게 한다.

## 3. 처리 프로세스

`PortfolioManagerAgent`는 후보별로 다음 순서로 동작한다.

```text
1. 포트폴리오 스냅샷 존재 여부 확인
2. 현금과 포트폴리오 가치 계산
3. 후보가 기존 보유 종목인지 확인
4. Analyst 분석 상태 확인
5. ResearchFile 품질 상태 확인
6. 후보별 suggested_amount 결정
7. 후보 우선순위 계산
8. 액션 초안 결정
9. 현재 보유 비중 계산
10. 후보별 signal 생성
```

후보별 기본 금액 결정:

```text
amount = candidate.suggested_amount or default_suggested_amount
```

우선순위 계산:

```text
priority = candidate.score if score exists else float(candidate.signal_count)
```

향후 Analyst/ResearchFile 반영 후 우선순위 보정:

```text
base_priority = candidate.score if score exists else signal_count

if analysis_status == "complete" and confidence == "high":
    priority = base_priority
elif analysis_status == "partial" or confidence in {"medium", "low"}:
    priority = base_priority * 0.7
elif analysis_status == "missing" or confidence == "none":
    priority = base_priority * 0.3

if research quality_status in {"missing", "incomplete", "stale", "unreadable"}:
    priority = min(priority, base_priority * 0.5)
```

v1 구현에서는 이 보정이 아직 필수는 아니지만, PLAN상 최종 방향은 분석 품질이 낮은 후보를 더 보수적으로 다루는 것이다.

현재 비중 계산:

```text
current_weight = existing_position.market_value / portfolio_value
```

포트폴리오 가치가 0이거나 스냅샷이 없으면 현재 비중은 0으로 표시하되, 판단 상태는 `needs_review`로 둔다.

## 4. 액션 초안 기준

`PortfolioManagerAgent`는 최종 주문 지시가 아니라 액션 초안을 만든다.

| 상황 | `side` | `action_detail` | 의미 |
|---|---|---|---|
| 기존 보유 종목 | `hold` | `review_existing_position` | 유지, 추가매수, 축소 여부를 검토해야 함 |
| 포트폴리오 가치 없음 | `hold` | `needs_portfolio_snapshot` | 계좌 정보 없이는 매수 판단 불가 |
| 현금 부족 | `hold` | `cash_limited` | 현금 부족으로 신규 매수 초안 불가 |
| Analyst 분석 없음 | `hold` | `needs_equity_research` | 기업 분석 없이는 신규 매수 판단 불가 |
| 리서치 파일 품질 부족 | `hold` | `needs_research_file_quality` | 기록/근거 부족으로 신규 매수 보류 |
| 신규 후보, 현금 충분 | `buy` | `new_buy_candidate` | Risk/Compliance 전제의 매수 검토 초안 |
| 후보 없음 | 없음 | 없음 | `needs_review` |

중요 원칙:

- `side="buy"`는 매수 확정이 아니다.
- `side="buy"`는 Risk와 Compliance가 모두 `approve`일 때만 Trader에서 주문안으로 유지될 수 있다.
- 기존 보유 종목은 자동 추가매수하지 않는다. 기본은 `hold`와 검토 사유 기록이다.
- `analysis_status != "complete"`인 신규 후보는 원칙적으로 `hold` 또는 낮은 우선순위로 둔다.
- `quality_status != "usable"`인 신규 후보는 원칙적으로 `hold` 또는 낮은 우선순위로 둔다.

## 5. 판정 기준

| Status | 조건 |
|---|---|
| `info` | 후보별 액션 초안을 만들었고, 포트폴리오/현금 정보가 판단 가능한 수준이다. |
| `needs_review` | 후보 없음, 포트폴리오 스냅샷 없음, 포트폴리오 가치 0, 현금 부족 중 하나라도 있다. |
| `block` | 사용하지 않는다. 차단은 Risk와 Compliance가 담당한다. |
| `approve` | 사용하지 않는다. Portfolio의 buy는 승인 상태가 아니다. |

경고를 남겨야 하는 경우:

- 후보가 0개다.
- 포트폴리오 JSON이 없거나 읽히지 않았다.
- `portfolio_value <= 0`이다.
- 후보의 제안 금액이 현금보다 크다.
- Analyst 분석이 `missing` 또는 `partial`이다.
- ResearchFile 품질이 `missing`, `incomplete`, `stale`, `unreadable`이다.
- Analyst와 ResearchFile의 source file mismatch가 있다.
- 기존 보유 종목의 현재가와 평균가가 모두 없어 market value가 0이다.
- 수량이 음수이거나 비정상 값이다.

## 6. 출력 계약

Portfolio 결과는 `portfolio_manager.json`으로 저장된다.

공통 구조:

```json
{
  "agent": "PortfolioManagerAgent",
  "status": "needs_review",
  "summary": "N portfolio action(s) drafted.",
  "signals": [],
  "warnings": [],
  "required_human_checks": [],
  "artifacts": {
    "portfolio_value": 0,
    "cash": 0
  }
}
```

각 `signals[]` 항목:

| 필드 | 의미 |
|---|---|
| `ticker` | 후보 종목코드 |
| `name` | 후보 종목명 |
| `side` | `buy` 또는 `hold` |
| `action_detail` | 액션 초안의 구체 사유 |
| `suggested_amount` | Risk와 Trader가 참고할 제안 금액 |
| `priority` | 후보 우선순위 |
| `analysis_status` | Analyst 분석 상태 |
| `research_quality_status` | 리서치 파일 품질 상태 |
| `portfolio_constraints` | 포트폴리오 관점 제약 목록 |
| `current_weight` | 기존 보유 비중 |
| `reason` | 사람이 읽을 판단 사유 |

`artifacts` 필드:

| 필드 | 의미 |
|---|---|
| `portfolio_value` | 현금 포함 전체 포트폴리오 가치 |
| `cash` | 사용 가능 현금 |

## 7. 다른 Agent와의 계약

Portfolio 출력은 다음 Agent에 영향을 준다.

| 다음 Agent | 사용하는 정보 | 목적 |
|---|---|---|
| `RiskManagerAgent` | `suggested_amount`, 포트폴리오 가치, 현금, 보유 종목 | 종목/섹터/현금/일간 한도 검사 |
| `TraderAgent` | `side`, `reason`, `suggested_amount` | 주문안 방향과 사유 결정 |
| `OperationsReportAgent` | warnings, artifacts | 최종 보고서의 계좌 상태 표시 |
| `EquityResearchAnalystAgent` | 선행 Agent. `analysis_status`, `confidence` 사용 | 기업 분석 품질 반영 |
| `ResearchFileAgent` | 선행 Agent. `quality_status`, `is_stale` 사용 | 리서치 기록 품질 반영 |

Portfolio가 보장해야 하는 것:

- 후보별 `side`는 항상 보수적으로 결정한다.
- 포트폴리오 데이터가 없으면 `buy`를 만들지 않는다.
- 분석 또는 리서치 품질이 부족하면 신규 후보를 보수적으로 낮춘다.
- 제안 금액은 숫자로 기록한다.
- 기존 보유 종목은 현재 비중을 계산해 남긴다.

Portfolio가 보장하지 않는 것:

- 해당 종목이 좋은 투자라는 보장
- 리스크 한도 통과 보장
- 준법 요건 통과 보장
- 기업 분석이 충분하다는 최종 보장
- 리서치 기록이 충분하다는 최종 보장
- 실제 주문 가능 수량 보장
- 현재가 최신성 보장

## 8. 사람 확인 항목

Portfolio 단계에서 사람이 확인해야 할 항목:

- 포트폴리오 JSON이 최신 계좌 상태인지 확인한다.
- 현금이 실제 주문 가능 현금인지 확인한다.
- 기존 보유 종목의 현재가가 최신인지 확인한다.
- 후보가 기존 보유 종목이면 추가매수, 유지, 축소 중 무엇이 맞는지 확인한다.
- Analyst가 지적한 `key_risks`가 포트폴리오 전체 리스크와 겹치는지 확인한다.
- ResearchFile이 `stale` 또는 `incomplete`이면 신규 매수 검토를 미뤄야 하는지 확인한다.
- 신규 후보가 여러 개면 우선순위가 실제 투자 의도와 맞는지 확인한다.
- 포트폴리오 전체 종목 수와 집중도가 개인 투자 원칙에 맞는지 확인한다.

## 9. 실패/예외 처리

| 상황 | 처리 |
|---|---|
| 포트폴리오 JSON 없음 | `portfolio_value=0`, `cash=0`, `needs_review` |
| 포트폴리오 JSON 읽기 실패 | 실행 단계에서 오류 처리 필요. v1 문서상 `needs_review`가 바람직 |
| `positions` 없음 | 현금만 있는 포트폴리오로 처리 |
| `cash` 없음 | 0으로 처리하고 `needs_review` |
| 후보 없음 | `needs_review` |
| 기존 보유 종목 current_price 없음 | avg_price로 market value 계산 |
| current_price와 avg_price 모두 없음 | market value 0, 경고 |
| 후보 suggested_amount 없음 | `default_suggested_amount` 사용 |
| suggested_amount > cash | `hold`, `cash_limited`, `needs_review` |
| Analyst 결과 없음 | v1 구현 전에는 경고만, 구현 후에는 `needs_equity_research` |
| Analyst 분석 missing | 신규 후보는 `hold`, 기존 보유는 `review_existing_position` |
| Research quality missing/incomplete | 신규 후보는 `hold`, 기존 보유는 `review_existing_position` |
| Analyst/Research source mismatch | `needs_review` |

실패 처리 원칙:

- 포트폴리오 정보가 없으면 매수 초안을 만들지 않는다.
- 계좌 상태가 불완전하면 Risk에서 다시 보수적으로 막을 수 있게 경고를 남긴다.
- 보유 종목 데이터가 이상해도 원본 포트폴리오 파일은 수정하지 않는다.

## 10. 구현 작업 목록

현재 구현 기준에서 Portfolio 관련 보강 작업은 다음 순서로 진행한다.

1. 포트폴리오 JSON 예시를 문서 또는 샘플 파일로 제공한다.
2. `load_portfolio()`가 누락 필드에 대해 더 명확한 경고를 만들 수 있게 개선한다.
3. 후보 없음 또는 포트폴리오 없음 경고를 후보별/전체 단위로 구분한다.
4. 기존 보유 종목의 `add_review`, `trim_review` 판단 기준을 v2에서 확장한다.
5. `sector`와 보유 종목 수를 artifacts에 추가해 Operations가 요약할 수 있게 한다.
6. Research 품질 점수가 생기면 priority 계산에 반영한다.
7. 최종 보고서에 포트폴리오 가치, 현금, 후보별 현재 비중을 표로 보여준다.
8. Analyst 결과의 `analysis_status`, `confidence`, `key_risks`를 Portfolio 입력으로 받는다.
9. ResearchFile 결과의 `quality_status`, `is_stale`, `analyst_source_file_mismatch`를 Portfolio 입력으로 받는다.
10. 신규 후보의 `buy` 초안 조건을 분석/리서치 품질까지 반영해 강화한다.

v1에서 하지 않는 구현:

- 자동 리밸런싱을 하지 않는다.
- 목표 비중 최적화를 하지 않는다.
- 세금, 수수료, 슬리피지를 반영하지 않는다.
- 실제 주문 가능 수량을 확정하지 않는다.
- 현금 부족 시 자동 매도 후보를 만들지 않는다.
- Analyst 분석이 없는데 낙관적 매수 초안을 만들지 않는다.

## 11. 테스트 시나리오

### 11.1 포트폴리오 없음

명령:

```bash
rtk python scripts/run_free_agent_pipeline.py --candidate 005930:삼성전자:100000 --no-require-research-file
```

기대 결과:

- `portfolio_value == 0`
- `cash == 0`
- 후보 `side == "hold"`
- `action_detail == "needs_portfolio_snapshot"`
- status는 `needs_review`

### 11.2 신규 후보, 현금 충분

포트폴리오 JSON:

```json
{
  "cash": 1000000,
  "positions": []
}
```

후보:

```text
005930:삼성전자:100000
```

기대 결과:

- 후보 `side == "buy"`
- `action_detail == "new_buy_candidate"`
- `suggested_amount == 100000`
- status는 `info`

### 11.3 신규 후보, 현금 부족

포트폴리오 JSON:

```json
{
  "cash": 50000,
  "positions": []
}
```

후보:

```text
005930:삼성전자:100000
```

기대 결과:

- 후보 `side == "hold"`
- `action_detail == "cash_limited"`
- warnings에 현금 부족 포함
- status는 `needs_review`

### 11.4 기존 보유 종목

포트폴리오 JSON:

```json
{
  "cash": 1000000,
  "positions": [
    {
      "ticker": "005930",
      "name": "삼성전자",
      "quantity": 10,
      "avg_price": 70000,
      "current_price": 80000,
      "sector": "반도체"
    }
  ]
}
```

기대 결과:

- 후보 `side == "hold"`
- `action_detail == "review_existing_position"`
- `current_weight`가 0보다 크다.
- reason에 기존 보유 종목 검토 문구가 포함된다.

### 11.5 후보 우선순위

조건:

- 후보 A는 `score=80`
- 후보 B는 `score=null`, `signal_count=5`

기대 결과:

- 후보 A priority는 80
- 후보 B priority는 5
- priority는 최종 매수 승인이 아니라 정렬/검토 참고값이다.

### 11.6 Analyst 분석 부족

조건:

- 신규 후보
- 현금 충분
- `analysis_status == "missing"`

기대 결과:

- 후보 `side == "hold"`
- `action_detail == "needs_equity_research"`
- warnings 또는 reason에 기업 분석 부족 포함
- status는 `needs_review`

### 11.7 ResearchFile 품질 부족

조건:

- 신규 후보
- 현금 충분
- `quality_status == "incomplete"` 또는 `is_stale == true`

기대 결과:

- 후보 `side == "hold"`
- `action_detail == "needs_research_file_quality"`
- reason에 리서치 기록 보완 필요 포함

### 11.8 기존 보유 + Analyst 리스크 발견

조건:

- 기존 보유 종목
- Analyst `key_risks` 존재

기대 결과:

- 후보 `side == "hold"`
- `action_detail == "review_existing_position"`
- reason에 리스크 재검토 필요 포함

## 12. 성공 기준

`PortfolioManagerAgent`는 다음 조건을 만족해야 성공이다.

- 신규 후보와 기존 보유 종목이 구분된다.
- 포트폴리오 스냅샷이 없으면 매수 초안을 만들지 않는다.
- 현금 부족이면 후보를 `hold`로 낮춘다.
- 후보별 `suggested_amount`가 기록된다.
- 기존 보유 종목의 현재 비중이 계산된다.
- 후보 우선순위가 기록된다.
- Analyst와 ResearchFile 품질이 우선순위와 액션 초안에 반영된다.
- Risk와 Trader가 사용할 수 있는 `side`, `reason`, `suggested_amount`가 명확하다.
- 실제 주문은 실행하지 않는다.

## 13. 향후 고도화

v2 이후 개선 후보:

- 목표 비중 정책 도입
- 보유 종목별 `add_review`, `hold`, `trim_review` 세분화
- 현금 비중 하한/상한 정책
- 섹터별 목표 비중 정책
- 후보별 thesis quality와 priority 결합
- Analyst confidence 기반 position sizing 초안
- 리서치 품질 기반 후보 승격/강등 정책
- 포트폴리오 전체 종목 수 제한
- 주문 후 예상 포트폴리오 상태 시뮬레이션
- 수수료/세금/슬리피지 반영

v2에서도 유지할 원칙:

- Portfolio는 투자 실행자가 아니다.
- Portfolio의 `buy`는 Risk/Compliance 전 단계의 초안이다.
- 포트폴리오 데이터가 없으면 보수적으로 `needs_review`를 남긴다.
- 기업 분석과 리서치 파일 품질이 부족하면 보수적으로 `hold`를 남긴다.
