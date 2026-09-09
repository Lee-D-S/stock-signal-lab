# RiskManagerAgent PLAN

작성일: 2026-05-08

## 1. 전문가 역할

`RiskManagerAgent`는 투자팀의 **최고 리스크 책임자**다.

역할은 좋은 투자 아이디어를 찾는 것이 아니라, 계좌가 훼손될 수 있는 행동을 코드 규칙으로 먼저 막는 것이다. 이 Agent는 수익 기회를 평가하지 않는다. Quant 신호가 강하고 Analyst 분석이 좋아도, 계좌 비중, 현금, 유동성, 일간 신규 매수 한도, 섹터 집중도가 위험하면 진행을 막는다.

핵심 책임:

- 주문금액 상한을 검사한다.
- 현금 부족을 검사한다.
- 일간 신규 매수 금액을 검사한다.
- 종목 최대 비중을 검사한다.
- 섹터 최대 비중을 검사한다.
- 거래대금/유동성 부족을 검사한다.
- 포트폴리오 데이터 누락을 `needs_review`로 남긴다.
- 리스크 위반 사유를 후보별로 기록한다.

하지 말아야 할 일:

- 데이터가 없을 때 통과시키지 않는다.
- 좋은 기업이라는 이유로 리스크 한도를 완화하지 않는다.
- Compliance 판단을 대신하지 않는다.
- Analyst 분석을 투자 승인으로 해석하지 않는다.
- 실제 주문 수량을 확정하지 않는다.
- 손실 가능성을 숨기거나 경고를 축약하지 않는다.

## 2. 입력 계약

### 2.1 후보 입력

Risk는 `Candidate`와 Portfolio 결과를 함께 사용한다.

후보에서 사용하는 필드:

| 필드 | 용도 |
|---|---|
| `ticker` | 기존 보유 종목과 매칭 |
| `name` | 경고와 보고서 표시 |
| `suggested_amount` | 후보별 주문 검토 금액 |
| `trade_amount` | 유동성 검사 |
| `sector` | 섹터 집중도 검사 |
| `current_price` | 향후 수량/가격 리스크 참고 |

Portfolio에서 사용하는 정보:

| 정보 | 용도 |
|---|---|
| `portfolio` | 기존 보유 종목 가치 계산 |
| `portfolio_value` | 종목/섹터 비중 계산 |
| `cash` | 현금 부족 검사 |
| `suggested_amount` | 리스크 노출 금액 |

### 2.2 설정 입력

현재 구현 또는 CLI에서 사용하는 주요 리스크 설정:

| 설정 | 기본값/출처 | 의미 |
|---|---|---|
| `max_order_amount` | `config.py settings.max_order_amount`, 기본 100,000원 | 단일 주문 검토 금액 상한 |
| `max_daily_new_buy_amount` | `settings.max_daily_spend`, 기본 1,000,000원 | 일간 신규 매수 총액 상한 |
| `max_position_pct` | CLI 기본 0.25 | 주문 후 단일 종목 최대 비중 |
| `max_sector_pct` | CLI 기본 0.40 | 주문 후 단일 섹터 최대 비중 |
| `liquidity_ratio_limit` | v1 문서 기준 1% | 주문금액이 관측 거래대금의 1%를 넘으면 검토 필요 |

v1에서는 설정이 CLI와 `config.py`에 분산되어 있다. 향후에는 risk policy 파일로 분리한다.

### 2.3 Analyst/Research 입력

Risk는 기업 분석을 직접 평가하지 않는다. 다만 다음 정보는 리스크 경고에 반영할 수 있다.

| 입력 | 사용 방식 |
|---|---|
| Analyst `key_risks` | 후보별 리스크 사유에 표시 |
| Analyst `disconfirmation_conditions` | 향후 손절/관찰 조건 참고 |
| ResearchFile `quality_status` | 리서치 품질 부족 시 리스크 검토 강화 |

중요: Analyst/Research 품질 부족은 Risk의 한도 계산을 대체하지 않는다. 계좌 리스크는 별도로 계산한다.

## 3. 처리 프로세스

Risk는 후보별로 다음 순서로 동작한다.

```text
1. 후보별 suggested_amount 결정
2. 일간 신규 매수 총액 계산
3. suggested_amount 양수 여부 검사
4. 단일 주문금액 상한 검사
5. 현금 부족 검사
6. 포트폴리오 가치 존재 여부 검사
7. 주문 후 종목 비중 계산
8. 주문 후 섹터 비중 계산
9. 거래대금/유동성 검사
10. 후보별 status와 warnings 생성
11. 전체 Risk status 계산
```

일간 신규 매수 총액:

```text
daily_new_buy_amount = sum(candidate suggested_amount for candidates not currently held)
```

종목 비중:

```text
existing_value = sum(position.market_value for same ticker)
projected_position_pct = (existing_value + suggested_amount) / portfolio_value
```

섹터 비중:

```text
sector = candidate.sector or existing_position.sector
sector_value = sum(position.market_value for same sector)
projected_sector_pct = (sector_value + suggested_amount) / portfolio_value
```

유동성 검사:

```text
if trade_amount is missing:
    needs_review
elif suggested_amount > trade_amount * 0.01:
    needs_review
```

## 4. 판정 기준

Risk는 `approve`, `block`, `needs_review`를 모두 사용한다.

| Status | 조건 |
|---|---|
| `approve` | 필수 리스크 데이터가 있고, 모든 한도를 통과했다. |
| `block` | 명확한 리스크 한도 위반이다. Trader는 반드시 `hold`로 낮춘다. |
| `needs_review` | 데이터 누락 또는 판단 불가다. Trader는 반드시 `hold`로 낮춘다. |
| `info` | 후보 없음 등 단순 정보 제공 상황에서만 가능하다. |

`block` 조건:

- `suggested_amount <= 0`
- `suggested_amount > max_order_amount`
- `daily_new_buy_amount > max_daily_new_buy_amount`
- `projected_position_pct > max_position_pct`
- `projected_sector_pct > max_sector_pct`

`needs_review` 조건:

- 현금 부족
- 포트폴리오 스냅샷 없음
- `portfolio_value <= 0`
- 섹터 정보 없음
- 거래대금/유동성 데이터 없음
- 주문금액이 거래대금의 1% 초과
- 현재가 또는 보유 종목 가격 데이터가 불완전함
- Analyst가 제시한 핵심 리스크가 있는데 아직 반영 정책이 없음

중요 원칙:

```text
Risk status가 approve가 아니면 TraderAgent는 주문안을 hold로 낮춘다.
```

## 5. 출력 계약

Risk 결과는 `risk_manager.json`으로 저장된다.

공통 구조:

```json
{
  "agent": "RiskManagerAgent",
  "status": "needs_review",
  "summary": "Risk guardrail checks completed.",
  "signals": [],
  "warnings": [],
  "required_human_checks": [],
  "artifacts": {
    "max_position_pct": 0.25,
    "max_sector_pct": 0.4,
    "max_order_amount": 100000,
    "max_daily_new_buy_amount": 1000000,
    "daily_new_buy_amount": 0
  }
}
```

각 `signals[]` 항목:

| 필드 | 의미 |
|---|---|
| `ticker` | 후보 종목코드 |
| `status` | 후보별 리스크 상태 |
| `warnings` | 후보별 리스크 경고 |

향후 확장 필드:

| 필드 | 의미 |
|---|---|
| `suggested_amount` | 검사 대상 금액 |
| `projected_position_pct` | 주문 후 예상 종목 비중 |
| `projected_sector_pct` | 주문 후 예상 섹터 비중 |
| `liquidity_ratio` | 거래대금 대비 주문금액 |
| `risk_constraints` | 적용된 한도와 위반 여부 |

## 6. 다른 Agent와의 계약

| 다음 Agent | 사용하는 정보 | 목적 |
|---|---|---|
| `TraderAgent` | 후보별 `status` | `approve`가 아니면 `hold`로 낮춤 |
| `OperationsReportAgent` | warnings, artifacts | 최종 보고서 리스크 섹션 |
| `ComplianceOfficerAgent` | 직접 의존 없음 | 독립 Gate 유지 |

Risk가 보장해야 하는 것:

- 한도 위반은 `block`으로 남긴다.
- 판단 불가능은 `needs_review`로 남긴다.
- 데이터 누락을 승인으로 해석하지 않는다.
- 후보별 차단 사유를 사람이 읽을 수 있게 남긴다.

Risk가 보장하지 않는 것:

- 투자 수익 가능성
- 준법 요건 충족 여부
- 리서치 품질
- 실제 주문 체결 가능성
- 세금/수수료/슬리피지 반영

## 7. 사람 확인 항목

Risk 단계에서 사람이 확인해야 할 항목:

- 포트폴리오 JSON이 최신인지 확인한다.
- 현금이 실제 주문 가능 현금인지 확인한다.
- 현재가와 보유 수량이 최신인지 확인한다.
- 후보 섹터가 정확한지 확인한다.
- 거래대금이 당일 또는 최근 평균 기준인지 확인한다.
- 이벤트 리스크가 있는지 확인한다.
- Analyst가 제시한 핵심 리스크가 포트폴리오 전체 리스크와 중복되는지 확인한다.
- 일간 신규 매수 한도가 개인 투자 원칙에 맞는지 확인한다.

## 8. 실패/예외 처리

| 상황 | 처리 |
|---|---|
| 후보 없음 | `info` 또는 `needs_review`, 전체 파이프라인은 계속 실행 |
| suggested_amount 없음 | `default_suggested_amount` 사용 |
| suggested_amount <= 0 | `block` |
| suggested_amount > max_order_amount | `block` |
| 현금 부족 | `needs_review` |
| portfolio_value <= 0 | `needs_review` |
| 동일 종목 기존 보유 가치 계산 불가 | `needs_review` |
| sector 없음 | `needs_review` |
| trade_amount 없음 | `needs_review` |
| 주문금액 > 거래대금 1% | `needs_review` |
| 일간 신규 매수 총액 초과 | `block` |

실패 처리 원칙:

- 리스크 데이터가 없으면 승인하지 않는다.
- 명확한 한도 위반은 사람 검토가 아니라 차단이다.
- 판단 불가능은 차단보다 낮은 단계가 아니라 `needs_review`이며, Trader에서는 동일하게 `hold`로 내려간다.
- 원본 포트폴리오 파일은 수정하지 않는다.

## 9. 구현 작업 목록

현재 구현 기준에서 Risk 관련 보강 작업은 다음 순서로 진행한다.

1. 후보별 `projected_position_pct`, `projected_sector_pct`, `liquidity_ratio`를 signals에 추가한다.
2. 섹터가 후보에 없을 때 별도 섹터 매핑 파일 또는 포트폴리오 보유 섹터를 활용한다.
3. 거래대금 기준을 당일, 최근 5일 평균, 최근 20일 평균 중 어떤 값인지 명시한다.
4. Analyst `key_risks`를 risk warnings에 참고 정보로 추가한다.
5. 포트폴리오 JSON 누락/오류를 `load_portfolio()` 단계에서 구조화된 경고로 넘긴다.
6. Risk policy를 CLI 옵션에서 별도 설정 파일로 분리한다.
7. Operations 보고서에 리스크 한도와 후보별 위반 항목을 표로 표시한다.

v1에서 하지 않는 구현:

- VaR, 베타, 상관관계 기반 포트폴리오 최적화를 하지 않는다.
- 실시간 가격 API를 호출하지 않는다.
- 자동 손절 주문을 만들지 않는다.
- 현금 부족 시 자동 매도 후보를 만들지 않는다.
- 리스크 한도를 자동 완화하지 않는다.

## 10. 테스트 시나리오

### 10.1 정상 통과

조건:

- 포트폴리오 가치 있음
- 현금 충분
- suggested_amount <= max_order_amount
- 종목/섹터 비중 한도 이내
- 거래대금 있음

기대 결과:

- 후보 status는 `approve`
- warnings 비어 있음 또는 경미한 정보만 있음

### 10.2 주문금액 상한 초과

조건:

- `suggested_amount > max_order_amount`

기대 결과:

- 후보 status는 `block`
- warnings에 max_order_amount 초과 포함
- Trader에서 `hold`

### 10.3 포트폴리오 스냅샷 없음

조건:

- 포트폴리오 JSON 없음
- `portfolio_value == 0`

기대 결과:

- 후보 status는 `needs_review`
- warnings에 포트폴리오 스냅샷 없음 포함
- Trader에서 `hold`

### 10.4 종목 비중 초과

조건:

- 기존 보유 가치 + suggested_amount가 `max_position_pct` 초과

기대 결과:

- 후보 status는 `block`
- projected position weight 초과 경고

### 10.5 섹터 비중 초과

조건:

- 같은 섹터 기존 보유 가치 + suggested_amount가 `max_sector_pct` 초과

기대 결과:

- 후보 status는 `block`
- projected sector weight 초과 경고

### 10.6 현금 부족

조건:

- `suggested_amount > cash`

기대 결과:

- 후보 status는 `needs_review`
- warnings에 현금 부족 포함
- Trader에서 `hold`

### 10.7 유동성 누락

조건:

- 후보 `trade_amount` 없음

기대 결과:

- 후보 status는 `needs_review`
- warnings에 liquidity missing 포함

### 10.8 일간 신규 매수 한도 초과

조건:

- 신규 후보들의 suggested_amount 합이 `max_daily_new_buy_amount` 초과

기대 결과:

- 관련 후보 status는 `block`
- artifacts에 `daily_new_buy_amount` 기록

## 11. 성공 기준

`RiskManagerAgent`는 다음 조건을 만족해야 성공이다.

- 명확한 한도 위반은 `block`으로 남긴다.
- 데이터 누락은 `needs_review`로 남긴다.
- Risk가 `approve`가 아니면 Trader 주문안은 `hold`가 된다.
- 후보별 리스크 사유가 JSON과 최종 보고서에 남는다.
- 포트폴리오 스냅샷이 없을 때 통과하지 않는다.
- 현금, 종목 비중, 섹터 비중, 일간 한도, 유동성 검사가 모두 동작한다.
- 실제 주문은 실행하지 않는다.

## 12. 향후 고도화

v2 이후 개선 후보:

- 별도 `risk_policy.json` 도입
- 종목별 변동성 기반 주문금액 조정
- 계좌 전체 베타와 시장 노출 계산
- 업종/테마 중복 노출 검사
- 이벤트 리스크 캘린더 연동
- 손절/익절 정책과 연결
- 실제 체결 후 리스크 사후 점검

v2에서도 유지할 원칙:

- 리스크 한도는 좋은 종목에도 적용된다.
- 데이터가 없으면 승인하지 않는다.
- Risk는 Compliance와 독립된 Gate다.
