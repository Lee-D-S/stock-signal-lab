# QuantSignalAgent PLAN

작성일: 2026-05-08

## 1. 전문가 역할

`QuantSignalAgent`는 투자팀의 **퀀트 리서치 책임자**다.

역할은 “무엇을 살지 결정하는 것”이 아니라, 기존 신호 생성 파이프라인에서 나온 여러 CSV와 수동 후보를 하나의 표준 후보 목록으로 바꾸는 것이다. 이 Agent가 잘해야 하는 일은 신호를 과장하지 않고, 출처와 신뢰도를 보존한 상태로 다음 Agent들이 검토할 수 있게 만드는 것이다.

핵심 책임:

- 기존 `07_전략신호` 산출물을 읽는다.
- 종목코드, 종목명, 신호 출처, 조건명, 점수, 현재가, 거래대금을 표준화한다.
- 같은 종목이 여러 신호에 잡히면 하나의 후보로 병합한다.
- 각 후보가 어떤 신호에서 왔는지 `signal_details`에 남긴다.
- 데이터가 오래됐거나 불완전하면 매수 후보에서 제거하지 않고 경고로 남긴다.

하지 말아야 할 일:

- 매수, 매도, 보유 결정을 내리지 않는다.
- 리서치 근거가 있다고 가정하지 않는다.
- Risk 또는 Compliance를 통과했다고 가정하지 않는다.
- 특정 조건이 좋다는 이유로 후보를 자동 승인하지 않는다.
- CSV 일부가 깨졌다고 전체 파이프라인을 중단하지 않는다.

## 2. 입력 계약

### 2.1 실행 입력

`QuantSignalAgent`는 `AgentContext.candidates`를 입력으로 받는다. 후보는 두 경로로 만들어진다.

| 입력 경로 | 생성 방식 | 의미 |
|---|---|---|
| 수동 후보 | `--candidate TICKER[:NAME[:AMOUNT]]` | 사용자가 직접 검토를 요청한 종목 |
| 자동 발견 후보 | `--discover --discover-limit N` | `07_전략신호/**/*.csv`에서 발견한 종목 |

수동 후보는 `source_type="manual"`로 기록한다.

자동 발견 후보는 `discover_candidates_from_csv()`가 다음 폴더를 탐색해 만든다.

```text
ai 주가 변동 원인 분석/07_전략신호/
```

### 2.2 주요 CSV 입력

현재 v1에서 우선 해석할 CSV 유형은 다음과 같다.

| 폴더 | 예시 파일 | `source_type` | 주요 컬럼 |
|---|---|---|---|
| `01_관심종목` | `관심종목_시그널_후보.csv` | `watchlist` | `ticker`, `name`, `signal_date`, `status`, `is_candidate` |
| `02_신규조건` | `신규조건_관심종목_시그널_후보.csv` | `new_condition` | `ticker`, `name`, `signal_date`, 조건 관련 컬럼 |
| `03_외국인수급` | `외국인순매수_연속_후보.csv` | `foreign_flow` | `ticker`, `name`, `condition_id`, `matched_conditions`, `event_close` |
| `04_캔들` | `캔들_패턴_스캔.csv` | `candlestick` | `ticker`, `name`, `pattern_id`, `confidence`, `close`, `trade_amount` |
| 기타 scoring 결과 | 향후 scoring CSV | `scoring` | `ticker`, `name`, `score` |
| 기타 CSV | 분류 불가 CSV | `csv_signal` | 가능한 범위의 공통 컬럼 |

CSV 컬럼명이 조금 달라도 아래 후보 필드만 추출할 수 있으면 파이프라인은 계속 진행한다.

필수 추출 우선순위:

| 표준 필드 | 우선 컬럼 |
|---|---|
| `ticker` | `ticker`, `종목코드`, `code`, 6자리 숫자 값 |
| `name` | `name`, `종목명`, `company_name`, ticker가 아닌 첫 번째 문자열 |
| `score` | `score`, `confidence`, `점수`, `신뢰도`, `change_rate`, `chg_pct` |
| `current_price` | `event_close`, `close`, `price` |
| `trade_amount` | `trade_amount`, `avg_trade_amount` |
| `condition` | `condition_id`, `pattern_id`, `matched_conditions` |
| `signal_date` | `signal_date`, `basis_date` |

### 2.3 설정 입력

| 설정 | 기본 의미 |
|---|---|
| `stale_signal_days` | 신호 파일이 며칠 이상 오래되면 경고할지 결정한다. 기본값은 3일이다. |
| `discover_limit` | 자동 발견 후보의 최대 개수다. |
| `default_suggested_amount` | 후보별 금액이 없을 때 Portfolio 단계에서 사용할 기본 주문 검토 금액이다. Quant는 이 값을 투자 판단에 사용하지 않는다. |

## 3. 처리 프로세스

`QuantSignalAgent`는 다음 순서로 동작해야 한다.

```text
1. 입력 후보 존재 여부 확인
2. 각 후보의 기본 필드 표준화
3. 신호 출처와 source_type 확인
4. CSV 기반 후보라면 파일 최신성 확인
5. signal_count와 signal_details 확인
6. AgentResult 생성
```

자동 발견 후보 생성은 `discover_candidates_from_csv()`에서 다음 순서로 처리한다.

```text
1. discovery root 존재 여부 확인
2. 최근 수정된 CSV부터 최대 50개 탐색
3. 빈 CSV는 건너뜀
4. 각 CSV에서 최대 500행 읽기
5. ticker가 없는 행은 건너뜀
6. ticker가 같으면 기존 후보에 signal_detail 추가
7. discover_limit에 도달하면 중단
8. score와 signal_count 기준으로 정렬
```

중복 병합 원칙:

- 같은 `ticker`는 하나의 `Candidate`로 병합한다.
- 최초 발견된 파일을 대표 `source`로 둔다.
- 모든 발견 근거는 `signal_details`에 추가한다.
- 점수가 없는 후보보다 점수가 있는 후보를 우선한다.
- `signal_count`는 병합된 신호 수다.

## 4. 판정 기준

`QuantSignalAgent`는 투자 승인 Agent가 아니므로 일반적으로 `approve`를 내지 않는다.

| Status | 조건 |
|---|---|
| `info` | 후보가 1개 이상 있고, 표준화 결과를 다음 Agent에 넘길 수 있다. |
| `needs_review` | 후보가 없거나, 모든 후보가 데이터 부족으로 사람 확인이 필요하다. |
| `block` | 사용하지 않는다. Quant 단계에서 후보를 차단하지 않는다. |
| `approve` | 사용하지 않는다. Quant 신호는 투자 승인으로 해석하지 않는다. |

경고를 남겨야 하는 경우:

- 후보가 0개다.
- 신호 파일이 `stale_signal_days`보다 오래됐다.
- 후보의 `signal_count`가 0 이하로 들어왔다.
- CSV에서 ticker는 찾았지만 name을 찾지 못했다.
- 가격 또는 거래대금이 없다.
- CSV 파일을 읽을 수 없거나 인코딩 문제가 있다.

가격 또는 거래대금 누락은 Quant에서 후보를 제거할 사유가 아니다. 이 정보는 Risk와 Trader에서 다시 `needs_review`로 처리한다.

## 5. 출력 계약

Quant 결과는 `quant_signal.json`으로 저장된다.

공통 구조:

```json
{
  "agent": "QuantSignalAgent",
  "status": "info",
  "summary": "N candidate(s) prepared for rule-based review.",
  "signals": [],
  "warnings": [],
  "required_human_checks": [],
  "artifacts": {
    "candidate_count": 0
  }
}
```

각 `signals[]` 항목은 `Candidate`를 직렬화한 형태다.

필수 필드:

| 필드 | 의미 |
|---|---|
| `ticker` | 6자리 종목코드 |
| `name` | 종목명. 없으면 빈 문자열 가능 |
| `source` | 수동 입력 또는 대표 CSV 파일 경로 |
| `source_type` | 신호 유형 |
| `score` | 점수. 없으면 `null` |
| `suggested_amount` | 수동 후보 금액. 없으면 `null` |
| `signal_count` | 병합된 신호 수 |
| `signal_details` | 신호별 세부 출처 |
| `current_price` | 현재가 또는 이벤트 종가 |
| `trade_amount` | 거래대금 |
| `sector` | 섹터. v1에서는 비어 있을 수 있음 |

`signal_details[]` 필드:

| 필드 | 의미 |
|---|---|
| `source_file` | 해당 신호가 나온 CSV 파일 |
| `source_type` | 신호 유형 |
| `signal_date` | 신호 날짜 |
| `condition` | 조건명, 패턴명, 매칭 조건 |
| `score` | 해당 신호의 점수 |

## 6. 다른 Agent와의 계약

Quant 출력은 다음 Agent들이 사용한다.

| 다음 Agent | 사용하는 필드 | 목적 |
|---|---|---|
| `ResearchFileAgent` | `ticker`, `name` | 리서치 파일 매칭 |
| `PortfolioManagerAgent` | `ticker`, `name`, `score`, `signal_count`, `suggested_amount` | 액션 초안과 우선순위 산정 |
| `RiskManagerAgent` | `ticker`, `trade_amount`, `sector`, `suggested_amount` | 유동성, 섹터, 금액 리스크 검사 |
| `ComplianceOfficerAgent` | `ticker`, `source`, `source_type` | 기록 요건과 수동 후보 출처 확인 |
| `TraderAgent` | `ticker`, `name`, `current_price`, `suggested_amount` | 주문안 수량 계산 |

Quant가 보장해야 하는 것:

- `ticker`는 문자열로 유지한다.
- `signal_details`는 비어 있을 수 있지만 필드는 항상 존재한다.
- 찾지 못한 값은 임의로 추정하지 않고 `null` 또는 빈 문자열로 둔다.
- CSV 파일 경로는 추적 가능하게 남긴다.

Quant가 보장하지 않는 것:

- 신호가 수익성이 있다는 보장
- 리서치가 충분하다는 보장
- 거래가 가능하다는 보장
- 가격이 최신이라는 보장
- 포트폴리오에 적합하다는 보장

## 7. 사람 확인 항목

Quant 단계에서 사람이 확인해야 할 항목:

- 신호가 최근 실행 결과인지 확인한다.
- 후보가 특정 CSV 한 종류에만 과도하게 치우쳤는지 확인한다.
- 캔들 신호의 방향이 실제 투자 의도와 맞는지 확인한다.
- 외국인 수급 신호가 단기 이벤트성인지 확인한다.
- 거래대금이 없는 후보는 Risk 단계에서 반드시 재확인한다.
- 수동 후보는 왜 넣었는지 별도 근거를 남긴다.

## 8. 실패/예외 처리

| 상황 | 처리 |
|---|---|
| discovery root 없음 | 후보 없음으로 처리하고 `needs_review` |
| CSV 파일 0개 | 후보 없음으로 처리하고 `needs_review` |
| 빈 CSV | 건너뛰고 경고 후보로 남길 수 있음 |
| CSV 인코딩 오류 | `utf-8-sig`, `cp949`, `utf-8` 순서로 재시도 |
| ticker 없음 | 해당 행 건너뜀 |
| name 없음 | 빈 문자열로 유지 |
| score 없음 | `null`로 유지 |
| current_price 없음 | `null`로 유지 |
| trade_amount 없음 | `null`로 유지 |
| 중복 ticker | 병합 |
| 너무 많은 후보 | `discover_limit`에서 중단 |

실패 처리 원칙:

- Quant는 최대한 후보 표준화를 계속한다.
- 판단에 필요한 데이터가 부족하면 다음 Gate에서 보수적으로 막도록 경고를 남긴다.
- 후보를 조용히 삭제하지 않는다. 단, ticker가 없는 행은 추적 가능한 종목이 아니므로 제외한다.

## 9. 구현 작업 목록

현재 구현 기준에서 Quant 관련 보강 작업은 다음 순서로 진행한다.

1. `discover_candidates_from_csv()`가 CSV별 source_type을 안정적으로 분류하도록 유지한다.
2. `extract_ticker()`, `extract_name()`, `extract_score()`, `extract_float()`를 CSV 컬럼 변형에 강하게 유지한다.
3. `signal_details`에 조건명, 신호 날짜, 점수, 파일 경로를 항상 남긴다.
4. `QuantSignalAgent.run()`에서 source 파일 age를 계산해 stale 경고를 남긴다.
5. name, current_price, trade_amount 누락 경고를 추가한다.
6. 후보 정렬 기준을 명확히 유지한다: `score` 우선, 다음 `signal_count`.
7. 최종 보고서에서 Quant의 대표 신호 출처를 사람이 볼 수 있게 Operations와 연결한다.

v1에서 하지 않는 구현:

- 백테스트를 Quant 실행 중 자동으로 돌리지 않는다.
- 실시간 가격 API를 호출하지 않는다.
- 모델 기반 점수 재계산을 하지 않는다.
- 종목 추천 문구를 생성하지 않는다.

## 10. 테스트 시나리오

### 10.1 수동 후보

명령:

```bash
rtk python scripts/run_free_agent_pipeline.py --candidate 005930:삼성전자:100000 --no-require-research-file
```

기대 결과:

- `quant_signal.json`이 생성된다.
- `signals[0].ticker == "005930"`
- `source_type == "manual"`
- `suggested_amount == 100000`
- status는 `info`

### 10.2 자동 발견 후보

명령:

```bash
rtk python scripts/run_free_agent_pipeline.py --discover --discover-limit 3 --no-require-research-file
```

기대 결과:

- 후보가 최대 3개 생성된다.
- 각 후보에 `ticker`, `name`, `source`, `source_type`, `signal_count`가 있다.
- 같은 ticker는 중복 행이 아니라 하나의 후보로 병합된다.

### 10.3 캔들 CSV

입력 예:

```text
signal_date,pattern_id,pattern_name,confidence,close,trade_amount,ticker,name
2026-05-07,tweezer_top,트위저 탑,65,263500,5118961250250,005930,삼성전자
```

기대 결과:

- `source_type == "candlestick"`
- `score == 65`
- `current_price == 263500`
- `trade_amount == 5118961250250`
- `signal_details[].condition == "tweezer_top"`

### 10.4 외국인 수급 CSV

입력 예:

```text
signal_date,ticker,name,condition_id,matched_conditions,event_close
2026-05-07,034020,두산에너빌리티,foreign_buy_streak_2,foreign_buy_streak_2,136400
```

기대 결과:

- `source_type == "foreign_flow"`
- `current_price == 136400`
- `signal_details[].condition == "foreign_buy_streak_2"`

### 10.5 후보 없음

조건:

- 수동 후보 없음
- `--discover` 미사용 또는 discovery root 비어 있음

기대 결과:

- `status == "needs_review"`
- warnings에 후보 없음 메시지 포함
- 파이프라인은 중단되지 않고 최종 보고서 생성

### 10.6 CSV 스키마 변형

조건:

- `종목코드`, `종목명`, `점수` 컬럼을 가진 CSV

기대 결과:

- ticker와 name을 추출한다.
- score를 추출한다.
- 모르는 컬럼은 무시한다.

## 11. 성공 기준

`QuantSignalAgent`는 다음 조건을 만족해야 성공이다.

- OpenAI API나 외부 유료 API 없이 동작한다.
- 수동 후보와 자동 발견 후보를 같은 `Candidate` 구조로 만든다.
- 자동 발견 후보는 `discover_limit`을 넘지 않는다.
- 중복 ticker는 병합된다.
- 신호 출처와 조건이 `signal_details`에 남는다.
- 가격과 거래대금이 있으면 추출한다.
- 가격과 거래대금이 없어도 실패하지 않고 경고 또는 `null`로 남긴다.
- 후보가 없으면 `needs_review`가 된다.
- Quant 결과만 보고 매수할 수 없다는 점이 문서와 보고서에 명확히 남는다.

## 12. 향후 고도화

v2 이후 개선 후보:

- DataQualityAgent 분리 전까지 Quant 안에서 CSV 품질 점수 계산
- 신호별 가중치 정책 추가
- 오래된 신호 자동 제외 옵션 추가
- 백테스트 성과와 연결한 condition quality score 추가
- 시장 국면별 신호 유효성 태그 추가
- 동일 종목에 상충 신호가 있을 때 conflict flag 추가
- 후보별 “왜 이 종목이 올라왔는지” 요약 문장 생성

v2에서도 유지할 원칙:

- Quant는 후보를 만들 뿐 투자 승인을 하지 않는다.
- 데이터 누락을 임의 추정하지 않는다.
- 모든 신호 출처는 추적 가능해야 한다.
