# EquityResearchAnalystAgent PLAN

작성일: 2026-05-08

## 1. 전문가 역할

`EquityResearchAnalystAgent`는 투자팀의 **기업 리서치 애널리스트**다.

역할은 Quant가 가져온 후보 종목에 대해 사업모델, 실적, 재무, 밸류에이션, 촉매, 리스크, 반증 조건을 구조화해 투자위원회가 검토할 수 있는 분석 초안을 만드는 것이다.

이 Agent는 주문 결정을 하지 않는다. 좋은 기업처럼 보여도 Portfolio, Risk, Compliance, Trader 단계를 반드시 통과해야 한다.

핵심 책임:

- 후보 기업의 사업모델과 수익 구조를 정리한다.
- 최근 실적과 재무 추세를 점검한다.
- 밸류에이션이 과도한지, 낮은지, 판단 불가인지 표시한다.
- 주가 변동 촉매와 이벤트를 정리한다.
- 핵심 리스크와 반증 조건을 제시한다.
- 리서치가 부족하면 “분석 미완료”로 남긴다.

하지 말아야 할 일:

- 매수/매도 주문을 지시하지 않는다.
- 목표가를 임의로 생성하지 않는다.
- 루머, 미확인 정보, 미공개 정보를 근거로 쓰지 않는다.
- Risk와 Compliance를 건너뛰지 않는다.
- 데이터가 없는데 확신 있는 결론을 만들지 않는다.
- 기존 리포트가 깨져 있거나 인코딩이 불완전한데 내용을 단정하지 않는다.

v1의 현실적 역할:

- 완전한 애널리스트 리포트를 자동 작성하는 것이 아니다.
- 기존 분석 파일과 신호 데이터를 읽어 “분석 가능/분석 부족/분석 불가”를 판단한다.
- 투자위원회가 보완해야 할 기업 분석 항목을 구체적으로 제시한다.
- 이후 사람이 직접 리포트를 보완하거나 별도 스크립트로 신규 리포트를 생성할 수 있게 한다.

## 2. 입력 계약

입력 후보는 `QuantSignalAgent`가 표준화한 `Candidate`다.

사용 필드:

| 필드 | 용도 |
|---|---|
| `ticker` | 기업 식별 |
| `name` | 기업별 리서치 폴더 매칭 |
| `source_type` | 후보가 나온 신호 유형 파악 |
| `signal_details` | 어떤 이벤트/조건이 후보화했는지 확인 |
| `current_price` | 밸류에이션과 가격 수준 검토 참고 |
| `trade_amount` | 관심도와 유동성 참고 |

주요 참고 데이터:

| 데이터 | 위치 |
|---|---|
| 기업별 기존 분석 | `ai 주가 변동 원인 분석/00_기업별분석/` |
| 기획/기준 문서 | `ai 주가 변동 원인 분석/01_기획/`, `02_기준/` |
| 원천 데이터 | `ai 주가 변동 원인 분석/03_원천데이터/` |
| 가설 검토 | `ai 주가 변동 원인 분석/05_가설검토/` |
| 백테스트 | `ai 주가 변동 원인 분석/06_백테스트/` |
| 전략 신호 | `ai 주가 변동 원인 분석/07_전략신호/` |
| 관찰 기록 | `ai 주가 변동 원인 분석/08_관찰기록/` |
| 일일 요약 | `ai 주가 변동 원인 분석/10_일일요약/` |
| 향후 공시/뉴스/재무 데이터 | 기존 scripts 산출물 또는 별도 데이터 파일 |

v1에서는 외부 유료 LLM API를 호출하지 않는다. 기존 파일과 규칙 기반 체크리스트로 분석 상태를 만든다.

### 2.1 필수 분석 항목

Analyst가 확인해야 하는 최소 분석 항목은 다음이다.

| 항목 | 질문 | 충분한 근거 예 |
|---|---|---|
| `business_model` | 이 회사는 무엇으로 돈을 버는가? | 제품/서비스, 고객, 매출 구조, 마진 구조 |
| `earnings_quality` | 최근 실적의 질은 어떤가? | 매출/영업이익 추세, 일회성 요인, 비용 구조 |
| `balance_sheet` | 재무 안정성은 어떤가? | 부채, 현금, 차입, 유동성 |
| `valuation` | 현재 가격은 비싼가 싼가? | PER/PBR/EV/EBITDA, 과거 밴드, peer 비교 |
| `catalyst` | 왜 지금 움직일 수 있는가? | 실적 발표, 수주, 정책, 공시, 수급, 업황 |
| `risk` | 틀릴 수 있는 이유는 무엇인가? | 실적 둔화, 경쟁, 규제, 재무 리스크 |
| `disconfirmation` | 어떤 일이 생기면 가설을 버릴 것인가? | 가격/실적/공시/수급 기반 반증 조건 |

### 2.2 분석 신뢰도

분석 신뢰도는 문서 존재 여부뿐 아니라 근거의 질로 평가한다.

| 등급 | 조건 |
|---|---|
| `high` | 필수 분석 항목이 대부분 있고, 최신 파일이며, 금지 정보 의존이 없다. |
| `medium` | 핵심 근거는 있으나 밸류에이션, 반증 조건, 리스크 중 일부가 약하다. |
| `low` | 파일은 있으나 원인후보/가격 이벤트 중심이라 기업 분석으로 부족하다. |
| `none` | 파일이 없거나 읽을 수 없다. |

## 3. 처리 프로세스

후보별 처리 순서:

```text
1. 후보 ticker/name 확인
2. 기업별 분석 폴더와 관련 파일 찾기
3. 최신 분석 파일과 보조 이벤트 파일 식별
4. 파일 읽기 가능 여부 확인
5. 사업모델 근거 확인
6. 실적/재무 근거 확인
7. 밸류에이션 근거 확인
8. 촉매와 이벤트 근거 확인
9. 핵심 리스크 확인
10. 반증 조건 확인
11. 금지/주의 정보 의존 여부 확인
12. 분석 완성도와 신뢰도 등급 부여
13. 분석 초안 또는 보완 필요 항목 출력
```

분석 완성도 등급:

| 등급 | 의미 |
|---|---|
| `complete` | 사업, 실적, 밸류에이션, 촉매, 리스크, 반증 조건이 모두 확인됨 |
| `partial` | 일부 근거는 있으나 투자위원회 검토 전 보완 필요 |
| `missing` | 분석 파일 또는 핵심 근거가 없음 |

분석 항목 판정:

| 항목 상태 | 의미 |
|---|---|
| `present` | 해당 항목의 근거가 문서에 명시되어 있다. |
| `weak` | 관련 단어는 있으나 투자 판단에 충분하지 않다. |
| `missing` | 해당 항목을 찾을 수 없다. |
| `unreadable` | 파일 인코딩/읽기 문제로 판단할 수 없다. |

## 4. 판정 기준

| Status | 조건 |
|---|---|
| `approve` | 모든 후보의 분석 완성도가 `complete`이고 금지 정보 의존이 없다. |
| `needs_review` | 분석이 없거나 `partial`, `missing`인 후보가 하나라도 있다. |
| `block` | 미공개 정보, 루머 의존 등 명백히 사용하면 안 되는 근거가 발견됐다. |
| `info` | 후보 없음 등 단순 정보 제공 상황. |

v1에서는 대부분 `needs_review`가 정상이다. 이 Agent의 목적은 확정 결론보다 분석 공백을 드러내는 것이다.

`block` 기준:

- 리서치 근거가 루머, 찌라시, 미공개 정보에 의존한다.
- 문서가 명백히 다른 기업에 대한 분석이다.
- 종목코드 또는 기업명이 후보와 충돌한다.

`needs_review` 기준:

- 분석 파일이 없다.
- 분석 파일은 있으나 사업모델/실적/밸류에이션/리스크/반증 조건 중 하나 이상이 없다.
- 최신 분석 파일이 너무 오래됐다.
- 파일 인코딩 문제로 내용을 신뢰하기 어렵다.
- 촉매가 가격 이벤트만 있고 기업 펀더멘털 근거가 없다.

## 5. 출력 계약

향후 구현 시 `equity_research_analyst.json`으로 저장한다.

후보별 출력 필드:

| 필드 | 의미 |
|---|---|
| `ticker` | 종목코드 |
| `name` | 종목명 |
| `analysis_status` | `complete`, `partial`, `missing` |
| `confidence` | `high`, `medium`, `low`, `none` |
| `business_model_summary` | 사업모델 요약 또는 비어 있음 |
| `earnings_check` | 실적/재무 확인 상태 |
| `balance_sheet_check` | 재무 안정성 확인 상태 |
| `valuation_check` | 밸류에이션 확인 상태 |
| `catalysts` | 촉매 목록 |
| `key_risks` | 핵심 리스크 목록 |
| `disconfirmation_conditions` | 반증 조건 |
| `missing_items` | 보완 필요 항목 |
| `forbidden_keyword_hits` | 루머/미공개 등 금지 정보 키워드 |
| `source_files` | 분석에 사용한 파일 |
| `analyst_notes` | 사람이 읽을 간단한 판단 메모 |

예상 출력 예:

```json
{
  "ticker": "005930",
  "name": "삼성전자",
  "analysis_status": "partial",
  "confidence": "medium",
  "business_model_summary": "반도체와 디바이스 중심 사업 구조 확인 필요",
  "earnings_check": "weak",
  "balance_sheet_check": "missing",
  "valuation_check": "missing",
  "catalysts": ["캔들/수급 신호 발생"],
  "key_risks": ["밸류에이션 근거 부족"],
  "disconfirmation_conditions": [],
  "missing_items": ["valuation", "disconfirmation"],
  "forbidden_keyword_hits": [],
  "source_files": ["..."],
  "analyst_notes": "기존 원인후보 분석은 있으나 투자 가설 문서로는 보완 필요"
}
```

## 6. 다른 Agent와의 계약

| 다음 Agent | 사용하는 정보 | 목적 |
|---|---|---|
| `ResearchFileAgent` | `source_files`, `missing_items` | 분석 파일 존재와 품질 검수 |
| `PortfolioManagerAgent` | `analysis_status`, `key_risks` | 후보 우선순위와 액션 보수화 |
| `ComplianceOfficerAgent` | source와 금지 정보 여부 | 투자근거와 기록 요건 확인 |
| `OperationsReportAgent` | missing_items, key_risks | 최종 보고서의 리서치 보완 항목 |

Analyst가 보장해야 하는 것:

- 분석 공백을 숨기지 않는다.
- 근거 파일을 추적 가능하게 남긴다.
- 투자 가설과 반증 조건을 분리한다.
- 분석 신뢰도를 `confidence`로 표시한다.
- 금지/주의 정보 의존 가능성을 별도 필드로 남긴다.

Analyst가 보장하지 않는 것:

- 투자 수익률
- 목표가 정확성
- 실적 추정 정확성
- 주문 가능 여부
- 리서치 파일 품질 최종 승인. 이 역할은 `ResearchFileAgent`와 `ComplianceOfficerAgent`가 맡는다.

## 7. 사람 확인 항목

- 사업모델 요약이 실제 기업 구조와 맞는지 확인한다.
- 최근 실적과 가이던스가 반영됐는지 확인한다.
- 밸류에이션 비교군이 적절한지 확인한다.
- 촉매가 이미 주가에 반영됐는지 확인한다.
- 리스크가 투자 가설을 무너뜨릴 정도인지 확인한다.
- 반증 조건이 구체적이고 관찰 가능한지 확인한다.
- 분석 파일이 깨져 보이면 원본 인코딩 또는 생성 스크립트를 확인한다.
- 원인후보 분석 파일을 투자 가설 문서로 사용할 수 있는지 확인한다.
- Quant 신호가 펀더멘털 분석과 충돌하는지 확인한다.

사람이 보완해야 하는 대표 항목:

```text
사업모델
최근 실적과 재무
밸류에이션
핵심 촉매
핵심 리스크
반증 조건
```

## 8. 실패/예외 처리

| 상황 | 처리 |
|---|---|
| 후보 없음 | `info` 또는 `needs_review`, 보고서에는 후보 없음 표시 |
| 기업별 분석 폴더 없음 | `analysis_status="missing"`, `confidence="none"` |
| 파일은 있으나 읽기 실패 | `analysis_status="partial"`, `confidence="low"` |
| 파일 인코딩 깨짐 | `missing_items`에 `readability` 추가 |
| 사업모델 없음 | `missing_items`에 `business_model` 추가 |
| 밸류에이션 없음 | `missing_items`에 `valuation` 추가 |
| 리스크 없음 | `missing_items`에 `risk` 추가 |
| 반증 조건 없음 | `missing_items`에 `disconfirmation` 추가 |
| 루머/미공개 키워드 발견 | `forbidden_keyword_hits` 기록, status는 `block` 또는 `needs_review` |
| 기존 파일이 가격 이벤트 중심 | `confidence="low"` 또는 `medium`, 보완 필요 |

실패 처리 원칙:

- 모르는 내용을 채우지 않는다.
- 분석 파일이 부족하면 부족한 항목을 구체적으로 남긴다.
- 파일을 자동 수정하거나 삭제하지 않는다.
- 주문 가능성 판단은 하지 않는다.

## 9. 구현 작업 목록

1. 새 Agent 클래스를 `core/agents/free_pipeline.py`에 추가한다.
2. Quant와 ResearchFile 사이에 실행 순서를 배치한다.
3. `equity_research_analyst.json` 산출물을 저장한다.
4. 기존 기업별 분석 파일을 읽어 체크리스트 기반 분석 상태를 만든다.
5. `analysis_status`, `missing_items`, `key_risks`, `disconfirmation_conditions`를 표준 출력한다.
6. Operations 보고서에 Analyst 요약 섹션을 추가한다.
7. 파일 읽기 품질을 `readability`로 기록한다.
8. 기존 `ResearchFileAgent`의 파일 탐색 로직을 재사용하거나 공통 helper로 분리한다.
9. Analyst 결과를 Portfolio priority에 반영할 수 있도록 `analysis_status` 계약을 고정한다.
10. Compliance가 금지/주의 키워드와 source_files를 확인할 수 있게 전달한다.

v1에서 하지 않는 구현:

- 유료 LLM API 호출
- 자동 목표가 산출
- 자동 매수 의견 생성
- 실시간 공시/뉴스 API 호출
- 완전한 자연어 장문 리포트 자동 생성

## 10. 테스트 시나리오

### 10.1 분석 파일 있음

조건:

- 후보: `005930:삼성전자`
- `00_기업별분석/삼성전자/` 폴더에 Markdown 파일 존재

기대 결과:

- `source_files`가 비어 있지 않다.
- `analysis_status`는 `complete` 또는 `partial`이다.
- 부족 항목은 `missing_items`에 기록된다.

### 10.2 분석 파일 없음

조건:

- 후보와 매칭되는 기업별 분석 폴더 또는 파일 없음

기대 결과:

- `analysis_status == "missing"`
- `confidence == "none"`
- `missing_items`에 주요 필수 분석 항목 포함
- status는 `needs_review`

### 10.3 가격 이벤트 중심 파일

조건:

- 원인후보 분석 파일은 있으나 사업모델, 밸류에이션, 반증 조건이 없음

기대 결과:

- `analysis_status == "partial"`
- `confidence`는 `low` 또는 `medium`
- `missing_items`에 `business_model`, `valuation`, `disconfirmation` 중 누락 항목 기록

### 10.4 금지 정보 키워드

조건:

- 파일에 `루머`, `미공개`, `확인필요` 등 키워드 포함

기대 결과:

- `forbidden_keyword_hits`에 키워드 기록
- status는 `block` 또는 `needs_review`
- Compliance가 같은 위험을 확인할 수 있다.

### 10.5 후보 없음

조건:

- Quant 후보 0개

기대 결과:

- 파이프라인은 종료되지 않는다.
- Analyst 산출물은 후보 없음 상태를 기록한다.

## 11. 성공 기준

- Quant 신호와 기업 분석이 분리된다.
- 투자 가설, 리스크, 반증 조건이 별도 필드로 남는다.
- 분석이 부족한 후보가 자동 매수로 넘어가지 않는다.
- ResearchFileAgent가 분석 파일 품질을 별도로 검수할 수 있다.
- 기존 분석 파일이 있어도 부족한 항목이 숨겨지지 않는다.
- 금지/주의 정보 의존 가능성이 별도 필드로 남는다.
- Portfolio와 Compliance가 Analyst 결과를 사용할 수 있다.

## 12. 향후 고도화

v2 이후 개선 후보:

- DART 재무제표 기반 실적/재무 자동 체크
- peer valuation table 생성
- 과거 밸류에이션 밴드 계산
- 투자 가설과 반증 조건 템플릿 파일 생성
- 로컬 LLM 기반 기존 리포트 요약
- Analyst 결과를 ResearchFileAgent가 직접 검수하도록 파이프라인 계약 개선
- 후보별 analyst score 도입

v2에서도 유지할 원칙:

- Analyst는 분석을 제공할 뿐 주문하지 않는다.
- 불확실한 내용은 확신 있는 결론으로 바꾸지 않는다.
- 분석 근거는 항상 파일과 출처로 추적 가능해야 한다.
