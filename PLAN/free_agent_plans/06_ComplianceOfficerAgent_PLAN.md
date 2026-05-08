# ComplianceOfficerAgent PLAN

작성일: 2026-05-08

## 1. 전문가 역할

`ComplianceOfficerAgent`는 투자팀의 **준법/기록 통제 책임자**다.

역할은 좋은 종목을 찾는 것이 아니라, 투자 판단이 추적 가능한 근거와 허용 가능한 정보에 기반했는지 확인하는 것이다. 이 Agent는 법률 의견을 제공하지 않는다. 대신 자동매매 파이프라인 안에서 “근거 없는 주문”, “출처 불명 후보”, “미확인/미공개 정보 의존”, “기록 누락”을 막는 독립 Gate 역할을 한다.

핵심 책임:

- 종목코드가 표준 형식인지 확인한다.
- 후보가 어디서 나왔는지 source를 확인한다.
- 수동 후보는 출처와 입력 사유가 남아 있는지 확인한다.
- 리서치 파일이 존재하는지 확인한다.
- 리서치 파일이 투자 판단에 필요한 최소 섹션을 갖췄는지 확인한다.
- 오래된 리서치 파일을 경고한다.
- 루머, 미확인, 미공개 정보 키워드를 탐지한다.
- Analyst가 참조한 파일과 ResearchFile 검수 결과가 충돌하는지 확인한다.
- 주문 제안이 최종 보고서와 JSON 기록에 남을 수 있는지 확인한다.

하지 말아야 할 일:

- 법률, 세무, 금융규제 자문을 하지 않는다.
- 수익률이 좋을 것 같다는 이유로 절차 위반을 승인하지 않는다.
- Risk 한도 판단을 대신하지 않는다.
- Analyst의 기업 분석을 대신 작성하지 않는다.
- 리서치 파일을 자동 수정하거나 삭제하지 않는다.
- 미확인 정보를 “참고용”이라는 이유로 승인하지 않는다.
- 사람이 확인해야 할 내용을 자동 통과시키지 않는다.

중요 원칙:

```text
Compliance status가 approve가 아니면 TraderAgent는 주문안을 hold로 낮춘다.
```

## 2. 입력 계약

### 2.1 후보 입력

Compliance는 `Candidate`를 기준으로 기본 절차 검사를 수행한다.

| 필드 | 용도 |
|---|---|
| `ticker` | 6자리 숫자 종목코드 검사 |
| `name` | 리서치 파일과 기업명 매칭 참고 |
| `source` | 후보 출처 기록 여부 확인 |
| `source_type` | 수동/자동 후보 구분 |
| `signal_details` | 후보 생성 근거 추적 |

### 2.2 Analyst 입력

`EquityResearchAnalystAgent`가 구현된 이후 Compliance는 다음 필드를 사용한다.

| 필드 | 용도 |
|---|---|
| `analysis_status` | 분석이 `missing` 또는 `partial`이면 절차상 검토 필요 |
| `confidence` | 분석 신뢰도가 낮으면 검토 필요 |
| `source_files` | 근거 파일이 실제 존재하는지 확인 |
| `missing_items` | 투자 근거 누락 항목 확인 |
| `forbidden_keyword_hits` | 금지/주의 정보 의존 가능성 확인 |
| `analyst_notes` | 사람이 확인해야 할 절차 이슈 추적 |

Compliance는 Analyst 결론을 투자 승인으로 해석하지 않는다. Analyst가 `complete`라고 해도 파일 검수나 금지 정보 문제가 있으면 `needs_review` 또는 `block`이 될 수 있다.

### 2.3 ResearchFile 입력

`ResearchFileAgent` 결과에서 사용하는 필드:

| 필드 | 용도 |
|---|---|
| `research_files` | 후보별 리서치 파일 존재 여부 |
| `quality_status` | `usable`, `incomplete`, `missing`, `stale`, `unreadable` 확인 |
| `missing_required_sections` | 필수 섹션 누락 확인 |
| `forbidden_keyword_hits` | 금지/주의 키워드 확인 |
| `is_stale` | 오래된 리서치 경고 |
| `analyst_source_file_mismatch` | Analyst와 파일 검수 결과 충돌 확인 |

현재 v1 구현에서는 Compliance가 파일 검색과 품질 검사를 직접 다시 수행한다. 향후에는 ResearchFile 결과를 직접 전달받아 중복 검사를 줄인다.

### 2.4 설정 입력

| 설정 | 의미 |
|---|---|
| `require_research_file` | 리서치 파일이 필수인지 여부 |
| `stale_signal_days` | 리서치 최신성 기준 |
| `forbidden_keywords` | 금지/주의 키워드 목록 |
| `required_sections` | 필수 리서치 섹션 목록 |

기본 금지/주의 키워드:

```text
루머
찌라시
미확인
확인필요
미공개
```

기본 필수 섹션:

```text
사업모델
투자 가설
리스크
반증 조건
```

## 3. 처리 프로세스

후보별 처리 순서:

```text
1. 후보 ticker/name/source 확인
2. ticker가 6자리 숫자인지 검사
3. source_type이 manual이면 source가 남아 있는지 검사
4. Analyst 결과가 있으면 analysis_status와 source_files 확인
5. ResearchFile 결과 또는 파일 시스템 검색으로 리서치 파일 확인
6. 리서치 필수 여부와 실제 파일 존재 여부 비교
7. 최신 리서치 파일 기준 필수 섹션 누락 확인
8. 금지/주의 키워드 탐지
9. Analyst source_files와 ResearchFile 검수 결과 충돌 확인
10. 후보별 compliance status 생성
11. 사람이 확인해야 할 항목 기록
12. 전체 Compliance status 계산
```

기록 추적성 검사:

- 후보가 어떤 신호에서 왔는지 확인 가능해야 한다.
- 수동 후보는 사람이 왜 넣었는지 최소한의 source가 남아야 한다.
- 리서치 파일 경로가 JSON 결과에 남아야 한다.
- 누락 섹션과 금지 키워드가 사람이 읽을 수 있게 남아야 한다.
- 최종 주문안은 Risk와 Compliance 상태를 함께 기록해야 한다.

## 4. 판정 기준

| Status | 조건 |
|---|---|
| `approve` | 종목코드, 출처, 리서치 기록, 금지 정보, Analyst/Research 충돌 검사를 모두 통과했다. |
| `needs_review` | 근거 부족, 리서치 누락, 출처 불명, 오래된 파일, 섹션 누락, 금지 키워드 경고 등 사람이 봐야 할 이슈가 있다. |
| `block` | 명백히 사용하면 안 되는 정보 또는 구조적 오류가 있다. |
| `info` | 후보 없음 등 단순 정보 제공 상황에서만 가능하다. |

`block` 조건:

- `ticker`가 6자리 숫자가 아니다.
- 문서가 명백히 다른 기업을 가리킨다.
- 미공개 정보 또는 루머만을 투자 근거로 삼는다.
- Analyst source file이 존재하지 않고 대체 근거도 없는데 분석 완료로 표시되어 있다.
- 주문 제안의 필수 기록 필드가 구조적으로 깨져 있다.

`needs_review` 조건:

- 리서치 파일이 없다.
- 리서치 파일은 있으나 필수 섹션이 빠져 있다.
- 리서치 파일이 stale 기준보다 오래됐다.
- 수동 후보인데 source가 비어 있다.
- 후보 source는 있으나 사람이 이해할 수 있는 근거가 부족하다.
- `루머`, `찌라시`, `미확인`, `확인필요`, `미공개` 키워드가 발견됐다.
- Analyst 결과가 `partial`, `missing`, `confidence=low`, `confidence=none`이다.
- Analyst와 ResearchFile의 파일 목록 또는 누락 항목이 충돌한다.
- 파일을 읽을 수 없어 검증이 불완전하다.

`approve` 조건:

- ticker 형식이 정상이다.
- 후보 source가 추적 가능하다.
- 리서치 파일이 있고 최신성/필수 섹션 검사를 통과했다.
- 금지/주의 키워드가 없다.
- Analyst와 ResearchFile 결과가 충돌하지 않는다.
- 필요한 사람이 확인할 항목이 남아 있지 않다.

## 5. 출력 계약

Compliance 결과는 `compliance_officer.json`으로 저장된다.

공통 구조:

```json
{
  "agent": "ComplianceOfficerAgent",
  "status": "needs_review",
  "summary": "Compliance guardrail checks completed.",
  "signals": [],
  "warnings": [],
  "required_human_checks": [],
  "artifacts": {
    "require_research_file": true
  }
}
```

각 `signals[]` 항목:

| 필드 | 의미 |
|---|---|
| `ticker` | 후보 종목코드 |
| `status` | 후보별 Compliance 상태 |
| `research_file_count` | 매칭된 리서치 파일 수 |
| `missing_required_sections` | 필수 섹션 누락 목록 |
| `forbidden_keyword_hits` | 금지/주의 키워드 목록 |
| `warnings` | 후보별 경고 |

향후 확장 필드:

| 필드 | 의미 |
|---|---|
| `record_status` | 기록 완결성 상태 |
| `manual_source_status` | 수동 후보 출처 검증 상태 |
| `analyst_status` | Analyst 결과 요약 |
| `research_quality_status` | ResearchFile 품질 요약 |
| `source_file_mismatch` | Analyst와 ResearchFile 파일 충돌 여부 |
| `compliance_checks` | 검사 항목별 pass/fail 목록 |

## 6. 다른 Agent와의 계약

| Agent | 사용하는 정보 | 목적 |
|---|---|---|
| `EquityResearchAnalystAgent` | source_files, forbidden_keyword_hits, analysis_status | 분석 근거와 금지 정보 의존 확인 |
| `ResearchFileAgent` | research_files, quality_status, stale, missing sections | 기록 품질 검증 |
| `RiskManagerAgent` | 직접 의존 없음 | 독립 Gate 유지 |
| `TraderAgent` | Compliance status | `approve`가 아니면 주문안을 `hold`로 낮춤 |
| `OperationsReportAgent` | warnings, required_human_checks | 최종 보고서의 준법/기록 섹션 |

Compliance가 보장해야 하는 것:

- 출처 없는 후보는 승인하지 않는다.
- 투자 근거 파일이 부족하면 승인하지 않는다.
- 금지/주의 키워드를 숨기지 않는다.
- Analyst와 ResearchFile 결과가 충돌하면 사람 검토로 넘긴다.
- 모든 경고는 최종 보고서에 남길 수 있게 구조화한다.

Compliance가 보장하지 않는 것:

- 법률적으로 문제가 없다는 확정 의견
- 투자 수익 가능성
- 기업 가치 적정성
- 리스크 한도 적정성
- 실제 주문 체결 가능성
- 세금, 공시 의무, 전문투자자 규정 해석

## 7. 사람 확인 항목

Compliance 단계에서 사람이 확인해야 할 항목:

- 후보 source가 실제로 신뢰 가능한 출처인지 확인한다.
- 수동 후보 입력 사유가 충분히 남아 있는지 확인한다.
- 리서치 파일이 실제 해당 기업 문서인지 확인한다.
- 오래된 리서치가 현재 판단에 여전히 유효한지 확인한다.
- 금지/주의 키워드가 단순 경고 문맥인지 실제 근거 의존인지 확인한다.
- Analyst가 사용한 파일이 ResearchFile 검수 결과와 일치하는지 확인한다.
- 투자 근거가 가격 이벤트만으로 구성되어 있지 않은지 확인한다.
- 최종 보고서에 Risk와 Compliance의 hold 사유가 누락되지 않았는지 확인한다.

특히 다음 문장이 포함된 문서는 자동 승인하지 않는다.

```text
루머 기반
확인 필요
미확인 정보
미공개 정보
찌라시
```

문맥상 부정 예시로 등장했더라도 사람이 확인해야 한다.

## 8. 실패/예외 처리

| 상황 | 처리 |
|---|---|
| 후보 없음 | `info` 또는 `needs_review`, 보고서에 후보 없음 기록 |
| ticker 형식 오류 | `block` |
| 수동 후보 source 없음 | `needs_review` |
| 리서치 파일 없음 | `needs_review` |
| 리서치 필수 설정이 꺼져 있음 | 파일 없음은 경고로 남기되 정책에 따라 status 조정 |
| 필수 섹션 누락 | `needs_review` |
| 리서치 stale | `needs_review` |
| 금지/주의 키워드 발견 | 기본 `needs_review`, 미공개/루머 의존이 명확하면 `block` |
| Analyst source_files 존재하지 않음 | `needs_review` 또는 구조적 충돌이면 `block` |
| Analyst complete, ResearchFile incomplete | `needs_review` |
| 파일 읽기 실패 | `needs_review` |
| 주문 proposal 필드 누락 | `needs_review` 또는 필수 구조 오류면 `block` |

실패 처리 원칙:

- 불확실한 정보는 자동 승인하지 않는다.
- 준법/기록 문제는 수익성 판단으로 덮지 않는다.
- 원본 리서치 파일은 자동 수정하지 않는다.
- 파일 삭제가 필요해 보여도 사용자 승인 없이 삭제하지 않는다.

## 9. 구현 작업 목록

현재 구현 기준에서 Compliance 관련 보강 작업은 다음 순서로 진행한다.

1. `check_compliance_rules()`의 경고를 후보별 구조화 필드로 확장한다.
2. `ComplianceOfficerAgent`가 ResearchFile 결과를 직접 입력받도록 pipeline 계약을 개선한다.
3. Analyst 결과의 `analysis_status`, `source_files`, `forbidden_keyword_hits`를 Compliance 검사에 연결한다.
4. 수동 후보 source 검사 기준을 명확히 한다.
5. `record_status`, `manual_source_status`, `research_quality_status`를 signals에 추가한다.
6. 금지/주의 키워드를 문서 상수로 분리한다.
7. 주문 제안 필드 검사를 Compliance 또는 Trader 전 단계에서 명확히 연결한다.
8. Operations 보고서에 Compliance 표를 추가한다.
9. `require_research_file=false`일 때도 경고가 최종 보고서에 남도록 한다.

v1에서 하지 않는 구현:

- 법률 데이터베이스 또는 규정 API 호출
- 공시 위반 여부 자동 판단
- 내부자 정보 여부의 법률적 확정 판단
- 뉴스 원문 전체 크롤링
- 리서치 파일 자동 수정
- 문제 파일 자동 삭제

## 10. 테스트 시나리오

### 10.1 정상 통과

조건:

- ticker가 6자리 숫자
- source가 존재
- 리서치 파일 존재
- 필수 섹션 존재
- 금지/주의 키워드 없음

기대 결과:

- 후보 status는 `approve`
- warnings는 비어 있거나 정보성만 포함

### 10.2 ticker 형식 오류

조건:

- 후보 ticker가 `ABC`, `00593`, 빈 값 등

기대 결과:

- 후보 status는 `block`
- warnings에 ticker 형식 오류 포함
- Trader에서 `hold`

### 10.3 수동 후보 source 없음

조건:

- `source_type == "manual"`
- `source == ""`

기대 결과:

- 후보 status는 `needs_review`
- warnings에 manual source 누락 포함
- Trader에서 `hold`

### 10.4 리서치 파일 없음

조건:

- `require_research_file == true`
- 후보와 매칭되는 리서치 파일 없음

기대 결과:

- 후보 status는 `needs_review`
- `research_file_count == 0`
- Trader에서 `hold`

### 10.5 필수 섹션 누락

조건:

- 리서치 파일은 있으나 사업모델, 투자 가설, 리스크, 반증 조건 중 일부 없음

기대 결과:

- `missing_required_sections`에 누락 항목 기록
- 후보 status는 `needs_review`

### 10.6 금지/주의 키워드

조건:

- 리서치 파일 또는 Analyst 결과에 `루머`, `미공개`, `미확인` 등 포함

기대 결과:

- `forbidden_keyword_hits`에 키워드 기록
- 기본 status는 `needs_review`
- 실제 투자 근거가 해당 정보에 의존하면 `block`

### 10.7 Analyst/Research 충돌

조건:

- Analyst는 `analysis_status="complete"`
- ResearchFile은 `quality_status="incomplete"` 또는 source file mismatch 표시

기대 결과:

- 후보 status는 `needs_review`
- warnings에 Analyst/Research mismatch 포함

### 10.8 리서치 필수 설정 해제

조건:

- `require_research_file == false`
- 리서치 파일 없음

기대 결과:

- 정책상 즉시 `needs_review`가 아닐 수 있지만 warnings에는 파일 없음 기록
- 최종 보고서에 “근거 파일 없이 검토됨”이 표시됨

## 11. 성공 기준

`ComplianceOfficerAgent`는 다음 조건을 만족해야 성공이다.

- 종목코드 오류는 `block`으로 남긴다.
- 출처 없는 수동 후보는 승인하지 않는다.
- 리서치 파일 부족은 승인으로 처리하지 않는다.
- 필수 섹션 누락이 구조화되어 남는다.
- 금지/주의 키워드가 구조화되어 남는다.
- Analyst와 ResearchFile 결과 충돌이 사람 검토로 남는다.
- Compliance가 `approve`가 아니면 Trader 주문안은 `hold`가 된다.
- 최종 보고서에 준법/기록 경고가 누락되지 않는다.
- 원본 파일은 자동 수정하거나 삭제하지 않는다.

## 12. 향후 고도화

v2 이후 개선 후보:

- `compliance_policy.json` 도입
- ResearchFile 결과 직접 재사용으로 중복 파일 검색 제거
- 후보 source별 신뢰도 등급화
- 금지/주의 키워드 문맥 분류
- 공시/뉴스 출처 URL 기록 표준화
- 최종 주문 proposal의 audit trail 생성
- 승인/보류 사유를 사람이 체크할 수 있는 Markdown 체크리스트 생성
- 일별 Compliance 이슈 누적 리포트 작성

v2에서도 유지할 원칙:

- Compliance는 수익성을 평가하지 않는다.
- 절차가 불완전하면 승인하지 않는다.
- 불확실한 정보는 사람 검토로 넘긴다.
- 파일 삭제와 원본 수정은 자동으로 하지 않는다.
