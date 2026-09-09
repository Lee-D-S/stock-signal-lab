# ResearchFileAgent PLAN

작성일: 2026-05-08

## 1. 전문가 역할

`ResearchFileAgent`는 투자팀의 **리서치 파일/품질 검수 책임자**다.

역할은 `EquityResearchAnalystAgent` 또는 기존 분석 파이프라인이 만든 리서치 파일이 실제로 존재하는지, 최신인지, 최소한의 논리 구조를 갖추고 있는지 확인하는 것이다. 이 Agent는 분석을 새로 쓰는 애널리스트가 아니라, 분석 기록과 품질을 검수하는 역할이다.

핵심 책임:

- 후보 종목별 리서치 파일을 찾는다.
- Analyst가 참조할 수 있는 기존 분석 파일을 식별한다.
- 파일명뿐 아니라 상위 폴더명까지 매칭한다.
- 리서치 파일의 최신 수정일을 기록한다.
- 필수 논리 섹션 존재 여부를 검사한다.
- 미확인 정보나 루머성 키워드를 탐지한다.
- 리서치가 부족한 후보는 `needs_review`로 다음 Agent에 전달한다.
- Analyst 산출물의 `source_files`, `missing_items`, `forbidden_keyword_hits`가 파일 검수 결과와 일치하는지 확인한다.

하지 말아야 할 일:

- 새 리서치 리포트를 자동 작성하지 않는다.
- 기업 분석을 직접 수행하지 않는다. 그 책임은 `EquityResearchAnalystAgent`에 있다.
- 리서치 파일이 있다는 이유만으로 투자 승인을 하지 않는다.
- 근거 없는 매수 의견이나 목표가를 만들지 않는다.
- 가격, 수량, 비중 판단을 하지 않는다.
- 법률/세무/인허가 의견을 제공하지 않는다.

## 2. 입력 계약

### 2.1 후보 입력

`ResearchFileAgent`는 `AgentContext.candidates`를 입력으로 받는다. Quant 단계에서 표준화된 후보가 들어온다.

후보에서 사용하는 필드:

| 필드 | 용도 |
|---|---|
| `ticker` | 파일명 또는 문서 경로에 종목코드가 포함되어 있는지 확인 |
| `name` | 기업별 분석 폴더 또는 파일명 매칭 |
| `source` | 수동 후보 또는 신호 출처 참고 |
| `source_type` | 리서치 요구 강도 판단 참고 |

### 2.2 Analyst 입력

`EquityResearchAnalystAgent`가 구현된 이후에는 `ResearchFileAgent`가 Analyst 결과도 함께 입력으로 받아야 한다.

Analyst 결과에서 사용하는 필드:

| 필드 | 용도 |
|---|---|
| `ticker` | 후보 매칭 |
| `analysis_status` | 분석 완성도와 파일 검수 결과 비교 |
| `confidence` | 파일 품질 검수 강도 판단 |
| `source_files` | Analyst가 실제로 근거로 삼은 파일 존재 여부 확인 |
| `missing_items` | Analyst가 말한 누락 항목과 파일 품질 검사 결과 비교 |
| `forbidden_keyword_hits` | 금지/주의 키워드 재확인 |

v1 현재 구현에서는 ResearchFile이 파일 검색과 품질 검사를 직접 수행한다. Analyst가 추가 구현되면 같은 파일을 두 번 찾지 않도록 공통 helper 또는 pipeline 전달 구조로 정리한다.

### 2.3 리서치 루트

기본 리서치 루트:

```text
ai 주가 변동 원인 분석/00_기업별분석/
```

현재 구조는 기업별 폴더 아래에 분기별 Markdown과 이벤트 JSONL이 쌓이는 형태다.

예시:

```text
00_기업별분석/
  삼성전자/
    삼성전자_2025_Q4_원인후보_실제분석.md
    삼성전자_2025_Q4_events.jsonl
  SKC/
    ...
```

v1에서 검색 대상 파일 확장자는 다음으로 제한한다.

| 확장자 | 처리 |
|---|---|
| `.md` | 우선 리서치 본문으로 검사 |
| `.txt` | 보조 리서치 텍스트로 검사 |
| `.csv` | 보조 데이터 문서로 검사 |
| `.jsonl` | v1에서는 직접 품질 검사 대상이 아님 |

### 2.4 필수 리서치 섹션

리서치 품질을 판단하기 위한 필수 논리 항목은 다음이다.

| 내부 키 | 의미 | 허용 키워드 예 |
|---|---|---|
| `business_model` | 사업모델과 수익 구조 | `사업모델`, `business model`, `BM`, `매출 구조` |
| `investment_thesis` | 투자 가설과 상승 근거 | `투자 가설`, `investment thesis`, `thesis`, `상승 근거` |
| `risk` | 주요 리스크 | `리스크`, `risk`, `위험` |
| `disconfirmation` | 반증 조건과 무효화 조건 | `반증 조건`, `반증`, `disconfirm`, `무효화` |

필수 섹션이 없다는 것은 “리서치 파일이 없다는 뜻”이 아니라, “투자 근거 문서로 쓰기에는 사람이 보완해야 한다는 뜻”이다.

Analyst 필수 분석 항목과의 관계:

| Analyst 항목 | ResearchFile 검수 항목 |
|---|---|
| `business_model` | `business_model` 섹션 존재 여부 |
| `earnings_quality` | 투자 가설 또는 실적 근거가 파일에 있는지 |
| `valuation` | `investment_thesis` 안에 가격/밸류에이션 근거가 있는지 |
| `risk` | `risk` 섹션 존재 여부 |
| `disconfirmation` | `disconfirmation` 섹션 존재 여부 |

ResearchFile은 Analyst처럼 내용을 해석하지 않는다. 파일 안에 해당 논리 구조가 “기록되어 있는지”를 검수한다.

### 2.5 금지/주의 키워드

다음 키워드는 자동 차단이 아니라 사람 확인 경고로 남긴다.

```text
루머
찌라시
미확인
확인필요
미공개
```

이 키워드가 있으면 `forbidden_keyword_hits`에 기록하고 Compliance가 다시 절차상 검토한다.

### 2.6 최신성 기준

기본 stale 기준은 120일이다.

판단 기준:

- 최신 파일의 수정일이 실행일 기준 120일을 넘으면 `is_stale=true`
- 리서치 파일이 없으면 `is_stale=false`로 두고 별도 `no research file found` 경고를 남긴다.
- 오래된 리포트는 삭제하지 않고 검토 경고로 남긴다.

## 3. 처리 프로세스

`ResearchFileAgent`는 후보별로 다음 순서로 동작한다.

```text
1. 후보 ticker와 name 확인
2. Analyst 결과가 있으면 source_files와 missing_items 확인
3. research_root 존재 여부 확인
4. research_root 아래 파일 검색
5. 파일명과 상위 폴더명에서 ticker 또는 name 매칭
6. Analyst source_files와 실제 매칭 파일 비교
7. 매칭 파일 최대 20개 정렬
8. 최신 파일 선택
9. 최신 파일 본문 읽기
10. 필수 섹션 키워드 검사
11. 금지/주의 키워드 검사
12. 최신성 검사
13. Analyst 누락 항목과 ResearchFile 누락 항목 비교
14. 후보별 research signal 생성
```

파일 매칭 원칙:

- `ticker`가 파일명 또는 상위 폴더 경로에 있으면 매칭한다.
- `name`이 파일명 또는 상위 폴더 경로에 있으면 매칭한다.
- 폴더명 매칭을 허용한다. 기업별 분석 폴더는 종목명 중심이기 때문이다.
- `.md`, `.txt`, `.csv`만 품질 검사 후보로 삼는다.
- 매칭 파일이 많으면 우선 20개까지만 결과에 남긴다.

본문 읽기 원칙:

- `utf-8`, `utf-8-sig`, `cp949` 순서로 읽기를 시도한다.
- 읽기 실패 시 파일은 매칭되었지만 품질 검사는 불완전하므로 경고한다.
- 품질 검사는 최신 파일 기준으로 수행한다.
- 파일 전체가 너무 크면 앞부분 일정 크기만 검사해도 된다.

## 4. 판정 기준

`ResearchFileAgent`는 투자 승인 Agent가 아니다. 리서치 품질 상태를 알려주는 Gate다.

| Status | 조건 |
|---|---|
| `approve` | 모든 후보에 리서치 파일이 있고, 최신이며, 필수 섹션과 금지 키워드 문제가 없다. |
| `needs_review` | 리서치 파일 없음, 섹션 누락, 오래된 리포트, 금지/주의 키워드, 후보 없음 중 하나라도 있다. |
| `block` | v1에서는 원칙적으로 사용하지 않는다. 단, Analyst가 명백한 다른 기업 파일 또는 미공개정보 의존을 표시한 경우 향후 `block`으로 승격할 수 있다. |
| `info` | 사용하지 않는다. 리서치 검사는 통과 또는 검토 필요로 남긴다. |

경고를 남겨야 하는 경우:

- 후보 종목의 리서치 파일이 없다.
- 리서치 루트가 없다.
- 최신 리서치 파일이 stale 기준보다 오래됐다.
- 필수 섹션이 누락됐다.
- 금지/주의 키워드가 있다.
- 파일은 찾았지만 본문을 읽을 수 없다.
- Analyst가 참조한 `source_files`가 실제로 존재하지 않는다.
- Analyst가 `complete`라고 했지만 ResearchFile 기준 필수 섹션이 누락됐다.
- 후보가 0개다.

중요 원칙:

- 리서치가 부족해도 후보를 삭제하지 않는다.
- Research 단계의 `needs_review`는 Trader에서 직접 차단하지 않는다.
- 대신 Compliance가 같은 품질 정보를 절차상 다시 검사하고, Trader는 Compliance 상태를 따른다.

## 5. 출력 계약

Research 결과는 `research_file.json`으로 저장된다.

공통 구조:

```json
{
  "agent": "ResearchFileAgent",
  "status": "needs_review",
  "summary": "Research file presence check completed.",
  "signals": [],
  "warnings": [],
  "required_human_checks": [],
  "artifacts": {
    "research_root": "..."
  }
}
```

각 `signals[]` 항목:

| 필드 | 의미 |
|---|---|
| `ticker` | 후보 종목코드 |
| `name` | 후보 종목명 |
| `research_files` | 매칭된 리서치 파일 경로 목록 |
| `latest_modified` | 최신 매칭 파일 수정 시각 |
| `missing_required_sections` | 누락된 필수 섹션 키 목록 |
| `forbidden_keyword_hits` | 발견된 금지/주의 키워드 |
| `is_stale` | 최신 파일이 오래됐는지 여부 |
| `analyst_source_file_mismatch` | Analyst가 사용한 파일과 실제 파일 검수 결과가 다른지 여부 |
| `quality_status` | `usable`, `incomplete`, `missing`, `stale`, `unreadable` |

`research_files`는 최대 5개까지만 AgentResult signal에 표시한다. 전체 매칭이 필요하면 구현에서 별도 artifact 확장을 고려한다.

## 6. 다른 Agent와의 계약

Research 출력은 다음 단계에 직접 또는 간접으로 영향을 준다.

| 다음 Agent | 사용하는 정보 | 목적 |
|---|---|---|
| `EquityResearchAnalystAgent` | v1에서는 선행 Agent. 향후 공통 파일 helper 공유 | 분석 파일과 품질 기준 일관성 유지 |
| `ComplianceOfficerAgent` | research file 존재, 섹션 누락, 금지 키워드, stale 여부 | 투자근거와 기록 요건 검사 |
| `PortfolioManagerAgent` | 향후 thesis quality score | 리서치 품질에 따른 우선순위 조정 |
| `OperationsReportAgent` | warnings, required_human_checks | 최종 보고서의 리서치 보완 항목 |

v1 현재 구현에서는 Compliance가 `find_research_files()`와 `analyze_research_files()`를 다시 실행한다. 문서상 계약은 Research 결과와 Compliance 결과가 같은 판단 기준을 공유해야 한다는 것이다.

Research가 보장해야 하는 것:

- 리서치 파일 경로는 추적 가능하게 남긴다.
- 누락 섹션은 내부 키로 기록한다.
- 오래된 파일 여부는 Boolean으로 기록한다.
- 금지/주의 키워드는 실제 발견된 단어로 기록한다.
- Analyst가 참조한 파일이 실제로 존재하는지 검수한다.
- 파일 품질 상태를 `quality_status`로 요약한다.

Research가 보장하지 않는 것:

- 리서치 내용이 맞다는 보장
- 실적 추정이 정확하다는 보장
- 투자 가설이 유효하다는 보장
- 공시/뉴스가 최신이라는 보장
- 매수할 가치가 있다는 보장
- 기업 분석 자체의 완성. 이 책임은 `EquityResearchAnalystAgent`에 있다.

## 7. 사람 확인 항목

Research 단계에서 사람이 확인해야 할 항목:

- 매칭된 리서치 파일이 실제 해당 기업 문서인지 확인한다.
- 최신 파일이 현재 투자 판단에 여전히 유효한지 확인한다.
- 사업모델, 투자 가설, 리스크, 반증 조건이 실제로 충분히 작성되어 있는지 확인한다.
- 분기별 원인후보 분석 파일이 투자 가설 문서로 사용 가능한지 확인한다.
- Analyst가 `complete`로 판단한 후보가 실제 파일 검수에서도 충분한지 확인한다.
- Analyst와 ResearchFile의 누락 항목이 서로 다르면 어느 쪽 기준이 맞는지 확인한다.
- 금지/주의 키워드가 단순 경고 문맥인지, 실제 미확인 정보 의존인지 확인한다.
- 리서치 파일이 없으면 신규 기업 리포트 생성이 필요한지 판단한다.

보고서에는 다음 안내를 반드시 남긴다.

```text
If no usable report exists, run scripts/run_new_company_reports.py --include-existing-missing.
```

## 8. 실패/예외 처리

| 상황 | 처리 |
|---|---|
| `research_root` 없음 | 모든 후보에 리서치 없음 경고, `needs_review` |
| 후보 0개 | `needs_review` |
| 매칭 파일 없음 | 후보별 `no research file found` 경고 |
| 파일은 있으나 읽기 실패 | 파일 경로는 남기고 품질 검사 경고 |
| Analyst source_files 없음 | 파일 시스템 검색 결과를 기준으로 검수 |
| Analyst source_files가 실제 없음 | mismatch 경고 |
| Analyst complete, ResearchFile incomplete | `needs_review`, mismatch 경고 |
| 필수 섹션 없음 | `missing_required_sections`에 기록 |
| 금지 키워드 발견 | `forbidden_keyword_hits`에 기록 |
| 최신 파일 120일 초과 | `is_stale=true` |
| 동일 종목 파일 다수 | 최신 파일 기준 품질 검사, 일부 경로만 출력 |
| 파일명에는 없고 폴더명에만 종목명 있음 | 매칭 허용 |

실패 처리 원칙:

- 리서치 품질 부족은 자동 매수 금지가 아니라 사람 검토 사유다.
- 단, Compliance는 리서치 부족을 절차상 `needs_review`로 유지해야 한다.
- 리서치 파일을 자동 삭제하거나 자동 수정하지 않는다.
- 문서 인코딩이 깨져도 원본 파일은 건드리지 않는다.

## 9. 구현 작업 목록

현재 구현 기준에서 Research 관련 보강 작업은 다음 순서로 진행한다.

1. `find_research_files()`가 파일명과 상위 폴더명을 함께 검색하도록 유지한다.
2. `.md`, `.txt`, `.csv` 파일만 리서치 품질 검사 대상으로 유지한다.
3. `analyze_research_files()`가 최신 파일 기준으로 필수 섹션, 금지 키워드, stale 여부를 반환하도록 유지한다.
4. 필수 섹션 키워드를 한국어/영어 혼합 문서에 맞게 보강한다.
5. 파일 읽기 실패 시 경고를 signals 또는 warnings에 남긴다.
6. Analyst 결과의 `source_files`, `missing_items`, `forbidden_keyword_hits`를 입력으로 받을 수 있게 pipeline 계약을 확장한다.
7. Research 결과를 Compliance에서 재사용할 수 있도록 향후 pipeline 전달 구조를 개선한다.
8. 최종 보고서에서 후보별 리서치 파일 경로와 보완 필요 항목을 더 잘 보이게 한다.
9. `quality_status`와 `analyst_source_file_mismatch` 필드를 추가한다.

v1에서 하지 않는 구현:

- 리서치 파일을 자동 생성하지 않는다.
- LLM으로 리서치 내용을 자동 요약하지 않는다.
- DART 또는 뉴스 API를 이 Agent에서 직접 호출하지 않는다.
- 투자 의견, 목표가, 매수 등급을 생성하지 않는다.

## 10. 테스트 시나리오

### 10.1 리서치 파일 있음

조건:

- `00_기업별분석/삼성전자/` 아래에 Markdown 파일 존재
- 후보: `005930:삼성전자`

기대 결과:

- `research_files`에 삼성전자 파일 경로가 포함된다.
- `latest_modified`가 비어 있지 않다.
- 파일 품질에 따라 `approve` 또는 `needs_review`가 반환된다.

### 10.2 리서치 파일 없음

조건:

- 후보 종목명 또는 ticker와 매칭되는 파일 없음

기대 결과:

- warnings에 `no research file found` 포함
- `research_files == []`
- status는 `needs_review`

### 10.3 필수 섹션 누락

조건:

- Markdown 파일은 있으나 `사업모델`, `투자 가설`, `리스크`, `반증 조건` 관련 키워드가 없음

기대 결과:

- `missing_required_sections`에 누락 항목 기록
- status는 `needs_review`

### 10.4 오래된 리서치

조건:

- 최신 매칭 파일 수정일이 실행일 기준 120일 초과

기대 결과:

- `is_stale == true`
- warnings에 stale 경고 포함
- status는 `needs_review`

### 10.5 금지/주의 키워드

조건:

- 리서치 파일에 `루머`, `미확인`, `미공개` 등 키워드 포함

기대 결과:

- `forbidden_keyword_hits`에 해당 키워드 기록
- Compliance에서 다시 `needs_review`로 확인 가능

### 10.6 폴더명 매칭

조건:

- 파일명에는 종목명이 없지만 상위 폴더명이 후보 `name`과 일치

기대 결과:

- 파일이 매칭된다.
- Research는 파일명만 보고 실패하지 않는다.

### 10.7 Analyst source file mismatch

조건:

- Analyst 결과의 `source_files`에 존재하지 않는 파일 경로가 포함됨
- 파일 시스템 검색으로는 다른 파일이 매칭됨

기대 결과:

- `analyst_source_file_mismatch == true`
- warnings에 mismatch 경고 포함
- status는 `needs_review`

### 10.8 Analyst complete but file incomplete

조건:

- Analyst는 `analysis_status="complete"`
- ResearchFile 기준 필수 섹션 일부가 누락됨

기대 결과:

- `missing_required_sections`에 누락 항목 기록
- `quality_status == "incomplete"`
- status는 `needs_review`

## 11. 성공 기준

`ResearchFileAgent`는 다음 조건을 만족해야 성공이다.

- 후보별 리서치 파일 존재 여부가 기록된다.
- 파일명뿐 아니라 상위 폴더명으로도 매칭된다.
- 최신 수정일이 기록된다.
- 필수 섹션 누락이 `missing_required_sections`에 기록된다.
- 오래된 리포트가 `is_stale`로 표시된다.
- 금지/주의 키워드가 `forbidden_keyword_hits`에 기록된다.
- 파일 품질 상태가 `quality_status`로 요약된다.
- Analyst가 참조한 파일과 실제 검수 파일이 불일치하면 경고가 남는다.
- 리서치가 부족한 후보가 자동 승인되지 않는다.
- 리서치 파일이 없어도 파이프라인은 중단되지 않는다.
- 최종 보고서에 사람이 보완할 리서치 항목이 남는다.

## 12. 향후 고도화

v2 이후 개선 후보:

- Research 결과를 Compliance에 직접 전달해 중복 파일 검색 제거
- Analyst와 ResearchFile이 공통 파일 인덱스를 사용하도록 개선
- 리서치 문서 템플릿 표준화
- 기업별 최신 리포트만 별도 인덱스로 관리
- `business_model`, `thesis`, `risk`, `disconfirmation` 섹션을 구조화된 YAML/JSON으로 추출
- DART 공시와 기존 리서치 최신성 비교
- 뉴스/공시 기반 신규 이벤트와 기존 투자 가설 충돌 여부 검사
- 로컬 LLM 또는 수동 검토 기반 리서치 품질 점수 추가

v2에서도 유지할 원칙:

- Research는 근거 품질을 확인할 뿐 투자 승인을 하지 않는다.
- ResearchFile은 분석 작성자가 아니라 기록 검수자다.
- 리서치가 없거나 부족하면 반드시 사람 검토가 필요하다.
- 원본 리서치 파일은 자동 수정하지 않는다.
