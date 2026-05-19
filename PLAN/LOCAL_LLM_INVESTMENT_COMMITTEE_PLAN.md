# 로컬 LLM 투자위원회 운영 PLAN

작성일: 2026-05-19

## 1. 목적

이 문서는 `C:\projects\auto-invest`의 Python 기반 투자 Agent 시스템 위에 로컬 LLM Agent 운영 계층을 독립 구현해, auto-invest를 1인 투자기업처럼 운영하기 위한 계획이다.

핵심 방향은 다른 공개 프로젝트의 코드를 가져와 붙이는 것이 아니다. Connect AI는 로컬 LLM으로 역할별 Agent를 운영할 수 있다는 참고 사례로만 둔다. `auto-invest`는 데이터 수집, 후보 발굴, 정량 계산, 리스크/준법 검증, 주문 제안 JSON 생성을 담당하는 투자 백오피스 엔진으로 유지한다. 새로 구현할 로컬 LLM Agent 계층은 각 결과를 읽고 해석, 반론, 질문, 회의록, 사람용 브리핑을 생성하는 투자위원회 운영 계층을 담당한다.

이 계층의 작업명은 **Local LLM Investment Committee Layer**로 둔다.

라이선스/저작권 원칙:

- Connect AI의 코드, 에셋, UI, 문구, 파일 구조를 복사하지 않는다.
- 로컬 LLM 호출, 역할별 프롬프트, 회의형 Agent 운영이라는 아이디어만 참고한다.
- auto-invest 내부에 필요한 최소 기능을 Python 코드로 새로 구현한다.
- 나중에 UI가 필요하면 auto-invest 자체 대시보드나 별도 확장으로 구현한다.

목표 구조:

```text
auto-invest
  -> 데이터 수집, 후보 발굴, 정량 계산
  -> 리스크/준법 hard gate
  -> AgentResult / OrderProposal JSON 생성

Local LLM Investment Committee Layer
  -> data/agent_runs 결과 수집
  -> 로컬 LLM이 Agent별 해석, 반론, 질문, 회의록, 브리핑 생성
  -> Python guardrail 결과를 사람이 이해할 수 있게 설명

사람
  -> 최종 승인, live 전환, 실제 주문 책임
```

## 2. 현재 상태

`auto-invest`에는 이미 코드 기반 투자 Agent 파이프라인이 있다.

| 구성요소 | 역할 |
|---|---|
| `core/agents/free_pipeline.py` | 투자 Agent 실행 순서와 결과 산출 |
| `core/agents/free_base.py` | `AgentContext`, `AgentResult`, `Candidate`, `OrderProposal` 공통 구조 |
| `scripts/run_free_agent_pipeline.py` | 수동 후보/발견 후보 기반 파이프라인 실행 |
| `scripts/run_agent_committee.py` | 기본 discovery를 포함한 투자위원회 실행 |
| `data/agent_runs/` | Agent별 JSON, Markdown 보고서 산출 위치 |

현재 Agent는 `QuantSignalAgent`, `EquityResearchAnalystAgent`, `ResearchFileAgent`, `PortfolioManagerAgent`, `RiskManagerAgent`, `ComplianceOfficerAgent`, `TraderAgent`, `OperationsReportAgent` 중심으로 구성되어 있다.

## 3. 운영 원칙

- `RiskManagerAgent`, `ComplianceOfficerAgent`, `TraderAgent`의 코드 기반 차단 규칙은 유지한다.
- LLM은 설명, 요약, 반론, 리서치 보강, 회의록 생성에만 사용한다.
- LLM이 직접 매수/매도 실행 여부를 최종 결정하지 않는다.
- 실제 주문 실행은 기본 범위에서 제외한다.
- 사람 승인 전까지 `execution_allowed`는 보수적으로 다룬다.
- 기본 운영은 `KIS_IS_MOCK=true`를 권장한다.
- LLM 출력은 투자 판단의 최종 근거가 아니라 검토 보조 자료로 표시한다.
- Python guardrail은 LLM 의견보다 항상 우선한다.
- LLM은 `execution_allowed=false`를 `true`로 바꿀 수 없다.
- LLM은 Python 결과보다 더 보수적인 상태로 낮추거나, 사람이 확인할 질문을 추가할 수만 있다.

최종 상태 결정 규칙:

```text
effective_status = Python guardrail 결과 우선

if RiskManagerAgent 또는 ComplianceOfficerAgent가 block:
  effective_status = block
elif Python 결과가 block 또는 needs_review:
  effective_status = Python 결과
elif LLM 의견이 block 또는 needs_review:
  effective_status = needs_review
else:
  effective_status = Python 결과 유지

LLM 의견은 execution_allowed를 직접 true로 변경할 수 없음
LLM 의견은 승인 상태를 더 보수적으로 낮출 수만 있음
```

## 3.1 무료 로컬 LLM 운영 원칙

기존 Agent를 코드 기반으로 만든 이유는 API 비용 없이 무료로 운영하기 위해서였다. 이 목표는 유지한다. 기본 설계는 Ollama 또는 LM Studio에서 실행되는 로컬 LLM을 사용해 토큰 과금 없이 LLM-backed Agent를 만드는 것이다.

무료로 처리할 수 있는 영역:

- 기존 `AgentResult` JSON/Markdown 요약
- 투자회의 발언 생성
- 리스크 반론과 체크리스트 작성
- 리서치 파일의 누락/충돌/오래된 근거 설명
- 사람에게 보낼 일일 브리핑과 텔레그램 요약

로컬 LLM만으로 약한 영역:

- 최신 뉴스 검색
- 최신 공시/실적 외부 검색
- 실시간 이슈 확인
- 웹 기반 출처 검증

따라서 기본값은 로컬 LLM 무료 운영으로 둔다. Gemini 같은 외부 API는 최신 외부 정보 검색이 꼭 필요한 경우에만 선택적으로 사용하며, API 키가 없으면 해당 보강 단계만 건너뛰고 나머지 Agent 운영은 계속한다.

## 3.2 노트북 하드웨어 운영 기준

초기 운영 기준은 16GB 시스템 RAM과 Intel Arc 내장/공유 GPU 메모리 환경을 기준으로 둔다. 이 환경에서도 로컬 LLM은 가능하지만, GPU 가속을 전제로 설계하지 않는다. 기본 설계는 CPU 또는 제한적인 GPU 가속에서도 실패하지 않는 작은 모델 중심 운영이다.

현재 확인된 기준 장비:

| 항목 | 기준 |
|---|---|
| 시스템 RAM | 16GB |
| GPU | Intel(R) Arc(TM) Graphics |
| GPU 메모리 | 공유 GPU 메모리 약 8.9GB |
| DirectX | DirectX 12, Feature Level 12.2 |

모델 선택 원칙:

- 1차 기본 모델은 3B급 또는 4B급 quantized 모델로 둔다.
- 한국어 설명과 요약 품질을 고려해 Qwen 계열 3B 모델을 우선 후보로 둔다.
- 7B/8B Q4 모델은 선택적으로 지원하되, 속도 저하와 메모리 압박을 감안한다.
- 14B 이상 모델은 16GB RAM 노트북의 기본 운영 범위에서 제외한다.
- 긴 리서치 원문 전체를 한 번에 넣지 않고, Python Agent 결과와 필요한 요약 컨텍스트만 입력한다.

실행 엔진 원칙:

- 1차 구현은 Ollama HTTP API를 기본 대상으로 둔다.
- Intel Arc GPU 가속은 필수 요구사항이 아니라 후속 최적화로 둔다.
- Ollama가 Intel GPU를 사용하지 못하고 CPU로 fallback되어도 파이프라인은 정상 동작해야 한다.
- LM Studio, llama.cpp Vulkan, IPEX-LLM은 성능 개선이 필요할 때 후속으로 검토한다.
- 로컬 LLM 서버가 꺼져 있거나 모델이 없으면 `skipped` 상태로 기록하고 Python Agent 결과만 저장한다.

권장 초기 모델 후보:

```text
qwen2.5:3b
gemma3:4b
```

이 모델 기준은 성능 최적화보다 안정적인 무료 운영을 우선한다. 더 큰 모델을 쓰는 경우에도 Final Gate와 Python guardrail 우선 원칙은 바뀌지 않는다.

## 4. 투자기업 LLM Agent 매핑

auto-invest 내부에 투자기업 전용 LLM Agent 정의를 추가한다. 이 정의는 Connect AI의 `src/agents.ts`를 가져오지 않고, auto-invest 도메인에 맞는 Python 설정/프롬프트로 새로 작성한다.

| LLM Agent | auto-invest 대응 | 책임 |
|---|---|---|
| CEO | InvestmentCommittee 역할 | 전체 투자회의 요약, 충돌 의견 정리 |
| Quant | `QuantSignalAgent` | 가격/거래대금/신호 후보 설명 |
| Analyst | `EquityResearchAnalystAgent`, `ResearchFileAgent` | 기업 리서치, 반증 조건, 누락 자료 정리 |
| Portfolio Manager | `PortfolioManagerAgent` | 포트폴리오 비중과 현금 상태 해석 |
| Risk Manager | `RiskManagerAgent` | 리스크 차단 사유 설명 |
| Compliance | `ComplianceOfficerAgent` | 규칙 위반, 근거 부족, 필수 기록 확인 |
| Trader | `TraderAgent` | 주문 제안 JSON을 사람이 이해할 수 있게 설명 |
| Secretary | `OperationsReportAgent` | 일일 브리핑, 텔레그램/대시보드 요약 |

참고 아이디어:

- 에이전트 정의: 이름, 역할, 페르소나, 책임을 명시한다.
- 회의 흐름: CEO가 Python 산출물을 읽고 역할별 LLM 의견을 요청한 뒤 종합한다.
- 로컬 LLM 호출: Ollama `/api/chat` 또는 LM Studio `/v1/chat/completions`를 지원한다.
- 에이전트별 모델 라우팅: 기본 모델을 쓰되, 필요하면 역할별 모델 override를 허용한다.

## 5. 1차 구현 범위

1차 목표는 auto-invest 내부에서 실행 가능한 독립 LLM 투자위원회 계층을 만드는 것이다.

### 5.1 기존 투자위원회 실행

기존 Python 투자위원회는 그대로 실행한다.

```powershell
rtk python scripts/run_agent_committee.py --discover-limit 10 --test-portfolio balanced
```

일일 신호 파이프라인은 별도 명령으로 연결한다.

```powershell
rtk python -u scripts/run_signal_research_pipeline.py --mode daily --recheck
```

### 5.2 최신 결과 읽기

새 LLM 투자위원회 스크립트는 `data/agent_runs/YYYY-MM-DD/<run_id>/` 아래 최신 실행 결과를 읽는다.

주요 입력 파일:

```text
quant_signal.json
equity_research_analyst.json
research_file.json
portfolio_manager.json
risk_manager.json
compliance_officer.json
trader_order_proposal.json
operations_report.json
final_committee_report.md
pipeline_manifest.json
```

`pipeline_manifest.json`이 있으면 우선 사용하고, 없으면 최신 수정 시각 기준으로 run 디렉터리를 찾는다.

### 5.3 LLM 요약 계층

로컬 LLM은 다음 출력만 담당한다.

- 오늘의 후보 종목 요약
- Agent별 approve/block/needs_review 사유 설명
- Risk/Compliance 차단 사유를 사람이 읽기 쉬운 언어로 변환
- Trader 주문 제안의 전제와 보류 조건 요약
- 다음 사람이 확인해야 할 체크리스트 생성

Gemini 같은 외부 API는 기본 경로가 아니다. 최신 뉴스, 공시, 외부 출처 확인이 꼭 필요한 보강 단계에서만 선택적으로 사용한다.

### 5.4 LLM 산출물 저장

로컬 LLM 계층이 생성한 산출물은 auto-invest의 원본 Python 산출물과 분리해서 저장한다.

예상 산출물:

```text
data/agent_runs/YYYY-MM-DD/<run_id>/
  quant_signal.json
  equity_research_analyst.json
  research_file.json
  portfolio_manager.json
  risk_manager.json
  compliance_officer.json
  trader_order_proposal.json
  operations_report.json
  final_committee_report.md
  pipeline_manifest.json

  local_llm_review.json
  investment_committee_minutes.md
  human_approval_brief.md
```

`local_llm_review.json` 예시:

```json
{
  "engine": "local_llm",
  "model": "qwen-or-gemma-local",
  "run_id": "...",
  "agents": {
    "quant": {
      "summary": "...",
      "objections": ["..."],
      "human_questions": ["..."]
    },
    "risk": {
      "summary": "...",
      "red_flags": ["..."]
    },
    "compliance": {
      "summary": "...",
      "cannot_override": true
    }
  },
  "final_gate": {
    "python_execution_allowed": false,
    "llm_recommendation": "needs_review",
    "effective_status": "needs_review"
  }
}
```

### 5.5 신규 모듈 구조

auto-invest 내부에 다음 모듈을 새로 추가한다.

```text
core/llm/
  __init__.py
  local_client.py          # Ollama / LM Studio 호출
  investment_agents.py    # Quant/Analyst/PM/Risk/Compliance/Trader/Secretary 프롬프트
  committee.py            # AgentResult JSON -> LLM 회의 실행
  schemas.py              # LLMReview, AgentReview, FinalGate 등 구조

scripts/
  run_llm_investment_committee.py
```

현재 구현 상태:

- `core/llm/schemas.py`, `core/llm/committee.py`, `scripts/run_llm_investment_committee.py` skeleton은 구현됐다.
- `core/llm/local_client.py` Ollama HTTP 클라이언트와 CLI `--model`, `--base-url`, `--timeout-sec`, `--no-llm` 옵션은 구현됐다.
- 최신 `data/agent_runs/YYYY-MM-DD/<run_id>` 탐색과 `pipeline_manifest.json` fallback 없는 run 탐색을 지원한다.
- 로컬 LLM이 설정되지 않았거나 서버 호출에 실패하면 `skipped` 리뷰를 생성한다.
- 로컬 LLM이 응답하면 Secretary 요약 1회를 생성한다.
- 로컬 LLM이 응답하면 `RiskManagerAgent`, `ComplianceOfficerAgent`, `TraderAgent` 결과에 대한 역할별 LLM 리뷰를 생성한다.
- 생성 산출물은 `local_llm_review.json`, `investment_committee_minutes.md`, `human_approval_brief.md`이다.
- 역할별 Quant/Analyst LLM 프롬프트는 다음 구현 단계로 남아 있다.

기본 실행 흐름:

```text
1. 기존 Python Agent 실행 또는 최신 run 선택
2. data/agent_runs 최신 run 읽기
3. 로컬 LLM 사용 가능 여부 확인
4. Agent별 LLM 의견 생성
5. Final Gate 적용
6. local_llm_review.json / investment_committee_minutes.md / human_approval_brief.md 저장
```

## 6. 후속 구현 단계

### Phase 1: auto-invest 내부 LLM 설정

- `.env` 또는 설정 파일에 로컬 LLM 설정을 추가한다.
- 기본값은 Ollama `http://127.0.0.1:11434`로 둔다.
- LM Studio는 `http://127.0.0.1:1234/v1`을 지원한다.
- 모델명이 비어 있으면 LLM 리뷰를 `skipped`로 기록하고 Python 결과만 표시한다.

예상 설정:

```text
LOCAL_LLM_ENABLED=true
LOCAL_LLM_BACKEND=ollama
LOCAL_LLM_BASE_URL=http://127.0.0.1:11434
LOCAL_LLM_MODEL=
LOCAL_LLM_TIMEOUT_SEC=300
```

### Phase 2: 로컬 LLM 클라이언트

- `core/llm/local_client.py`를 구현한다.
- Ollama `/api/chat`와 LM Studio `/v1/chat/completions`를 지원한다.
- 호출 실패, 모델 없음, 서버 꺼짐은 예외로 종료하지 않고 `skipped` 상태로 반환한다.
- JSON 모드가 가능한 백엔드는 JSON 출력을 우선 요청하되, 실패하면 텍스트 파싱 fallback을 둔다.

현재는 Ollama `/api/chat`만 구현되어 있다. LM Studio와 JSON 모드 파싱은 후속 단계로 남긴다.

### Phase 3: 회의형 요약

- 각 Python Agent JSON 결과를 LLM Agent 발언으로 변환한다.
- CEO가 최종 회의록을 생성한다.
- Secretary가 텔레그램 또는 일일 브리핑 형식으로 짧게 요약한다.
- 모든 프롬프트는 투자 조언이 아니라 검토 보조, 리스크 설명, 사람 확인사항 생성에 초점을 둔다.

### Phase 4: 선택적 LLM 고도화

- 로컬 LLM은 기본 엔진으로 사용하며, 내부 산출물 요약과 반론 생성을 토큰 과금 없이 처리한다.
- Gemini 등 외부 모델은 최신 뉴스/외부 요인 검색이 필요한 리서치 보강에만 선택적으로 사용한다.
- 외부 API 키가 없거나 호출에 실패해도 로컬 LLM 기반 Agent 회의는 계속 진행한다.
- 모든 LLM 출력은 투자 판단의 근거가 아니라 검토 보조 자료로 표시한다.

### Phase 5: Python Agent를 LLM-backed Agent로 확장

기존 Python Agent는 데이터 수집, 계산, 검증, 차단 규칙을 담당하고, 각 역할별 LLM Agent는 해당 결과를 해석해 의견을 낸다. 구현은 auto-invest 내부에서 새로 작성한다.

기본 실행 모델은 무료 로컬 LLM이다. 각 LLM-backed Agent는 auto-invest의 `core/llm/local_client.py`를 통해 호출한다. 모델이 없거나 로컬 서버가 꺼져 있으면 Python Agent 결과와 기존 Markdown 보고서만 표시하고, LLM 의견 생성은 `skipped` 상태로 기록한다.

목표 구조:

```text
Python Agent
  -> 숫자 계산, 파일 검사, 리스크/준법 hard block
  -> AgentResult JSON 생성

LLM-backed Agent
  -> AgentResult + 리서치 문서 + 포트폴리오 상태 입력
  -> 역할별 투자 의견, 반론, 질문, 사람 확인사항 생성

Final Gate
  -> Python guardrail 결과를 최종 우선
  -> LLM 의견은 execution_allowed를 직접 true로 바꾸지 못함
  -> LLM 의견과 Python 결과를 분리 저장
```

역할별 확장:

| Python Agent | LLM-backed Agent | LLM의 책임 |
|---|---|---|
| `QuantSignalAgent` | Quant LLM Agent | 신호 품질, 과최적화 위험, 추가 확인 지표 설명 |
| `EquityResearchAnalystAgent` | Analyst LLM Agent | 투자 thesis, catalyst, risk, disconfirmation 작성 |
| `ResearchFileAgent` | Evidence LLM Agent | 리서치 누락, 오래된 근거, 근거 충돌 해석 |
| `PortfolioManagerAgent` | PM LLM Agent | 비중 조정 의견, 현금 사용 우선순위, 집중도 코멘트 |
| `RiskManagerAgent` | Risk LLM Agent | 손실 시나리오, 변동성, 리스크 완화 질문 생성 |
| `ComplianceOfficerAgent` | Compliance LLM Agent | 준법/기록 누락을 사람이 이해할 문장으로 설명 |
| `TraderAgent` | Trader LLM Agent | 주문 방식, 보류 조건, 체결 전 체크리스트 작성 |
| `OperationsReportAgent` | Secretary LLM Agent | 투자회의록, 일일 브리핑, 텔레그램 요약 작성 |

### Phase 6: UI와 운영 화면

초기 구현은 CLI와 Markdown/JSON 산출물 중심으로 완료한다. UI는 별도 후속 과제로 둔다.

- 1차 UI는 만들지 않는다.
- CLI 산출물과 Markdown 보고서만으로 검증한다.
- 이후 필요하면 auto-invest 자체 대시보드 또는 별도 VS Code 확장을 만든다.
- 결과 로더는 `pipeline_manifest.json`을 우선 읽고, 없으면 최신 `data/agent_runs/YYYY-MM-DD/<run_id>`를 탐색한다.
- UI를 만들 경우 Agent별 Python 결과와 LLM 의견을 분리해서 표시한다.
- LLM 호출은 auto-invest 내부 로컬 LLM 클라이언트를 사용한다.
- Gemini 같은 외부 모델은 리서치 보강용 선택 옵션으로 남긴다.
- UI에는 현재 사용 중인 엔진을 명확히 표시한다: `Local LLM`, `External API`, `No LLM`.
- `No LLM` 상태에서도 Python Agent 실행과 결과 조회는 가능해야 한다.
- Risk/Compliance/Trader의 hard gate 결과는 UI에서 별도 강조하고 LLM 요약보다 위에 표시한다.
- Trader 제안은 “주문 실행”이 아니라 “사람 검토용 제안”으로만 표시한다.

로컬 LLM 계층에서 생성할 출력은 `auto-invest` 원본 산출물과 분리한다. 초기에는 `data/agent_runs/YYYY-MM-DD/<run_id>/local_llm_review.json` 또는 같은 내용의 Markdown 요약으로 저장한다.

## 7. 제외 범위

- 기존 `auto-invest` Agent의 계산, 검증, 리스크/준법 hard guardrail을 LLM으로 대체하지 않는다.
- 각 Agent의 해석/토론/질문/보고서 작성 기능은 LLM-backed Agent로 확장할 수 있다.
- LLM이 직접 주문 API를 호출하지 않는다.
- 실거래 자동 주문을 이번 연동 범위에 넣지 않는다.
- `auto-invest`의 리스크/준법 규칙을 LLM 판단이나 UI 판단으로 우회하지 않는다.
- Connect AI의 코드, 에셋, UI, 문구를 복사하지 않는다.

## 8. 검증 기준

- auto-invest에서 LLM 투자위원회 실행 명령을 호출할 수 있다.
- `data/agent_runs` 최신 결과를 찾아 Agent별 상태를 표시할 수 있다.
- Ollama/LM Studio가 켜져 있으면 로컬 LLM으로 Agent별 의견을 생성할 수 있다.
- 로컬 LLM이 꺼져 있어도 Python Agent 결과 표시는 실패하지 않는다.
- Gemini API 키가 없어도 외부 리서치 보강만 건너뛰고 무료 로컬 흐름은 유지된다.
- Risk/Compliance가 `block` 또는 `needs_review`일 때 UI와 요약에 명확히 드러난다.
- Trader 제안이 있어도 사람 승인 전에는 실행 가능하다고 표현하지 않는다.
- 명령 실패, `.env` 누락, KIS mock/live 상태를 사용자에게 명확히 보여준다.
- `local_llm_review.json`, `investment_committee_minutes.md`, `human_approval_brief.md`가 원본 Python 산출물과 분리 저장된다.
- LLM이 생성한 추천이나 의견이 Python guardrail의 `block`, `needs_review`, `execution_allowed=false`를 우회하지 못한다.
- Connect AI 프로젝트 없이도 auto-invest 단독으로 실행된다.

후속 UI 검증 기준:

- UI에서 `Python result`, `LLM review`, `Final gate`가 구분되어 보인다.
- Risk/Compliance/Trader의 hard gate 결과가 LLM 요약보다 위에 표시된다.
- Trader 제안이 “주문 실행”이 아니라 “사람 검토용 제안”으로 표시된다.

## 9. 운영 원칙

최종 형태는 다음 역할 분담을 유지한다.

```text
auto-invest = 투자 데이터, 규칙, 파이프라인, 산출물, 안전장치
local LLM   = Agent별 해석, 반론, 회의록, 브리핑
사람        = 최종 승인, 실거래 책임, 전략 변경 결정
```

따라서 auto-invest 자체가 투자기업의 백오피스 엔진과 투자위원회 회의록 생성기를 모두 가진다. Connect AI는 구현 참고 사례로만 남긴다.
