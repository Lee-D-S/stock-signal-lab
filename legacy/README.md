# Legacy

이 디렉터리는 현재 active 예측 연구소에서 사용하지 않는 과거 코드를 보존한다.

## 보존 영역

- `auto_trading/`: 과거 자동매매 엔트리포인트, broker, dashboard, scheduler, strategies, notifier, SQLite 모델
- `text_llm/`: 과거 Agent, guardrail, Gemini/LLM 및 텍스트 분석 코드
- `research/scripts/`: 과거 백테스트, discovery, scoring, screener, 관찰·보고서 생성 스크립트
- `research/diagnostics/`: 과거 실시간 종목·포트폴리오 검증용 일회성 스크립트
- `research_data/`: 과거 리서치 산출물·보고서·이벤트·전략·관찰 데이터
- data/: historical runtime caches and agent outputs (not active experiment inputs)
- `docs/`: 과거 자동매매·Agent·스크리너·캔들 분석 문서
- `plans/`: 과거 자동매매·Agent·위원회·규칙형 연구 계획
- `workflows/`: 비활성화된 과거 GitHub Actions

## 사용 규칙

- active 코드에서 `legacy/`를 import하지 않는다.
- active GitHub Actions는 `legacy/`를 실행하지 않는다.
- legacy 코드를 다시 활용할 때는 필요한 숫자 계산만 새 `forecast/` 모듈로 명시적으로 이식하고 테스트한다.
- 비밀값, token cache, SQLite runtime DB, raw/Parquet 산출물은 이 디렉터리에 추가하지 않는다.

현재 active 시스템은 `forecast/`, `core/api/`, 최소한의 `scripts/`, 그리고 두 개의 forecast GitHub Actions로 제한한다.
