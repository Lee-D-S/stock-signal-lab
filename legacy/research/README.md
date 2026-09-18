# Legacy research stack

이 디렉터리는 새 `forecast/experiment`에서 사용하지 않는 과거 리서치·전략·관찰 자동화 코드를 보존한다.

- `scripts/`: 과거 유니버스, 기업별 보고서, 이벤트, 백테스트, 전략 신호, 관찰·요약 스크립트
- `scripts/analysis_paths.py`: 보관된 리서치 데이터의 공통 경로 모듈
- `scripts/sync_analysis_paths.py`: 보관된 문서 경로 검증 도구
- 데이터 루트: `legacy/research_data/ai 주가 변동 원인 분석/`
- Legacy runtime cache root: legacy/data/

재사용이 필요하면 먼저 숫자형 데이터 계산만 선별해 `forecast/`의 데이터 계약과 테스트로 이식한다. 이 디렉터리의 코드나 텍스트·LLM 분석 결과를 active 모델 입력으로 직접 연결하지 않는다.

경로 점검:

```powershell
rtk python legacy/research/scripts/sync_analysis_paths.py --check
```
