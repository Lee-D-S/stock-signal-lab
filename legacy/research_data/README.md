# Legacy research data

`ai 주가 변동 원인 분석/`은 기존 리서치·전략·관찰 자동화가 생성한 보관용 산출물이다.

- 기업별 분기 보고서와 `events.jsonl`
- 원천 이벤트·패턴·가설·백테스트 결과
- 전략 신호·관찰 로그·일일 요약

이 데이터는 새 `forecast/experiment`의 직접 입력이 아니다. 새 실험은 고정 유니버스와 KIS/DART 숫자형 Parquet 계약을 사용한다.

기존 데이터를 재사용할 때는 `legacy/research/scripts/analysis_paths.py`를 통해 경로를 확인하고, 필요한 숫자 데이터만 새 active 스키마로 변환한 뒤 별도 검증한다.
