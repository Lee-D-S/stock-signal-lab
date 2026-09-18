# Legacy runtime data

이 디렉터리는 새 `forecast/experiment`에서 사용하지 않는 과거 자동매매·리서치·Agent 시스템의 실행 산출물과 캐시를 보존한다.

- `agent_runs/`: 과거 Agent/LLM 실행 결과
- `dart_fundamental_cache/`, `investor_flow_cache/`, `ohlcv_cache/`: 과거 리서치용 데이터 캐시
- `portfolios/`, `manual_portfolio_input.json`: 과거 포트폴리오 입력·검증 산출물
- `reports/`: 과거 전략·종목 발굴 리포트
- `dart_corp_codes.json`, `dart_corp_info.json`, `gemini_external_cache.json`: 과거 보조 캐시

활성 주가예측 실험실은 `data/forecast/`만 사용한다. 이 디렉터리의 파일은 active 모델 입력이나 운영 데이터로 직접 연결하지 않는다. 재사용할 때는 필요한 숫자형 데이터만 검토하고, `forecast/`의 데이터 계약과 테스트를 거쳐 별도로 이식한다.
