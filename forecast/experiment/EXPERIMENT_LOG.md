# 주가예측 실험 목록

| ID | 상태 | 실험명 | 피처 수 | 검증 기준 최저 BA | 2025 RF BA | 결론 | 상세 |
|---|---|---|---:|---:|---:|---|---|
| EXP-000001 | 완료 | 가격·거래량 baseline | 41 | 0.512900 | 0.521464 | Random Forest를 기준 모델로 채택 | [상세](experiments/EXP-000001/README.md) |
| EXP-000002 | 완료·현재 기준 | OHLC 가격 형태 피처 v2 | 50 | 0.513560 | 0.524145 | baseline 대비 개선; Random Forest 유지 | [상세](experiments/EXP-000002/README.md) |
| EXP-000003 | 완료·보류 | 가격·거래량 결합 피처 v3 | 58 | 0.513326 | 0.521943 | EXP-000002보다 낮아 v2 유지 | [상세](experiments/EXP-000003/README.md) |