# EXP-000002 — OHLC 가격 형태 피처 v2

## 요약

- 상태: 완료·현재 기준
- 목적: 기존 가격·거래량 피처에 갭, 캔들 형태, rolling 고가·저가 위치 정보를 추가
- 기업 목록: EXP-000001과 동일한 2024-12-31 기준 50개 기업
- 학습 기간: 2015-01-01 ~ 2024-12-30
- 검증 기간: 2020, 2021, 2022, 2023, 2024
- 최종 holdout: 2025년 전체
- training snapshot: 8e1e8100feb99b1a
- model snapshot: 93c392d44b7a31d5
- feature schema: price-volume-v2
- 피처 수: 50
- Git commit: 6002282

## 변경 내용

다음 9개 피처를 기존 41개 피처에 추가했다.

- gap_return
- open_close_return
- candle_body_pct
- upper_wick_pct
- lower_wick_pct
- close_position_20d
- close_position_60d
- distance_to_high_20d
- distance_from_low_20d

새 API 호출 없이 기존 OHLCV 원본에서 로컬 계산했다.

## 검증 결과

| 모델 | 2020 BA | 2021 BA | 2022 BA | 2023 BA | 2024 BA | 최저 BA | 순위 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Random Forest | 0.513560 | 0.520899 | 0.517806 | 0.516697 | 0.518181 | 0.513560 | 1 |
| Logistic | 0.514692 | 0.524036 | 0.518997 | 0.512902 | 0.523663 | 0.512902 | 2 |
| HistGradientBoosting | 0.513119 | 0.521946 | 0.518623 | 0.511845 | 0.514380 | 0.511845 | 3 |
| Baseline | 0.500000 | 0.500000 | 0.500000 | 0.500000 | 0.500000 | 0.500000 | 4 |
## EXP-000001 대비 변화

- Random Forest 최저 연도 BA: 0.512900 → 0.513560
- Random Forest 2025 BA: 0.521464 → 0.524145
- 피처 수: 41 → 50
- 새 API 호출: 없음

## 2025 holdout 결과

| 모델 | balanced accuracy | ROC-AUC | Brier | Log loss |
|---|---:|---:|---:|---:|
| Random Forest | 0.524145 | 0.536784 | 0.250008 | 0.693219 |
| Logistic | 0.519944 | 0.546402 | 0.249350 | 0.691850 |
| Ensemble | 0.518091 | 0.545832 | 0.249249 | 0.691643 |
| HistGradientBoosting | 0.516925 | 0.540142 | 0.249746 | 0.692683 |
| Baseline | 0.500000 | 0.500000 | 0.250623 | 0.694397 |

## 결론

새 가격 형태 피처 묶음은 Random Forest의 검증·2025 holdout 성능을 모두 소폭 개선했다. Random Forest를 기준 모델로 유지하고, 다음 실험은 거래량·가격 결합 피처를 별도 묶음으로 추가해 효과를 확인한다.

## 산출물

- 평가 결과: ../../../../data/forecast_experiment/experiments/EXP-000002/price_volume_model_evaluations.json
- 모델 roster: ../../../../data/forecast_experiment/experiments/EXP-000002/price_volume_model_roster.json
- 2025 실험 보고서: ../../../../data/forecast_experiment/experiments/EXP-000002/price_volume_model_experiment_2025.md
- 2025 요약 CSV: ../../../../data/forecast_experiment/experiments/EXP-000002/price_volume_model_summary_2025.csv
- 2025 예측 Parquet·CSV: 같은 폴더
- 학습 데이터 요약: ../../../../data/forecast_experiment/training_dataset_2015_2024.md