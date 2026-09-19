# EXP-000003 — 가격·거래량 결합 피처 v3

## 요약

- 상태: 완료·보류
- 목적: EXP-000002의 50개 피처에 거래량과 가격 움직임의 결합 정보를 추가
- 기업 목록: EXP-000001과 동일한 2024-12-31 기준 50개 기업
- 학습 기간: 2015-01-01 ~ 2024-12-30
- 검증 기간: 2020, 2021, 2022, 2023, 2024
- 최종 holdout: 2025년 전체
- training snapshot: e028cad0a217c88c
- model snapshot: 2820e389ccb175b9
- feature schema: price-volume-v3
- 피처 수: 58
- 실행 결과 폴더: data/forecast_experiment/experiments/EXP-000003
- Git commit: 85395116ef6d0e06d6f04a6a03e7f9d9413fc679

## 변경 내용

다음 8개 피처를 EXP-000002의 50개 피처에 추가했다.

- volume_zscore_20d
- volume_zscore_60d
- turnover_zscore_20d
- turnover_zscore_60d
- up_volume_share_20d
- down_volume_share_20d
- return_volume_interaction_1d
- return_volume_interaction_5d

새 API 호출 없이 기존 OHLCV 원본에서 로컬 계산했다. 새 피처의 학습 데이터 결측은 0건이었다.

## 검증 결과

| 모델 | 2020 BA | 2021 BA | 2022 BA | 2023 BA | 2024 BA | 최저 BA | 순위 |
|---|---:|---:|---:|---:|---:|---:|---:|
| HistGradientBoosting | 0.515277 | 0.519319 | 0.519233 | 0.513326 | 0.515932 | 0.513326 | 1 |
| Random Forest | 0.511108 | 0.519487 | 0.514137 | 0.514614 | 0.515935 | 0.511108 | 2 |
| Logistic | 0.510869 | 0.521492 | 0.520096 | 0.511560 | 0.521246 | 0.510869 | 3 |
| Baseline | 0.500000 | 0.500000 | 0.500000 | 0.500000 | 0.500000 | 0.500000 | 4 |

## 2025 holdout 결과

| 모델 | balanced accuracy | ROC-AUC | Brier | Log loss |
|---|---:|---:|---:|---:|
| Random Forest | 0.521943 | 0.534777 | 0.250169 | 0.693546 |
| HistGradientBoosting | 0.520870 | 0.540367 | 0.249449 | 0.692061 |
| Logistic | 0.520158 | 0.544730 | 0.249465 | 0.692082 |
| Ensemble | 0.512396 | 0.545128 | 0.249309 | 0.691762 |
| Baseline | 0.500000 | 0.500000 | 0.250623 | 0.694397 |

## EXP-000002 대비 변화

- 피처 수: 50 → 58
- EXP-000002의 최저 BA: 0.513560
- EXP-000003의 최고 최저 BA: 0.513326
- Random Forest 최저 BA: 0.513560 → 0.511108
- Random Forest 2025 BA: 0.524145 → 0.521943
- HistGradientBoosting 2025 BA: 0.516925 → 0.520870

## 결론

가격·거래량 결합 피처는 HistGradientBoosting의 성능을 개선했지만, 전체 모델 선정 정책의 기준인 최저 연도 balanced accuracy에서는 EXP-000002의 Random Forest보다 낮았다. 따라서 v3 피처를 현재 기본 피처셋으로 채택하지 않고 EXP-000002의 v2 피처셋을 유지한다.

향후 시장·업종 상대수익률 실험은 v2 피처셋을 기준으로 시작한다.

## 산출물

- 평가 결과: data/forecast_experiment/experiments/EXP-000003/price_volume_model_evaluations.json
- 모델 roster: data/forecast_experiment/experiments/EXP-000003/price_volume_model_roster.json
- 2025 실험 보고서: data/forecast_experiment/experiments/EXP-000003/price_volume_model_experiment_2025.md
- 2025 요약 CSV: data/forecast_experiment/experiments/EXP-000003/price_volume_model_summary_2025.csv
- 2025 예측 Parquet·CSV: 같은 폴더
- 모델 bundle: data/forecast_experiment/experiments/EXP-000003/models_price_volume_2025