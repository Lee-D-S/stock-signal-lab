# EXP-000004 시장·시장대비 상대수익률 피처 v4

## 요약

- 상태: 완료·보류
- 목적: 고정 50개 기업을 KOSPI/KOSDAQ 시장별로 묶어 시장 평균수익률과 종목의 시장 대비 상대수익률을 추가
- 기업 목록: EXP-000001과 동일한 2024-12-31 기준 50개 기업
- 학습 기간: 2015-01-01 ~ 2024-12-30
- 검증 기간: 2020, 2021, 2022, 2023, 2024
- 최종 holdout: 2025년 전체
- training snapshot: a2bbd2f1abd02775
- model snapshot: 79b2952e4b9b8654
- feature schema: price-volume-v4
- 피처 수: 58
- 실행 결과 폴더: data/forecast_experiment/experiments/EXP-000004
- Git commit: v4 실험 구현 커밋 반영 예정

## 변경 내용

v2의 50개 피처에 다음 8개 피처를 추가했다.

- market_return_1d
- market_return_5d
- market_return_20d
- market_return_60d
- market_relative_return_1d
- market_relative_return_5d
- market_relative_return_20d
- market_relative_return_60d

시장수익률은 동일 날짜·시장 구분(KOSPI 또는 KOSDAQ)의 고정 유니버스 종목 수익률을 동일 가중 평균한 값이다. 업종 분류 데이터는 현재 원본에 없어 이번 실험에는 포함하지 않았다. 모든 피처는 해당 feature_asof 날짜의 종가까지로 계산했다.

## 검증 결과

| 모델 | 2020 BA | 2021 BA | 2022 BA | 2023 BA | 2024 BA | 최악 BA | 순위 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Logistic | 0.510906 | 0.517636 | 0.518513 | 0.512798 | 0.520403 | 0.510906 | 1 |
| Random Forest | 0.514115 | 0.523206 | 0.518657 | 0.510333 | 0.513895 | 0.510333 | 2 |
| HistGradientBoosting | 0.505755 | 0.530959 | 0.523350 | 0.514176 | 0.508555 | 0.505755 | 3 |
| Baseline | 0.500000 | 0.500000 | 0.500000 | 0.500000 | 0.500000 | 0.500000 | 4 |

## 2025 holdout 결과

| 모델 | balanced accuracy | ROC-AUC | Brier | Log loss |
|---|---:|---:|---:|---:|
| Random Forest | 0.519979 | 0.527188 | 0.250864 | 0.694962 |
| Logistic | 0.518754 | 0.549119 | 0.249241 | 0.691635 |
| Ensemble | 0.514013 | 0.532368 | 0.250042 | 0.693253 |
| HistGradientBoosting | 0.511101 | 0.516668 | 0.254881 | 0.703606 |
| Baseline | 0.500000 | 0.500000 | 0.250623 | 0.694397 |

## EXP-000002와 비교

- 최악 연도 BA: 0.513560 → 0.510906
- Random Forest 2025 BA: 0.524145 → 0.519979
- Logistic 2025 BA: 0.519944 → 0.518754
- HistGradientBoosting 2025 BA: 0.516925 → 0.511101

## 결론

시장별 평균수익률과 시장 대비 상대수익률을 추가했지만 EXP-000002의 활성 v2 기준선을 개선하지 못했다. 따라서 v4 피처는 활성 학습 데이터에 채택하지 않고 보류한다. 이후 실험은 v2 피처셋에서 시작한다.

## 산출물

- 평가 결과: data/forecast_experiment/experiments/EXP-000004/price_volume_model_evaluations.json
- 모델 roster: data/forecast_experiment/experiments/EXP-000004/price_volume_model_roster.json
- 2025 실험 보고서: data/forecast_experiment/experiments/EXP-000004/price_volume_model_experiment_2025.md
- 2025 요약 CSV: data/forecast_experiment/experiments/EXP-000004/price_volume_model_summary_2025.csv
- 2025 예측 Parquet·CSV: 같은 폴더
- 모델 bundle: data/forecast_experiment/experiments/EXP-000004/models_price_volume_2025