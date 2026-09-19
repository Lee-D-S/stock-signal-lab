# EXP-000001 — 41개 가격·거래량 baseline

## 요약

- 상태: 완료
- 목적: 고정 기업 50개에 대해 가격·거래량만으로 다음 거래일 상승·하락을 예측하는 기준선 확립
- 기업 목록: 2024-12-31 기준 50개 기업
- 학습 기간: 2015-01-01 ~ 2024-12-30
- 검증 기간: 2020, 2021, 2022, 2023, 2024
- 최종 holdout: 2025년 전체
- feature schema: price-volume-v1
- 피처 수: 41
- Git 기준: 3ba4eaa
- 모델 snapshot: 27d86a58a74cc089

## 실험 구성

OHLCV, 수익률, 이동변동성, 이동평균 괴리, 거래량·거래대금 비율, 당일 range, 당일 close position을 사용했다.

후보 모델은 Majority baseline, Logistic, Random Forest, HistGradientBoosting이다. 모델 선정은 검증연도별 점수를 평균내지 않고 최저 연도 balanced accuracy, baseline 초과 연수, 최저 연도 ROC-AUC, 최악 연도 Brier, 최악 연도 log loss 순으로 비교했다.

## 검증 결과

| 모델 | 2020 BA | 2021 BA | 2022 BA | 2023 BA | 2024 BA | 최저 BA | 순위 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Random Forest | 0.512900 | 0.513432 | 0.514096 | 0.513125 | 0.516812 | 0.512900 | 1 |
| HistGradientBoosting | 0.517202 | 0.523719 | 0.513855 | 0.510532 | 0.513365 | 0.510532 | 2 |
| Logistic | 0.514076 | 0.525323 | 0.519527 | 0.510041 | 0.523957 | 0.510041 | 3 |
| Baseline | 0.500000 | 0.500000 | 0.500000 | 0.500000 | 0.500000 | 0.500000 | 4 |

## 2025 holdout 결과

| 모델 | balanced accuracy |
|---|---:|
| Random Forest | 0.521464 |
| Logistic | 0.519786 |
| HistGradientBoosting | 0.518914 |
| Ensemble | 0.515807 |
| Baseline | 0.500000 |

## 결론

Random Forest를 기준 모델로 채택했다. 이후 피처·모델 변경 실험에서 Random Forest를 비교 기준으로 사용한다. 단일 모델만 고정하지 않고 검증을 통과한 후보군도 함께 보존한다.

## 산출물

- 실행 로그: ../../../../data/forecast_experiment/experiments/EXP-000001/price_volume_model_run.log
- 원본 실행 로그: ../../../../data/forecast_experiment/price_volume_model_run.log
- 참고: v1의 상세 JSON·예측 파일은 이후 v2 실행으로 공통 output 경로에서 덮어써졌으므로, 보존된 실행 로그와 이 문서의 수치를 기준으로 기록한다.
- v1 코드 기준: 3ba4eaa