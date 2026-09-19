# 주가예측 실험 기록

고정 기업 주가 방향성 실험의 공식 실험 장부다. 실험 결과를 다시 찾거나 동일한 설정을 반복하는 일을 막기 위해 완료·실패·보류 실험을 모두 기록한다.

## 운영 규칙

- 실험마다 고유한 experiment_id를 부여한다.
- 결과를 비교할 때는 기업 목록, 데이터 snapshot, feature schema, 모델 설정, 검증 기간, 평가 정책을 함께 확인한다.
- 2025년 결과는 2020~2024년 검증으로 설정을 확정한 뒤 확인하는 최종 holdout으로 기록한다.
- 평균 점수만 기록하지 않고 검증연도별 점수와 최저 연도 성능을 함께 기록한다.
- 성능이 개선되지 않은 실험도 삭제하지 않고 결론과 다음 조치를 기록한다.
- Git commit과 결과 artifact 경로를 남겨 코드·데이터·결과를 재현할 수 있게 한다.

## 중복 확인 키

새 실험을 등록하기 전에 다음 항목이 기존 실험과 모두 같은지 확인한다.

- universe_id
- training_data_snapshot
- feature_schema_version와 feature_columns
- model configuration
- validation years
- selection policy

모두 같으면 새 실험을 만들지 않고 기존 experiment_id를 재사용한다. 일부라도 다르면 새 실험 ID를 발급한다.

## 실험 목록

| ID | 상태 | 핵심 변경 | 피처 수 | 검증 기준 최저 BA | 2025 RF BA | 결론 |
|---|---|---|---:|---:|---:|---|
| EXP-0001 | 완료 | 가격·거래량 baseline | 41 | 0.512900 | 0.521464 | Random Forest를 기준 모델로 채택 |
| EXP-0002 | 완료·현재 기준 | OHLC 가격 형태 피처 9개 추가 | 50 | 0.513560 | 0.524145 | baseline 대비 개선; Random Forest 유지 |

---

## EXP-0001 — 41개 가격·거래량 baseline

- 상태: 완료
- 목적: 고정 기업 50개에 대해 가격·거래량만으로 다음 거래일 상승·하락을 예측하는 기준선 확립
- 기업 목록: 2024-12-31 기준 50개 기업
- 학습 기간: 2015-01-01 ~ 2024-12-30
- 검증 기간: 2020, 2021, 2022, 2023, 2024
- 최종 holdout: 2025년 전체
- feature schema: price-volume-v1
- 피처 수: 41
- 포함 데이터: OHLCV, 수익률, 이동변동성, 이동평균 괴리, 거래량·거래대금 비율, 당일 range, 당일 close position
- 후보 모델: Majority baseline, Logistic, Random Forest, HistGradientBoosting
- 모델 선정 정책: 연도별 검증값을 평균내지 않고 최저 연도 balanced accuracy를 우선 비교
- Git 기준: 3ba4eaa
- 참고 실행 로그: data/forecast_experiment/price_volume_model_run.log

### 검증 결과

| 모델 | 최저 연도 BA | baseline 초과 연수 | 순위 |
|---|---:|---:|---:|
| Random Forest | 0.512900 | 5 | 1 |
| HistGradientBoosting | 0.510532 | 5 | 2 |
| Logistic | 0.510041 | 5 | 3 |
| Baseline | 0.500000 | 0 | 4 |

### 2025 holdout 결과

| 모델 | balanced accuracy |
|---|---:|
| Random Forest | 0.521464 |
| Logistic | 0.519786 |
| HistGradientBoosting | 0.518914 |
| Ensemble | 0.515807 |
| Baseline | 0.500000 |

### 결론

Random Forest를 당시 기준 모델로 채택했다. 단일 모델만 고정하지 않고 이후 피처·모델 변경 실험에서 비교 기준으로 사용한다.

---

## EXP-0002 — OHLC 가격 형태 피처 v2

- 상태: 완료·현재 기준
- 목적: 기존 가격·거래량 피처에 갭과 캔들 형태, rolling 고가·저가 위치 정보를 추가
- 기업 목록: EXP-0001과 동일
- 학습 기간: 2015-01-01 ~ 2024-12-30
- 검증 기간: 2020, 2021, 2022, 2023, 2024
- 최종 holdout: 2025년 전체
- training snapshot: 8e1e8100feb99b1a
- model snapshot: 93c392d44b7a31d5
- feature schema: price-volume-v2
- 피처 수: 50
- 추가 피처: gap_return, open_close_return, candle_body_pct, upper_wick_pct, lower_wick_pct, close_position_20d, close_position_60d, distance_to_high_20d, distance_from_low_20d
- 후보 모델: Majority baseline, Logistic, Random Forest, HistGradientBoosting
- 모델 선정 정책: 연도별 검증값을 평균내지 않고 최저 연도 balanced accuracy, baseline 초과 연수, 최저 연도 ROC-AUC, 최악 연도 Brier, 최악 연도 log loss 순으로 비교
- Git commit: 6002282
- 결과: data/forecast_experiment/price_volume_model_evaluations.json, data/forecast_experiment/price_volume_model_experiment_2025.md, data/forecast_experiment/price_volume_model_roster.json

### 검증 결과

| 모델 | 최저 연도 BA | baseline 초과 연수 | 순위 |
|---|---:|---:|---:|
| Random Forest | 0.513560 | 5 | 1 |
| Logistic | 0.512902 | 5 | 2 |
| HistGradientBoosting | 0.511845 | 5 | 3 |
| Baseline | 0.500000 | 0 | 4 |

### EXP-0001 대비 변화

- Random Forest 최저 연도 BA: 0.512900 → 0.513560
- Random Forest 2025 BA: 0.521464 → 0.524145
- 피처 수: 41 → 50
- 새 API 호출: 없음. 기존 OHLCV 원본에서 로컬 계산

### 2025 holdout 결과

| 모델 | balanced accuracy | ROC-AUC | Brier | Log loss |
|---|---:|---:|---:|---:|
| Random Forest | 0.524145 | 0.536784 | 0.250008 | 0.693219 |
| Logistic | 0.519944 | 0.546402 | 0.249350 | 0.691850 |
| Ensemble | 0.518091 | 0.545832 | 0.249249 | 0.691643 |
| HistGradientBoosting | 0.516925 | 0.540142 | 0.249746 | 0.692683 |
| Baseline | 0.500000 | 0.500000 | 0.250623 | 0.694397 |

### 결론

새 가격 형태 피처 묶음은 Random Forest의 검증·2025 holdout 성능을 모두 소폭 개선했다. Random Forest를 기준 모델로 유지하고, 다음 실험은 거래량·가격 결합 피처를 별도 묶음으로 추가해 효과를 확인한다.

---

## 새 실험 기록 템플릿

다음 블록을 복사해 새 실험을 등록한다.

## EXP-XXXX — 실험 제목

- 상태: 계획 / 실행 중 / 완료 / 실패 / 보류
- 목적:
- 가설:
- 기업 목록·universe_id:
- 학습 기간:
- 검증 기간:
- 최종 holdout:
- training snapshot:
- feature schema 및 피처 수:
- 변경한 피처:
- 후보 모델 및 하이퍼파라미터:
- 모델 선정 정책:
- Git commit:
- 결과 artifact:

### 검증 결과

| 모델 | 연도별 점수 | 최저 연도 BA | baseline 초과 연수 | 순위 |
|---|---|---:|---:|---:|
| 모델명 | 2020: / 2021: / 2022: / 2023: / 2024: |  |  |  |

### holdout 결과

| 모델 | balanced accuracy | ROC-AUC | Brier | Log loss |
|---|---:|---:|---:|---:|
| 모델명 |  |  |  |  |

### 결론

- 무엇이 개선되었는가:
- 무엇이 개선되지 않았는가:
- 다음 실험: