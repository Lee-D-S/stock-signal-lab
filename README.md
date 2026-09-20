# 숫자 기반 주가예측 실험실

> 고정된 국내 기업 목록을 대상으로 시점 누수를 방지하며 주가 상승·하락 방향을 예측하고, 실험 결과를 재현 가능하게 관리하는 연구 환경입니다.

## 프로젝트 요약

이 브랜치는 현재 운영 중인 주가예측 실험 환경입니다. 핵심 질문은 다음과 같습니다.

> 특정 거래일 이전에 알 수 있었던 정보만 사용했을 때, 각 기업의 다음 거래일 상승·하락 방향을 예측할 수 있는가?

이 프로젝트는 리서치와 주문 실행을 분리합니다. 읽기 전용 시장·기업 데이터를 수집하고, 시점 기준 피처를 생성하며, walk-forward 검증으로 여러 후보 모델을 평가합니다. 선정된 모델은 매일 예측하지만 주문은 실행하지 않습니다.

| 구분 | 현재 설계 |
| --- | --- |
| 기업 목록 | 2024-12-31 기준으로 고정한 국내 상장사 50개 |
| 과거 입력 데이터 | KIS OHLCV 데이터, 필요한 경우 구조화된 OpenDART 데이터 |
| 예측 대상 | 다음 거래일 상승·하락 방향 |
| 홀드아웃 | 모델 선택과 분리한 2025년 전체 라벨 |
| 일일 운영 | 허용된 기준시점까지의 데이터로 예측 후 다음 거래일에 채점 |
| 현재 기본 모델 | Price-volume v2 피처 세트 + Random Forest |
| 실행 정책 | 읽기 전용 리서치이며 active forecast 코드에서는 주문 API를 호출하지 않음 |

## 이 실험의 핵심

단순히 분류 모델을 학습하는 것보다 중요한 것은 평가 결과가 실제 의미를 갖도록 정보 사용 시점을 통제하는 것입니다.

- 2025년 평가 기간이 시작되기 전에 기업 목록을 고정했습니다.
- 예측일의 피처는 해당 날짜까지 이용 가능한 정보만 사용합니다.
- 이동·롤링 피처에는 명시적인 warm-up 구간을 적용하며 미래 행을 사용하지 않습니다.
- 라벨은 피처와 별도로 생성한 후 기업과 거래일 기준으로 결합합니다.
- 무작위 train/test 분할 대신 expanding walk-forward 검증을 사용합니다.
- 실험마다 고유 ID, 요약 로그, 상세 기록, 피처 snapshot을 저장합니다.
- 일일 실행에서는 먼저 예측을 기록하고, 다음 거래일의 실제 라벨이 확보된 뒤 성능을 계산합니다.

## 전체 흐름

```text
KIS / OpenDART (읽기 전용)
        |
        v
고정된 50개 기업 목록
        |
        v
원천 OHLCV -> 시점 기준 피처 -> 다음 거래일 라벨
        |
        v
Walk-forward 검증 + 2025년 홀드아웃 평가
        |
        v
선정 모델 목록 + 앙상블
        |
        v
일일 예측 -> 다음 거래일 성능표
```

활성 패키지는 의도적으로 숫자 데이터 중심으로 구성했습니다. 과거 자동매매, 뉴스, Telegram, LLM 및 광범위한 리서치 산출물은 `legacy/`에 보존되어 있으며 active forecast 모델의 입력으로 사용하지 않습니다.

## 현재 실험 결과

검증 연도별로 balanced accuracy를 기록하며, 여러 연도의 평균 하나로 변동성을 숨기지 않고 최악의 검증 연도를 우선해 모델을 선정합니다.

| 실험 | 변경 내용 | 2025 Random Forest balanced accuracy | 결정 |
| --- | --- | ---: | --- |
| EXP-000001 | 초기 price-volume baseline, 41개 피처 | 0.521464 | 대체됨 |
| EXP-000002 | OHLC 가격 형태 피처, 50개 피처 | 0.524145 | 현재 기본 모델 |
| EXP-000003 | 거래량-가격 상호작용 피처, 58개 피처 | 0.521943 | 보류 |
| EXP-000004 | 시장 및 시장 대비 수익률 피처, 58개 피처 | 0.519979 | 보류 |

위 수치는 실험 비교를 위한 결과이며 투자 가능 수익률을 보장하지 않습니다. 새로운 피처나 모델이 out-of-sample 성능을 실제로 개선하는지 반복 가능하게 검증하는 것이 목적입니다.

## 저장소 구조

| 경로 | 역할 |
| --- | --- |
| `forecast/` | 활성 숫자 기반 주가예측 패키지와 CLI |
| `forecast/experiment/` | 고정 기업 목록, 데이터셋, 피처, 모델, 실험, 일일 실행 코드 |
| `forecast/experiment/EXPERIMENT_LOG.md` | 실험 하나당 한 행으로 관리하는 인덱스 |
| `forecast/experiment/experiments/` | 실험 ID별 상세 기록과 산출물 |
| `forecast/labels/` | 라벨 생성 워크플로와 데이터 계약 |
| `core/api/` | 활성 데이터 수집기가 사용하는 읽기 전용 KIS 클라이언트 |
| `legacy/` | 과거 자동매매·리서치·LLM·원천 산출물 보관 영역 |
| `data/forecast/` | 생성된 예측 산출물. 로컬·실행 데이터이며 소스 코드는 아님 |

주요 문서는 다음과 같습니다.

- [Forecast 패키지 안내](forecast/README.md)
- [고정 기업 실험 안내](forecast/experiment/README.md)
- [실험 목록](forecast/experiment/EXPERIMENT_LOG.md)
- [일일 예측 및 기준시점 계약](forecast/experiment/DAILY_PRICE_VOLUME.md)
- [현재 기본 모델 실험 기록](forecast/experiment/experiments/EXP-000002/README.md)

## 실행 방법

저장소 루트에서 실행합니다. 이 작업 환경에서는 `rtk` 명령 래퍼를 사용하며, 별도 환경에서는 `python`으로 바꿔 실행할 수 있습니다.

```powershell
rtk python -m unittest discover -s forecast/tests -p "test_*.py"
rtk python -m unittest discover -s forecast/experiment/tests -p "test_*.py"
rtk python -m forecast.cli --help
rtk python -m forecast.cli daily --dry-run
```

필요한 로컬 인증 정보와 데이터 설정이 있는 경우 읽기 전용 수집 및 주간 평가 진입점은 다음과 같습니다.

```powershell
rtk python -m forecast.online_auto --artifact-root forecast_artifacts
rtk python -m forecast.weekly --input <labelled-parquet-path>
```

이 브랜치에서는 주문 API를 활성화하지 않습니다. 생성되는 Parquet, CSV, JSON, Excel 산출물은 Git이 아닌 설정된 로컬 산출물 경로에 보관해야 합니다.

## 한계와 다음 연구 과제

- 고정 기업 목록은 실험 통제를 위한 것이며 과거 지수 편입 종목을 재현하지 않으므로 survivorship bias가 있을 수 있습니다.
- 첫 평가 기간은 2025년입니다. 향후 평가 기간을 추가할 때도 새 기간을 미리 튜닝에 사용하지 않도록 홀드아웃 원칙을 유지해야 합니다.
- 현재 피처군은 가격·거래량 중심입니다. 섹터, 재무, 거시경제 등 새로운 데이터는 명시적인 as-of-date 계약과 함께 추가해야 합니다.
- 이진 방향 예측에서 balanced accuracy만으로 경제적 가치를 설명할 수 없습니다. 향후 calibration, turnover, 거래비용 가정, 포트폴리오 단위 분석이 필요합니다.
- 특정 홀드아웃 기간에서 우수한 모델이 곧바로 실전 모델이나 투자 추천을 의미하지는 않습니다.

## 포트폴리오 관점의 의미

이 프로젝트는 고정 기업 목록 정의, 재현 가능한 데이터 수집, 시점 기준 통제, 피처·라벨 생성, walk-forward 모델 비교, 실패 실험 보존, 일일 예측과 지연 채점까지의 전체 실험 사이클을 구현합니다. 핵심은 데이터 누수를 방지하고 모든 결과를 추적·검증할 수 있게 만드는 것입니다.
