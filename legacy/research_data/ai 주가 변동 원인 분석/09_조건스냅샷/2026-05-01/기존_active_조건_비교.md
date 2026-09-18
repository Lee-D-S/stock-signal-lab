# 기존 active 조건 vs 2026-05-01 스냅샷 비교

## 결론

- 기존 active 조건: 5개
- 오늘 스냅샷에서 발견된 조건 후보: 9개
- 기존과 동일하게 다시 잡힌 조건: 3개
- active에는 없던 신규 후보 조건: 6개
- 기존 active였지만 이번 스냅샷에서 빠진 조건: 2개

## 기존과 동일

| status | snapshot_hypothesis_id | active_hypothesis_id | market_regime | direction | amount_tag | flow_category | dart_tag | window_category | snapshot_action_hint | active_use_type | note |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 기존과 동일 | H03 | H04 | 유동성/저금리 후반장 | up | 거래대금급증 | 외국인기관동반매도 | DART공시동반 | 직접반응 | 추격매수 회피 후보 | 회피 후보 | 기존 active 조건과 같은 6축 조건 |
| 기존과 동일 | H06 | H01 | 변동성 장세 | up | 거래대금약함 | 외국인기관동반매수 | DART공시동반 | 직접반응 | 진입 후보 | 매수 후보 | 기존 active 조건과 같은 6축 조건 |
| 기존과 동일 | H08 | H02 | 변동성 장세 | down | 거래대금약함 | 외국인기관동반매수 | DART공시동반 | 직접반응 | 반등 관찰 후보 | 반등 감시 후보 | 기존 active 조건과 같은 6축 조건 |

## 신규 후보

| status | snapshot_hypothesis_id | active_hypothesis_id | market_regime | direction | amount_tag | flow_category | dart_tag | window_category | snapshot_action_hint | active_use_type | note |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 신규 후보 | H01 |  | 변동성 장세 | up | 거래대금약함 | 수급엇갈림 | 주변공시부재 | 선반영 | 진입 후보 |  | 기존 active 5개에는 없던 6축 조건 |
| 신규 후보 | H02 |  | 변동성 장세 | down | 거래대금약함 | 외국인기관동반매도 | 주변공시부재 | 직접반응 | 반등 관찰 후보 |  | 기존 active 5개에는 없던 6축 조건 |
| 신규 후보 | H04 |  | 금리 인상/긴축장 | up | 거래대금급증 | 외국인기관동반매도 | DART공시동반 | 직접반응 | 추격매수 회피 후보 |  | 기존 active 5개에는 없던 6축 조건 |
| 신규 후보 | H05 |  | 변동성 장세 | up | 거래대금약함 | 외국인기관동반매수 | 주변공시부재 | 직접반응 | 진입 후보 |  | 기존 active 5개에는 없던 6축 조건 |
| 신규 후보 | H07 |  | AI/전력기기 테마장 | down | 거래대금약함 | 외국인기관동반매수 | 주변공시부재 | 선반영 | 반등 관찰 후보 |  | 기존 active 5개에는 없던 6축 조건 |
| 신규 후보 | H09 |  | AI/전력기기 테마장 | up | 거래대금급증 | 외국인기관동반매도 | DART공시동반 | 직접반응 | 추격매수 회피 후보 |  | 기존 active 5개에는 없던 6축 조건 |

## 이번 스냅샷에서 탈락한 기존 조건

| status | snapshot_hypothesis_id | active_hypothesis_id | market_regime | direction | amount_tag | flow_category | dart_tag | window_category | snapshot_action_hint | active_use_type | note |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 이번 스냅샷에서 탈락 |  | H03 | AI/전력기기 테마장 | down | 거래대금평균상회 | 수급엇갈림 | 주변공시부재 | 누적배경 |  | 반등 감시 후보 | 오늘 신규 events 포함 재분석에서는 통과 기준 미달 또는 우선 후보 제외 |
| 이번 스냅샷에서 탈락 |  | H06 | 반도체 반등장 | up | 거래대금급증 | 외국인기관동반매도 | DART공시동반 | 직접반응 |  | 회피 후보 | 오늘 신규 events 포함 재분석에서는 통과 기준 미달 또는 우선 후보 제외 |

## 해석

- 오늘 새로 active에 추가한 조건은 없다.
- 신규 후보 6개는 검증 대기 상태이며, 기존 조건명 H01~H06에 바로 섞으면 안 된다.
- 기존과 동일한 3개는 성능 수치만 업데이트 후보로 보고, 조건 ID는 기존 active ID를 유지하는 편이 맞다.
- 탈락한 2개는 삭제가 아니라 보류/관찰 유지 대상으로 두고 표본 누적 후 판단한다.
