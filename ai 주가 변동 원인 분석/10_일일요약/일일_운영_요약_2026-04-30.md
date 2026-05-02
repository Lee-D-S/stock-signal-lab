# 일일 운영 요약 - 2026-04-30

## 1. 거래대금 상위 유니버스

- 스캔 종목 수: 30
- 신규/미보유 기업 추정: 0
- 보고서 생성 필요 추정: 0

### 상위 유니버스

| rank | ticker | name | trade_amount | universe_status | report_status |
| --- | --- | --- | --- | --- | --- |
| 1 | 005930 | 삼성전자 | 4984706020346 | existing | exists |
| 2 | 000660 | SK하이닉스 | 4355325578994 | existing | exists |
| 3 | 001440 | 대한전선 | 1887826030175 | existing | exists |
| 4 | 006340 | 대원전선 | 1271887928065 | existing | exists |
| 5 | 042700 | 한미반도체 | 976313892000 | existing | exists |
| 6 | 009150 | 삼성전기 | 764553041500 | existing | exists |
| 7 | 066570 | LG전자 | 669211924750 | existing | exists |
| 8 | 062040 | 산일전기 | 660362028750 | existing | exists |
| 9 | 009830 | 한화솔루션 | 625551970175 | existing | exists |
| 10 | 005380 | 현대차 | 621344074500 | existing | exists |
| 11 | 006400 | 삼성SDI | 601812980000 | existing | exists |
| 12 | 402340 | SK스퀘어 | 530920383000 | existing | exists |
| 13 | 025860 | 남해화학 | 509428622545 | existing | exists |
| 14 | 010170 | 대한광통신 | 509115802595 | existing | exists |
| 15 | 034020 | 두산에너빌리티 | 431277650800 | existing | exists |
| 16 | 047040 | 대우건설 | 415433928850 | existing | exists |
| 17 | 064350 | 현대로템 | 401995538000 | existing | exists |
| 18 | 032830 | 삼성생명 | 393997303500 | existing | exists |
| 19 | 050890 | 쏠리드 | 373166097915 | existing | exists |
| 20 | 199820 | 제일일렉트릭 | 362315213205 | existing | exists |
| 21 | 012450 | 한화에어로스페이스 | 335666994000 | existing | exists |
| 22 | 322000 | HD현대에너지솔루션 | 332657410000 | existing | exists |
| 23 | 010120 | LS ELECTRIC | 324239792250 | existing | exists |
| 24 | 058470 | 리노공업 | 313587899150 | existing | exists |
| 25 | 035420 | NAVER | 296339309000 | existing | exists |
| 26 | 329180 | HD현대중공업 | 280201230500 | existing | exists |
| 27 | 105560 | KB금융 | 265026166256 | existing | exists |
| 28 | 005490 | POSCO홀딩스 | 263012627000 | existing | exists |
| 29 | 024060 | 흥구석유 | 258701218100 | existing | exists |
| 30 | 432720 | 퀄리타스반도체 | 234195354600 | existing | exists |

## 2. 전략 신호

- 전체 스캔 행: 30
- 초기 후보: 1
- 수급 확정 후보: 1
- 수급 불일치/보류: 0
- 오류: 2

### 확정 후보

| priority | hypothesis_id | use_type | ticker | name | signal_date | direction | chg_pct | amount_tag | flow_category_recheck | suggested_response |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1.0 | H02 | 반등 감시 후보 | 005930 | 삼성전자 | 2026-04-30 | down | -2.43% | 거래대금약함 | 외국인기관동반매수 | 하락 이벤트 다음 거래일 반등 확인 후 단기 진입 검토 |

## 3. 관찰 로그

- 누적 관찰 후보: 3
- 기준일 신규 관찰 후보: 1
- 누적 D+ 추적값 입력 수: 12

### 기준일 관찰 후보

| signal_date | ticker | name | hypothesis_id | use_type | event_close | next_close_return_pct | d_plus_5_return_pct | d_plus_10_return_pct | d_plus_20_return_pct | result_label |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-04-30 | 005930 | 삼성전자 | H02 | 반등 감시 후보 | 220500 |  |  |  |  |  |

### 조건별 관찰 성과

| hypothesis_id | use_type | sample_count | completed_count | next_close_avg_return_pct | d_plus_5_avg_return_pct | d_plus_10_avg_return_pct | d_plus_20_avg_return_pct | positive_label_count | negative_label_count | result_status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| H01 | 매수 후보 | 1 | 1 | -1.63% |  |  |  | 0 | 0 | 표본 부족 |
| H02 | 반등 감시 후보 | 2 | 1 | +2.94% |  |  |  | 1 | 0 | 표본 부족 |

## 4. 다음 확인

- D+1/D+5/D+10/D+20 도래 항목이 있으면 `run_observation_tracking_update.py` 결과를 확인한다.
- 신규 조건은 active에 자동 승격하지 않는다.
- 오류 CSV가 있으면 KIS/DART 인증, 휴장일, 상장일, 데이터 누락 여부를 확인한다.
