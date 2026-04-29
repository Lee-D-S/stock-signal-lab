 1. 모멘텀 조건

  - RSI 최솟값/최댓값: --rsi-min, --rsi-max
  - Stochastic %K 최솟값/최댓값: --stoch-min, --stoch-max
  - MACD 히스토그램 양수: --macd-positive
  - MACD 히스토그램 음수→양수 전환: --macd-cross-up
  - MACD 히스토그램 증가: 스코어링 내부 조건
  - OBV 상승: --obv-rising, OBV MA5 > MA20

  2. 추세 조건

  - 이동평균 정배열: --ma-align
      - 예: 5,20,60
      - 예: 60,120,240
  - 현재가 > MA20
  - 단기 정배열: MA5 > MA20 > MA60
  - 장기 정배열: MA60 > MA120 > MA240

  3. 거래량/수급 조건

  - 거래량 > 20일 평균 거래량: --vol-above-ma
  - 거래량 급증: 현재 거래량 > 20일 평균 거래량 x 1.5
  - OBV 상승 추세
  - 외국인 연속 순매수/순매도: scripts/foreign_consec_buy.py:1
      - 다만 이건 아직 메인 스코어링/백테스트 조건에는 통합되지 않음

  4. 거래대금 조건

  - 최소 거래대금 필터: --min-amount
  - 거래대금 기준 정렬: --sort amount
  - 유니버스 선정 기준을 거래량순으로 설정: --by volume

  현재는 “거래대금 증가율” 자체는 없음. 거래량 증가 조건은 있고, 거래대금은 필터/정렬로만 씁니다.

  5. 볼린저 밴드 조건

  - 볼린저 밴드 상단 돌파: --bb-breakout
  - 볼린저 밴드 폭 축소: --bb-squeeze
  - 현재가 > MA20, 즉 볼린저 중간선 위

  6. 피보나치 조건

  - 현재가가 최근 60거래일 기준 피보나치 지지선 ±2% 이내: --fib-support

  7. 밸류에이션 조건

  - PER 이하: --per-max
  - PBR 이하: --pbr-max
  - EPS 이상: --eps-min
  - BPS 이상: --bps-min

  8. 재무/실적 조건

  - ROE 이상: --roe-min
  - ROA 이상: --roa-min
  - 영업이익률 이상: --op-margin-min
  - 순이익률 이상: --net-margin-min
  - 부채비율 이하: --debt-max
  - 매출액 이상: --revenue-min
  - 영업이익 이상: --op-income-min
  - 당기순이익 이상: --net-income-min

  9. 업종 조건

  - 업종별 등락률 조회 가능
  - 뉴스 기반 업종 감성 신호 있음
  - 업종 신호 정확도 검증 있음

  다만 “이 종목이 현재 주도 업종에 속하는가”를 자동으로 스코어에 반영하는 조건은 아직 없습니다.

  현재 테스트 가능한 실행 단위

  - 단일 종목 판정: rtk python scripts/judge_ticker.py 삼성전자
  - 조건 스크리닝: rtk python scripts/screener.py --ma-align 5,20,60 --macd-positive --vol-above-ma
  - 스코어링 스크린: rtk python scripts/run_scoring.py --mode screen
  - 임계값 테스트: rtk python scripts/run_scoring.py --mode threshold
  - 백테스트: rtk python scripts/run_backtest.py --help
  - 워크포워드 테스트: rtk python scripts/run_walkforward.py --help
  - 외국인 연속 순매수: rtk python scripts/foreign_consec_buy.py --days 10

  요약하면, 지금 바로 테스트 가능한 건 기술적 지표, 거래량, 거래대금 필터, 밸류에이션, 재무 조건입니다.
  아직 미흡한 건 외국인 순매수 통합, 주도 업종 통합, 거래대금 증가율 조건입니다.
