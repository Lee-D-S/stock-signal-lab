# 투자 Agent 실행 흐름 예시 매뉴얼

작성일: 2026-05-11

## 1. 목적

이 문서는 `FreeAgentPipeline`을 실행했을 때 각 Agent가 어떤 입력을 받고, 무엇을 판단하고, 어떤 결과물을 다음 Agent에 넘기는지 예시로 설명한다.

핵심은 다음 한 문장이다.

```text
Pipeline은 매수 종목을 자동으로 확정하는 도구가 아니라, 투자위원회 검토 과정을 빠짐없이 기록하는 도구다.
```

따라서 최종 사용자는 “Trader가 buy를 만들었는가”만 보면 안 된다. `Risk`, `Compliance`, `Research`, `Operations`의 경고와 보류 사유까지 함께 읽어야 한다.

## 2. 예시 실행 명령

포트폴리오 스냅샷이 있는 상태에서 삼성전자를 100,000원 검토 금액으로 넣는 예시다.

```powershell
rtk python scripts/run_free_agent_pipeline.py --candidate 005930:삼성전자:100000 --portfolio-json data\portfolio_snapshot.json --research-root "legacy\research_data\ai 주가 변동 원인 분석\00_기업별분석"
```

날짜를 생략하면 오늘 날짜가 투자 판단 기준일로 들어간다.

기준일을 명시하고 싶으면 `--as-of-date`를 쓴다.

```powershell
rtk python scripts/run_free_agent_pipeline.py --as-of-date 2026-05-11 --candidate 005930:삼성전자:100000 --portfolio-json data\portfolio_snapshot.json --research-root "legacy\research_data\ai 주가 변동 원인 분석\00_기업별분석"
```

주의할 점:

- `--as-of-date`는 투자 판단 기준일이다.
- 미래 매매일이나 미래 예측일이 아니다.
- 오늘보다 미래 날짜는 기본 실행에서 차단된다.
- 같은 기준일에 여러 번 실행하면 `data/agent_runs/YYYY-MM-DD/HHMMSS_microseconds/` 아래에 실행별 폴더가 따로 생긴다.

## 3. 전체 흐름 요약

실행 순서는 고정되어 있다.

```text
QuantSignalAgent
→ StrategyDecisionAgent
→ EquityResearchAnalystAgent
→ ResearchFileAgent
→ PortfolioManagerAgent
→ RiskManagerAgent
→ ComplianceOfficerAgent
→ TraderAgent
→ OperationsReportAgent
```

각 Agent는 앞 Agent의 결과를 받아 자기 분야만 판단한다.  
어떤 Agent도 전체 결론을 혼자 확정하지 않는다.

## 4. 단계별 예시

### 4.1 QuantSignalAgent

입력:

```text
005930:삼성전자:100000
```

하는 일:

- 후보 종목코드와 종목명을 표준화한다.
- 후보 출처가 수동 입력인지 CSV 신호인지 기록한다.
- 현재가, 거래대금, 신호 상세가 있는지 확인한다.

생성 파일:

```text
quant_signal.json
```

예시 판단:

```text
삼성전자 후보 1개를 정리했다.
다만 수동 후보라 current_price, trade_amount, signal_details가 부족하다.
```

다음 Agent로 넘기는 것:

- 후보 종목코드
- 종목명
- 검토 금액
- 후보 출처
- 신호 상세와 데이터 누락 경고

사용자가 읽을 포인트:

```text
이 후보가 왜 들어왔는가?
가격/거래대금/신호 근거가 충분한가?
```

수동 후보만 넣었다면 데이터가 부족하다는 경고가 나오는 것이 정상이다.

### 4.2 StrategyDecisionAgent

입력:

- Quant가 정리한 후보
- 후보의 `signal_details`
- 전략 조건 CSV와 관찰 성과 요약

하는 일:

- 후보가 어떤 `hypothesis_id` 또는 조건에서 왔는지 찾는다.
- 조건의 `action_hint`를 표준 전략 방향으로 바꾼다.
- 기대수익, hit rate, 진입 규칙, 계획 보유기간을 기록한다.
- 회피 또는 청산 감시 후보를 신규 매수 후보와 분리한다.

생성 파일:

```text
strategy_decision.json
```

예시 판단:

```text
H01은 buy_candidate, next_open, 20거래일 보유 계획이다.
H04/H06은 추격매수 회피 또는 청산 감시 후보로 신규 매수 초안에 쓰면 안 된다.
```

다음 Agent로 넘기는 것:

- `decision_type`
- `entry_rule`
- `planned_hold_days`
- `expected_return_pct`
- `hit_rate`
- `exit_rule`

사용자가 읽을 포인트:

```text
이 후보는 매수 후보인가, 반등 감시인가, 회피인가, 청산 감시인가?
기대수익과 hit rate는 과거 조건 성과일 뿐 수익 보장이 아닌가?
```

### 4.3 EquityResearchAnalystAgent

입력:

- Quant가 정리한 후보
- `research-root` 아래의 기업별 리서치 파일

하는 일:

- 후보 기업에 맞는 리서치 파일을 찾는다.
- 사업모델, 실적, 밸류에이션, 촉매, 리스크, 반증 조건이 있는지 본다.
- 리서치가 충분하면 `complete`, 부족하면 `partial`로 표시한다.

생성 파일:

```text
equity_research_analyst.json
```

예시 판단:

```text
삼성전자 리서치 파일은 일부 근거가 있지만 사업모델, 리스크, 반증 조건이 부족하다.
분석 상태는 partial이다.
```

다음 Agent로 넘기는 것:

- 분석 상태
- 신뢰도
- 누락 항목
- 핵심 리스크
- 사용한 리서치 파일 목록

사용자가 읽을 포인트:

```text
기업 분석이 complete인가?
누락 항목이 투자 판단에 치명적인가?
리스크와 반증 조건이 명확한가?
```

여기서 `partial`이면 신규 매수 판단은 보수적으로 가는 것이 맞다.

### 4.4 ResearchFileAgent

입력:

- 후보
- Analyst가 사용한 리서치 파일 정보
- 실제 리서치 파일 목록

하는 일:

- 리서치 파일이 존재하는지 확인한다.
- 필수 섹션이 있는지 확인한다.
- Analyst가 본 파일과 ResearchFile이 찾은 파일이 일치하는지 확인한다.
- 오래된 파일인지 확인한다.

생성 파일:

```text
research_file.json
```

예시 판단:

```text
리서치 파일은 있지만 투자 가설, 리스크, 반증 조건 같은 필수 섹션이 부족하다.
ResearchFile 상태는 needs_review다.
```

다음 Agent로 넘기는 것:

- 리서치 파일 품질 상태
- 누락 섹션
- 오래된 파일 여부
- Analyst와 ResearchFile 간 불일치 여부

사용자가 읽을 포인트:

```text
이 투자 아이디어를 뒷받침하는 파일이 실제로 있는가?
리서치 파일이 최신이고 구조적으로 충분한가?
```

### 4.5 PortfolioManagerAgent

입력:

- 후보
- StrategyDecision 결과
- 포트폴리오 JSON
- Analyst 결과
- ResearchFile 결과

하는 일:

- 현재 현금이 충분한지 확인한다.
- 이미 보유한 종목인지 확인한다.
- 전략 방향이 `buy_candidate`인지 확인한다.
- 리서치와 분석이 충분한지 확인한다.
- 신규 매수, 보유, 검토 필요 중 하나로 액션 초안을 만든다.

생성 파일:

```text
portfolio_manager.json
```

예시 판단:

```text
검토 금액은 100,000원인데 주문 가능 현금이 80,203원이다.
또 리서치 상태도 complete가 아니다.
따라서 신규 매수 초안이 아니라 hold로 둔다.
```

다음 Agent로 넘기는 것:

- 후보별 `side`
- 제안 금액
- 포트폴리오 관점 사유
- 현금 부족, 리서치 부족 같은 제약

사용자가 읽을 포인트:

```text
내 계좌 상황에서 이 후보를 지금 살 수 있는가?
현금, 기존 보유, 리서치 품질을 고려해도 신규 매수가 맞는가?
```

### 4.6 RiskManagerAgent

입력:

- 후보
- 포트폴리오
- PortfolioManager 결과
- Analyst/Research 결과
- 리스크 한도 설정

하는 일:

- 현금 한도를 확인한다.
- 종목 비중과 섹터 비중을 확인한다.
- 거래대금 대비 주문 금액이 과도한지 확인한다.
- 데이터가 부족하면 승인하지 않고 `needs_review`로 남긴다.

생성 파일:

```text
risk_manager.json
```

예시 판단:

```text
현금이 부족하다.
수동 후보라 거래대금 정보가 없어 유동성 검사를 할 수 없다.
섹터 정보도 부족하다.
Risk 상태는 needs_review다.
```

다음 Agent로 넘기는 것:

- 후보별 리스크 상태
- 리스크 경고
- projected position/sector 정보
- 유동성 판단 결과

사용자가 읽을 포인트:

```text
이 주문이 계좌 전체에서 너무 큰가?
특정 섹터에 과도하게 몰리는가?
유동성이 충분한가?
```

Risk가 `approve`가 아니면 Trader는 매수 주문 초안을 만들면 안 된다.

### 4.7 ComplianceOfficerAgent

입력:

- 후보
- Analyst 결과
- ResearchFile 결과
- 리서치 파일 품질
- 후보 출처

하는 일:

- 종목코드와 출처가 추적 가능한지 확인한다.
- 리서치 근거가 충분한지 확인한다.
- Analyst와 ResearchFile 결과가 충돌하는지 확인한다.
- 루머, 미공개, 확인 불가 같은 위험 키워드가 있는지 확인한다.

생성 파일:

```text
compliance_officer.json
```

예시 판단:

```text
리서치 파일에 필수 섹션이 부족하다.
Analyst는 일부 누락 항목을 발견했고 ResearchFile도 누락을 발견했다.
기록이 완전하지 않으므로 Compliance 상태는 needs_review다.
```

다음 Agent로 넘기는 것:

- 후보별 준법 상태
- 기록 상태
- 리서치 품질 상태
- 준법 경고

사용자가 읽을 포인트:

```text
이 판단의 출처와 기록이 남아 있는가?
루머나 미공개 정보에 기대고 있지 않은가?
리서치 증거가 서로 충돌하지 않는가?
```

Compliance가 `approve`가 아니면 Trader는 매수 주문 초안을 만들면 안 된다.

### 4.8 TraderAgent

입력:

- PortfolioManager 결과
- RiskManager 결과
- ComplianceOfficer 결과
- 후보 현재가

하는 일:

- 실제 주문을 실행하지 않는다.
- Risk와 Compliance가 모두 `approve`인지 확인한다.
- 둘 중 하나라도 `approve`가 아니면 `hold`로 만든다.
- `hold`이면 금액과 수량을 0으로 만든다.

생성 파일:

```text
trader_order_proposal.json
```

예시 판단:

```text
Risk 상태가 needs_review다.
Compliance 상태도 needs_review다.
따라서 최종 주문 초안은 hold다.
금액 0, 수량 0, broker_api_called=false다.
```

다음 Agent로 넘기는 것:

- 최종 주문 초안
- `side`
- 금액과 수량
- Risk/Compliance 상태
- 최종 hold 사유

사용자가 읽을 포인트:

```text
side가 buy인가 hold인가?
hold라면 왜 hold인가?
broker_api_called=false인가?
```

중요:

```text
TraderAgent의 결과는 실제 주문이 아니다.
사람이 별도로 검토한 뒤 수동 주문 여부를 결정해야 한다.
```

### 4.9 OperationsReportAgent

입력:

- 모든 AgentResult

하는 일:

- 전체 결과를 사람이 읽을 수 있는 보고서로 묶는다.
- 누락 Agent가 있는지 확인한다.
- 최종 상태, 경고, 사람이 확인할 항목을 정리한다.
- 텔레그램 요약용 텍스트도 생성한다.

생성 파일:

```text
final_committee_report.md
operations_report.json
telegram_summary.txt
pipeline_manifest.json
```

예시 판단:

```text
전체 상태는 needs_review다.
삼성전자는 hold다.
현금, 리서치, 유동성, 준법 기록 측면에서 추가 확인이 필요하다.
실제 주문은 실행되지 않았다.
```

사용자가 읽을 포인트:

```text
가장 먼저 final_committee_report.md를 읽는다.
그 다음 trader_order_proposal.json에서 side와 hold 사유를 확인한다.
마지막으로 risk_manager.json과 compliance_officer.json에서 구체 경고를 확인한다.
```

## 5. 사용자는 최종적으로 무엇을 보면 되는가

가장 먼저 볼 파일:

```text
data/agent_runs/YYYY-MM-DD/HHMMSS_microseconds/final_committee_report.md
```

그 다음 볼 파일:

```text
trader_order_proposal.json
risk_manager.json
compliance_officer.json
pipeline_manifest.json
```

판단 순서:

1. `final_committee_report.md`에서 전체 상태를 본다.
2. 후보 표에서 `방향`이 `buy`인지 `hold`인지 본다.
3. `hold`면 주요 사유와 경고를 읽는다.
4. `RiskManagerAgent` 경고가 있으면 주문하지 않는다.
5. `ComplianceOfficerAgent` 경고가 있으면 주문하지 않는다.
6. `broker_api_called=false`인지 확인한다.
7. 모든 경고를 사람이 해소한 뒤에만 별도 수동 주문을 검토한다.

## 6. 예시 결론 해석

예를 들어 보고서가 이렇게 나오면:

```text
후보: 삼성전자
방향: hold
Risk: needs_review
Compliance: needs_review
주요 사유: 현금 부족, 리서치 필수 섹션 부족, 유동성 데이터 부족
```

사용자의 해석은 다음과 같아야 한다.

```text
지금 이 시스템은 삼성전자를 사라고 말한 것이 아니다.
오히려 현재 데이터로는 매수 판단이 부족하다고 기록한 것이다.
따라서 사용자는 주문하지 말고, 부족한 데이터와 리서치를 보강해야 한다.
```

반대로 어떤 후보가 `buy` 초안으로 나와도 이것은 자동 주문 지시가 아니다.

```text
buy는 “Risk와 Compliance 관점에서 주문 초안 생성이 가능하다”는 뜻이지,
사람의 최종 투자 판단을 대체하지 않는다.
```

## 7. 자주 생기는 오해

### `--as-of-date`는 미래 예측일인가?

아니다. 투자 판단 기준일이다.

`--as-of-date 2026-05-11`은 “2026-05-11 기준으로 보고서를 기록한다”는 뜻이다.  
그 날짜의 미래 시세나 미래 공시를 가져와 분석한다는 뜻이 아니다.

### `needs_review`는 실패인가?

아니다. 사람이 검토해야 한다는 뜻이다.

투자 시스템에서 데이터가 부족한데도 `approve`가 나오는 것이 더 위험하다.

### `TraderAgent`가 있으면 자동 주문하는가?

아니다. 이 Pipeline은 실제 주문 API를 호출하지 않는다.

항상 확인해야 하는 값:

```text
broker_api_called=false
```

### 후보가 1개만 나오는 이유는?

명령에서 `--candidate`를 한 번만 넣었기 때문이다.

여러 후보를 넣으려면 `--candidate`를 여러 번 쓰거나, `--discover`로 전략 신호 CSV에서 후보를 찾게 해야 한다.

## 8. 운영 권장 명령

agent 회의/토론 전체 실행:

```powershell
rtk python scripts/run_agent_committee.py --discover-limit 10 --test-portfolio balanced
```

이 명령은 후보를 직접 넣지 않으면 최근 전략 신호 CSV에서 후보를 자동 발견한다. 내부 단계는 기존 `run_free_agent_pipeline.py`와 같고, 개별 옵션을 세밀하게 조정해야 할 때만 아래 명령을 직접 사용한다.

수동 테스트:

```powershell
rtk python scripts/run_free_agent_pipeline.py --candidate 005930:삼성전자:100000 --portfolio-json data\portfolio_snapshot.json --research-root "legacy\research_data\ai 주가 변동 원인 분석\00_기업별분석"
```

전략 신호 기반 실행:

```powershell
rtk python scripts/run_free_agent_pipeline.py --discover --discover-limit 10 --portfolio-json data\portfolio_snapshot.json --research-root "legacy\research_data\ai 주가 변동 원인 분석\00_기업별분석"
```

기준일 명시 실행:

```powershell
rtk python scripts/run_free_agent_pipeline.py --as-of-date 2026-05-11 --discover --discover-limit 10 --portfolio-json data\portfolio_snapshot.json --research-root "legacy\research_data\ai 주가 변동 원인 분석\00_기업별분석"
```

실운영에서는 보통 `--as-of-date`를 생략해 오늘 날짜를 쓰는 편이 안전하다.
