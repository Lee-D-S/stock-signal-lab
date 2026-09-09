# 분석 한계와 위험

## 확인이 필요한 기술 위험

### 1. 실거래 경로

`core/broker.py`는 `KIS_IS_MOCK` 값에 따라 mock/live 주문 TR ID와 서버를 선택한다. 분석·테스트 환경에서 `.env`가 실거래 자격증명을 포함하거나 `KIS_IS_MOCK=false`이면 실제 주문 경로가 열릴 수 있다.

### 2. 시장 데이터의 실전 서버 강제 사용

`core/api/client.py:get_marketdata`는 모의투자 여부와 무관하게 실전 서버 토큰을 사용할 수 있다. 운영 환경변수와 KIS 권한을 별도로 점검해야 한다.

### 3. DB 스키마 변경

현재 DB 초기화는 `Base.metadata.create_all` 중심이며 migration 파일은 확인되지 않았다. 컬럼 변경·삭제·데이터 마이그레이션 절차가 명시되어 있지 않다.

### 4. Dashboard 상태 변경

전략 toggle과 ticker 변경은 프로세스 메모리의 `STRATEGIES` 객체를 바꾼다. DB에 영구 저장하지 않으며, `DASHBOARD_API_KEY`가 비어 있으면 POST 인증도 강제되지 않는다.

### 5. 자동 commit/push

`.github/workflows/daily_auto.yml`은 `contents: write` 권한으로 리서치 산출물을 commit/push한다. 생성물의 품질·민감정보·대량 변경을 CI 단계에서 별도 검토하는 절차가 필요하다.

### 6. 동적 호출과 서브프로세스

Trailmark에서 2,775개의 proxy node가 나왔다. ORM 메서드, APScheduler, `subprocess.run`, 동적 import, `sys.path` 조작은 정적 그래프에서 완전한 연결로 보이지 않는다.

### 7. 외부 API와 시간 조건

KIS 수급 API에는 시간 제한이 있고, DART·Gemini·뉴스·Telegram은 네트워크 상태와 rate limit의 영향을 받는다. Windows event loop와 장 마감 이후 실행 조건도 운영 절차에 포함되어 있다.

## 검증 결과

| 검사 | 결과 |
| --- | --- |
| `trailmark --version` | Trailmark 0.5.0 |
| 전체 `analyze --summary` | 3,992 nodes / 11,088 call edges |
| 전체 entrypoint scan | 64개 |
| `sync_analysis_paths.py --check` | 통과 |
| 리서치 주요 output path check | 모두 존재 |
| `scripts.test_event_condition_discovery` | 현재 환경에서 `google.generativeai` 미설치로 import 실패 |

단위 테스트 실패는 분석 대상 코드의 assertion 실패가 아니라 개발 환경 의존성 부족으로 발생했다. 의존성을 설치하지 않았으므로 네트워크 기반 API·주문·전체 파이프라인 실행은 의도적으로 수행하지 않았다.

## 남은 확인 작업

- 의존성 설치 후 전체 import smoke test와 단위 테스트 재실행
- mock KIS 자격증명으로 dashboard와 scheduler 시작 smoke test
- KIS/DART/Gemini/Telegram 실제 응답을 사용하는 좁은 통합 테스트
- DB 스키마 migration 필요 여부와 기존 `auto_invest.db` 호환성 확인
- CI가 생성·commit하는 파일의 범위와 비밀값 유출 방지 검토
- Trailmark import edge 미생성 원인 확인 후 모듈 그래프 재생성

