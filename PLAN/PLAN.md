# Auto-Invest 프로젝트 플랜

## Context
한국투자증권 Open Trading API를 활용한 국내 주식/ETF 자동 매매 프로그램.
- 위치: `C:\projects\auto-invest`
- 목표: 전략 플러그인 구조로 여러 전략을 쉽게 추가/교체 가능하게 설계
- 초기 전략: 이동평균 크로스(MA Cross)
- 모니터링: FastAPI 대시보드 + 텔레그램 알림

---

## 기술 스택

| 역할 | 라이브러리 |
|------|-----------|
| HTTP 클라이언트 | `httpx` (async) |
| 실시간 시세 | `websockets` |
| 기술 지표 | `pandas` + `pandas-ta` |
| DB ORM | `SQLAlchemy` + `aiosqlite` (SQLite) |
| 스케줄링 | `APScheduler` |
| 대시보드 API | `FastAPI` + `uvicorn` |
| 텔레그램 알림 | `python-telegram-bot` |
| 설정 관리 | `pydantic-settings` |

---

## 프로젝트 구조

```
auto-invest/
├── core/
│   ├── api/
│   │   ├── client.py          # 한국투자증권 REST API 클라이언트 (httpx)
│   │   ├── auth.py            # OAuth2 토큰 발급/갱신/캐싱
│   │   └── websocket.py       # 실시간 시세 WebSocket 수신
│   ├── broker.py              # 매수/매도/취소 주문 실행
│   └── market_data.py         # 현재가, 차트(OHLCV) 조회
├── strategies/
│   ├── base.py                # BaseStrategy 추상 클래스
│   └── ma_cross.py            # 이동평균 크로스 전략 (첫 구현)
├── scheduler/
│   └── runner.py              # APScheduler - 장중(09:00~15:30) 자동 실행
├── notifier/
│   └── telegram.py            # 텔레그램 봇 매수/매도/에러 알림
├── models/
│   ├── database.py            # SQLAlchemy engine/session 설정
│   ├── trade_log.py           # TradeLog 모델 (매매 내역)
│   └── position.py            # Position 모델 (현재 포지션)
├── dashboard/                 # FastAPI 대시보드
│   ├── main.py                # FastAPI app, Swagger UI
│   └── routers/
│       ├── trades.py          # GET /trades - 매매 내역
│       ├── positions.py       # GET /positions - 잔고/포지션
│       └── strategies.py      # GET/POST /strategies - 전략 설정
├── config.py                  # pydantic-settings 환경변수 관리
├── main.py                    # 진입점 (scheduler + dashboard 동시 실행)
├── requirements.txt
└── .env.example
```

---

## 구현 단계

### Phase 1 — 프로젝트 기반 세팅
- `requirements.txt` 작성
- `.env.example` 작성 (API 키, 텔레그램 봇 토큰 등)
- `config.py` — pydantic-settings로 환경변수 로드
- `models/database.py` — SQLite + SQLAlchemy async 엔진 설정
- `models/trade_log.py`, `models/position.py` — DB 모델 정의

### Phase 2 — 한국투자증권 API 클라이언트
- `core/api/auth.py` — OAuth2 토큰 발급/만료 시 자동 갱신
- `core/api/client.py` — 인증 헤더 자동 주입, 에러 핸들링
- `core/market_data.py` — 현재가 조회, 일봉/분봉 OHLCV 조회
- `core/broker.py` — 지정가/시장가 매수·매도 주문, 잔고 조회

### Phase 3 — 전략 엔진
- `strategies/base.py` — BaseStrategy 추상 클래스
  ```python
  class BaseStrategy(ABC):
      name: str
      tickers: list[str]          # 감시 종목 리스트

      @abstractmethod
      def should_buy(self, ticker: str, df: pd.DataFrame) -> bool: ...

      @abstractmethod
      def should_sell(self, ticker: str, df: pd.DataFrame) -> bool: ...

      def get_order_quantity(self, ticker: str, price: float) -> int: ...
  ```
- `strategies/ma_cross.py` — MA Cross 전략 (단기/장기 이동평균 파라미터화)

### Phase 4 — 스케줄러
- `scheduler/runner.py`
  - APScheduler로 1분/5분 주기로 전략 실행
  - 장 운영 시간(09:00~15:30, 평일) 외 자동 비활성화
  - 등록된 모든 전략 순회하며 매수/매도 신호 체크

### Phase 5 — 텔레그램 알림
- `notifier/telegram.py`
  - 매수 체결, 매도 체결, 에러 발생 시 메시지 전송
  - 일일 수익률 요약 (장 마감 후 15:35)

### Phase 6 — FastAPI 대시보드
- `dashboard/main.py` — FastAPI 앱, Swagger UI 자동 제공
- `GET /trades` — 날짜 필터 가능한 매매 내역
- `GET /positions` — 현재 보유 종목, 평단가, 수익률
- `GET /strategies` — 활성화된 전략 목록
- `POST /strategies/{name}/toggle` — 전략 활성화/비활성화

### Phase 7 — 통합 및 진입점
- `main.py` — 스케줄러와 FastAPI 서버를 `asyncio`로 동시 실행

---

## 전략 추가 방법 (플러그인 구조)
나중에 전략을 추가할 때는 `strategies/` 아래 파일 하나만 추가하면 됩니다:
```python
# strategies/rsi.py
class RSIStrategy(BaseStrategy):
    name = "rsi"
    ...
```
그 다음 `scheduler/runner.py`의 전략 목록에 등록하면 끝.

---

## 환경변수 (.env)
```
# 한국투자증권 API
KIS_APP_KEY=
KIS_APP_SECRET=
KIS_ACCOUNT_NO=
KIS_IS_MOCK=true            # 모의투자 여부

# 텔레그램
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

# 대시보드
DASHBOARD_PORT=8000
```

---

## 검증 방법
1. **모의투자 환경**으로 먼저 실행 (`KIS_IS_MOCK=true`)
2. `python main.py` 실행 후 `http://localhost:8000/docs` 에서 Swagger UI 확인
3. 삼성전자(005930) 등 종목 추가 후 신호 발생 여부 로그 확인
4. 텔레그램 봇에 테스트 메시지 수신 확인
5. SQLite DB에 `trade_log` 레코드 쌓이는지 확인
