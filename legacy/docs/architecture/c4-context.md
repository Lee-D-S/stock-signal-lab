# C4 시스템 컨텍스트

```mermaid
C4Context
  title Auto Invest & Stock Research Platform - System Context

  Person(operator, "운영자/연구자", "전략을 실행하고 후보·관찰 결과를 검토하는 사용자")
  System(platform, "Auto Invest & Stock Research Platform", "KIS 기반 자동매매 런타임과 주가 원인 리서치·백테스트 플랫폼")

  System_Ext(kis, "한국투자증권 KIS API", "OAuth2, 시세, OHLCV, 잔고, 현금 주문")
  System_Ext(dart, "DART OpenAPI", "기업 코드, 공시, 재무 데이터")
  System_Ext(gemini, "Google Gemini", "뉴스·기업 분석 보조")
  System_Ext(ollama, "Ollama", "선택적 로컬 LLM 검토")
  System_Ext(news, "뉴스 소스", "Naver Finance와 RSS 피드")
  System_Ext(telegram, "Telegram", "체결·오류·일일 요약 알림")
  System_Ext(actions, "GitHub Actions", "오전 공시 확인과 일일 리서치 자동 실행")

  Rel(operator, platform, "실행·설정·대시보드 조회")
  Rel(platform, kis, "시장 데이터 조회 및 모의/실주문 API 호출", "HTTPS")
  Rel(platform, dart, "기업·공시·재무 데이터 조회", "HTTPS")
  Rel(platform, gemini, "뉴스·기업 분석 요청", "HTTPS")
  Rel(platform, ollama, "선택적 Agent 검토 요청", "HTTP")
  Rel(platform, news, "뉴스·RSS 수집", "HTTP/RSS")
  Rel(platform, telegram, "운영 알림 전송", "HTTPS")
  Rel(actions, platform, "예약된 CLI 파이프라인 실행", "CI runner")
```

## 컨텍스트 해석

운영자는 두 가지 작업을 수행한다.

1. KIS API에 연결된 자동매매 앱을 모의투자 또는 실거래 모드로 운영한다.
2. 기업 보고서와 시장 데이터를 기반으로 리서치·백테스트·관찰 결과를 검토한다.

GitHub Actions는 사람이 직접 실행하지 않아도 리서치 결과를 갱신하고 변경 사항을 commit/push한다. Telegram은 명령 채널이 아니라 결과·오류 알림 채널이다.

