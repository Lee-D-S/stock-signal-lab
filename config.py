from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # 한국투자증권 — 모의투자
    kis_mock_app_key: str = ""
    kis_mock_app_secret: str = ""
    kis_mock_account_no: str = ""

    # 한국투자증권 — 실거래
    kis_real_app_key: str = ""
    kis_real_app_secret: str = ""
    kis_real_account_no: str = ""

    # true=모의투자, false=실거래
    kis_is_mock: bool = True

    @property
    def kis_app_key(self) -> str:
        return self.kis_mock_app_key if self.kis_is_mock else self.kis_real_app_key

    @property
    def kis_app_secret(self) -> str:
        return self.kis_mock_app_secret if self.kis_is_mock else self.kis_real_app_secret

    @property
    def kis_account_no(self) -> str:
        return self.kis_mock_account_no if self.kis_is_mock else self.kis_real_account_no

    # 텔레그램
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # Gemini
    gemini_api_key: str = ""

    # DART
    dart_api_key: str = ""

    # 대시보드
    dashboard_host: str = "0.0.0.0"
    dashboard_port: int = 8000

    # 매매 설정
    max_order_amount: int = 100000
    max_positions: int = 10
    news_crawl_interval_min: int = 10
    stop_loss_pct: float = -0.05   # 손절 기준 (-5%)
    take_profit_pct: float = 0.10  # 익절 기준 (+10%)

    @property
    def kis_base_url(self) -> str:
        if self.kis_is_mock:
            return "https://openapivts.koreainvestment.com:29443"
        return "https://openapi.koreainvestment.com:9443"


settings = Settings()
