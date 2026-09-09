from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # KIS read-only market data credentials.
    kis_mock_app_key: str = ""
    kis_mock_app_secret: str = ""
    kis_account_no: str = ""
    kis_real_app_key: str = ""
    kis_real_app_secret: str = ""
    kis_real_account_no: str = ""
    kis_is_mock: bool = True

    # Structured OpenDART data.
    dart_api_key: str = ""

    # Forecast artifact location.
    forecast_artifact_dir: str = "data/forecast"

    @property
    def kis_app_key(self) -> str:
        return self.kis_mock_app_key if self.kis_is_mock else self.kis_real_app_key

    @property
    def kis_app_secret(self) -> str:
        return self.kis_mock_app_secret if self.kis_is_mock else self.kis_real_app_secret

    @property
    def kis_account(self) -> str:
        return self.kis_account_no if self.kis_is_mock else self.kis_real_account_no

    @property
    def kis_base_url(self) -> str:
        if self.kis_is_mock:
            return "https://openapivts.koreainvestment.com:29443"
        return "https://openapi.koreainvestment.com:9443"


settings = Settings()
