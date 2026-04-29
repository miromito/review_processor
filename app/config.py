from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    mongodb_uri: str = "mongodb://localhost:27017"
    mongodb_db: str = "review_analytics"
    openai_api_key: str = ""
    openai_model: str = "gpt-5.4-nano"
    token_limit_t: int = 100_000
    # JSON [{index,text},...] в одном запросе к модели (без учёта system и ответа)
    analysis_batch_token_budget: int = Field(default=50_000, ge=1)
    max_upload_mb: int = 15
    max_import_rows: int = 10_000
    # Google Sheets: загрузка CSV по export URL
    sheet_fetch_timeout_s: int = 45
    sheet_max_bytes: int = Field(default=20 * 1024 * 1024, ge=1)

    # JWT: если AUTH_USERNAME непустой — все страницы и /api кроме логина требуют cookie access_token
    auth_username: str = ""
    auth_password: str = ""
    jwt_secret: str = ""
    jwt_expire_minutes: int = Field(default=60 * 24 * 7, ge=1)
    cookie_secure: bool = False

    # Почта (пустой SMTP — письма не отправляются, проект и анализ работают как раньше)
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    smtp_use_ssl: bool = False
    smtp_use_starttls: bool = True
    mail_from: str = ""
    # Ссылка на проект в письмах (без хвоста)
    app_base_url: str = "http://127.0.0.1:8000"

    @model_validator(mode="after")
    def jwt_required_when_auth_on(self):
        if self.auth_username.strip() and not self.jwt_secret.strip():
            raise ValueError("JWT_SECRET обязателен, если задан AUTH_USERNAME")
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
