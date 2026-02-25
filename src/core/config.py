"""
Configuration management using Pydantic Settings.
All settings loaded from environment variables or .env file.
"""

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Application
    app_env: Literal["development", "staging", "production"] = Field(
        default="development", alias="APP_ENV"
    )
    app_url: str = Field(default="http://localhost:8000", alias="APP_URL")
    secret_key: str = Field(..., alias="SECRET_KEY")

    # Supabase
    supabase_url: str = Field(..., alias="SUPABASE_URL")
    supabase_key: str = Field(..., alias="SUPABASE_KEY")
    database_url: str = Field(..., alias="DATABASE_URL")

    # Redis (Celery Broker)
    redis_url: str = Field(default="redis://localhost:6379/0", alias="REDIS_URL")

    # Google OAuth2
    google_client_id: str = Field(..., alias="GOOGLE_CLIENT_ID")
    google_client_secret: str = Field(..., alias="GOOGLE_CLIENT_SECRET")
    google_redirect_uri: str = Field(..., alias="GOOGLE_REDIRECT_URI")

    # Anthropic Claude API
    anthropic_api_key: str = Field(..., alias="ANTHROPIC_API_KEY")

    # Obsidian Vault
    obsidian_vault_path: str = Field(
        default="/Users/tooeycourtemanche/Documents/Obsidian Vault - Gmail",
        alias="OBSIDIAN_VAULT_PATH",
    )

    # Gmail Accounts
    gmail_account_1_label: str = Field(default="procore-main", alias="GMAIL_ACCOUNT_1_LABEL")
    gmail_account_1_email: str = Field(default="tooey@procore.com", alias="GMAIL_ACCOUNT_1_EMAIL")
    gmail_account_2_label: str = Field(
        default="procore-private", alias="GMAIL_ACCOUNT_2_LABEL"
    )
    gmail_account_2_email: str = Field(default="2e@procore.com", alias="GMAIL_ACCOUNT_2_EMAIL")
    gmail_account_3_label: str = Field(default="personal", alias="GMAIL_ACCOUNT_3_LABEL")
    gmail_account_3_email: str = Field(
        default="tooey@hth-corp.com", alias="GMAIL_ACCOUNT_3_EMAIL"
    )

    # Rate Limiting
    # Gmail API limits: 250 QPM (~4 QPS average), ~10-20 concurrent requests max
    gmail_rate_limit_qps: int = Field(default=3, alias="GMAIL_RATE_LIMIT_QPS")  # Very conservative: 3 QPS = 180 QPM (72% of limit)
    gmail_rate_limit_burst: int = Field(default=100, alias="GMAIL_RATE_LIMIT_BURST")  # Burst capacity: 100 tokens for initial processing, refills at 3/sec (rebuilds in ~33s)
    gmail_batch_size: int = Field(default=500, alias="GMAIL_BATCH_SIZE")  # Message ID fetch size (single API call)

    # Claude Batch Processing
    claude_batch_size: int = Field(default=100, alias="CLAUDE_BATCH_SIZE")
    claude_model: str = Field(
        default="claude-haiku-4.5-20241022",
        alias="CLAUDE_MODEL",
    )

    # Optional: Monitoring
    sentry_dsn: str | None = Field(default=None, alias="SENTRY_DSN")
    flower_port: int = Field(default=5555, alias="FLOWER_PORT")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("obsidian_vault_path")
    @classmethod
    def validate_vault_path(cls, v: str) -> str:
        """Ensure vault path is absolute."""
        path = Path(v)
        if not path.is_absolute():
            raise ValueError("OBSIDIAN_VAULT_PATH must be an absolute path")
        return v

    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.app_env == "production"

    @property
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.app_env == "development"

    def get_gmail_accounts(self) -> list[dict[str, str]]:
        """Get all configured Gmail accounts."""
        return [
            {"label": self.gmail_account_1_label, "email": self.gmail_account_1_email},
            {"label": self.gmail_account_2_label, "email": self.gmail_account_2_email},
            {"label": self.gmail_account_3_label, "email": self.gmail_account_3_email},
        ]


@lru_cache
def get_settings() -> Settings:
    """
    Get cached settings instance.
    Uses lru_cache to ensure settings are loaded only once.
    """
    return Settings()


# Export singleton instance
settings = get_settings()
