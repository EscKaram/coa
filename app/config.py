"""Настройки сервиса из переменных окружения."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    jwt_secret: str
    access_token_ttl_minutes: int
    refresh_token_ttl_days: int
    # N из задания: минимальный интервал между CREATE_ORDER (и между UPDATE_ORDER) одного пользователя.
    order_rate_limit_minutes: int
    admin_email: str | None
    admin_password: str | None


def _load() -> Settings:
    return Settings(
        database_url=os.getenv(
            "DATABASE_URL", "postgresql+asyncpg://marketplace:marketplace@localhost:5433/marketplace"
        ),
        jwt_secret=os.getenv("JWT_SECRET", "dev-secret-change-me-dev-secret-32b"),
        access_token_ttl_minutes=int(os.getenv("ACCESS_TOKEN_TTL_MINUTES", "15")),
        refresh_token_ttl_days=int(os.getenv("REFRESH_TOKEN_TTL_DAYS", "7")),
        order_rate_limit_minutes=int(os.getenv("ORDER_RATE_LIMIT_MINUTES", "1")),
        admin_email=os.getenv("ADMIN_EMAIL"),
        admin_password=os.getenv("ADMIN_PASSWORD"),
    )


settings = _load()
