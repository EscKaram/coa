"""Настройки Booking Service из переменных окружения."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    flight_service_addr: str
    flight_service_api_key: str
    # Дедлайн одной попытки вызова Flight Service.
    flight_rpc_timeout_ms: int
    retry_max_attempts: int
    retry_initial_backoff_ms: int
    retry_backoff_multiplier: float
    cb_failure_threshold: int
    cb_window_seconds: float
    cb_open_timeout_seconds: float


def load_settings() -> Settings:
    api_key = os.getenv("FLIGHT_SERVICE_API_KEY", "")
    if not api_key:
        raise ValueError("FLIGHT_SERVICE_API_KEY is required")
    settings = Settings(
        database_url=os.getenv("DATABASE_URL", "postgresql+asyncpg://bookings:bookings@localhost:5434/bookings"),
        flight_service_addr=os.getenv("FLIGHT_SERVICE_ADDR", "localhost:50051"),
        flight_service_api_key=api_key,
        flight_rpc_timeout_ms=int(os.getenv("FLIGHT_RPC_TIMEOUT_MS", "2000")),
        retry_max_attempts=int(os.getenv("RETRY_MAX_ATTEMPTS", "3")),
        retry_initial_backoff_ms=int(os.getenv("RETRY_INITIAL_BACKOFF_MS", "100")),
        retry_backoff_multiplier=float(os.getenv("RETRY_BACKOFF_MULTIPLIER", "2")),
        cb_failure_threshold=int(os.getenv("CB_FAILURE_THRESHOLD", "5")),
        cb_window_seconds=float(os.getenv("CB_WINDOW_SECONDS", "30")),
        cb_open_timeout_seconds=float(os.getenv("CB_OPEN_TIMEOUT_SECONDS", "15")),
    )
    if not 1 <= settings.retry_max_attempts <= 10:
        raise ValueError("RETRY_MAX_ATTEMPTS must be in [1, 10]")
    if settings.flight_rpc_timeout_ms <= 0 or settings.retry_initial_backoff_ms < 0 or settings.retry_backoff_multiplier < 1:
        raise ValueError("FLIGHT_RPC_TIMEOUT_MS > 0, RETRY_INITIAL_BACKOFF_MS >= 0, RETRY_BACKOFF_MULTIPLIER >= 1")
    if settings.cb_failure_threshold < 1 or settings.cb_window_seconds <= 0 or settings.cb_open_timeout_seconds <= 0:
        raise ValueError("CB_FAILURE_THRESHOLD >= 1, CB_WINDOW_SECONDS > 0, CB_OPEN_TIMEOUT_SECONDS > 0")
    return settings
