"""Настройки Flight Service из переменных окружения."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    port: int
    database_url: str
    db_pool_size: int
    # Допустимые API-ключи клиентов. Несколько ключей — для ротации без простоя.
    api_keys: tuple[str, ...]
    redis_sentinels: tuple[tuple[str, int], ...]
    redis_master_name: str
    redis_socket_timeout_ms: int
    cache_ttl_seconds: int
    # TTL счётчиков поколений (см. app/cache.py). Должен быть заметно больше времени одного запроса.
    cache_generation_ttl_seconds: int
    search_limit: int


def _sentinels(raw: str) -> tuple[tuple[str, int], ...]:
    result = []
    for item in filter(None, (part.strip() for part in raw.split(","))):
        host, _, port = item.rpartition(":")
        result.append((host, int(port)))
    if not result:
        raise ValueError("REDIS_SENTINELS must list at least one host:port")
    return tuple(result)


def load_settings() -> Settings:
    api_keys = tuple(k.strip() for k in os.getenv("FLIGHT_SERVICE_API_KEYS", "").split(",") if k.strip())
    if not api_keys:
        # Без ключей сервис не стартует: работать без аутентификации он не должен.
        raise ValueError("FLIGHT_SERVICE_API_KEYS must contain at least one key")
    settings = Settings(
        port=int(os.getenv("GRPC_PORT", "50051")),
        database_url=os.getenv("DATABASE_URL", "postgresql+asyncpg://flights:flights@localhost:5435/flights"),
        db_pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
        api_keys=api_keys,
        redis_sentinels=_sentinels(os.getenv("REDIS_SENTINELS", "localhost:26379")),
        redis_master_name=os.getenv("REDIS_MASTER_NAME", "mymaster"),
        redis_socket_timeout_ms=int(os.getenv("REDIS_SOCKET_TIMEOUT_MS", "300")),
        cache_ttl_seconds=int(os.getenv("CACHE_TTL_SECONDS", "300")),
        cache_generation_ttl_seconds=int(os.getenv("CACHE_GENERATION_TTL_SECONDS", "3600")),
        search_limit=int(os.getenv("SEARCH_LIMIT", "100")),
    )
    if settings.cache_ttl_seconds <= 0 or settings.cache_generation_ttl_seconds <= settings.cache_ttl_seconds:
        raise ValueError("CACHE_TTL_SECONDS must be > 0 and < CACHE_GENERATION_TTL_SECONDS")
    return settings
