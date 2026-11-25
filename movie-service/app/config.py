"""Настройки Movie Service из переменных окружения."""

import os
from dataclasses import dataclass
from pathlib import Path


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _bool(name: str, default: bool) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    http_port: int
    log_level: str

    kafka_bootstrap_servers: str
    schema_registry_url: str
    topic: str
    schema_subject: str
    # "latest" или номер версии в Schema Registry, которой продюсер сериализует события.
    schema_version: str

    # Публикация: попытки уровня приложения поверх внутренних повторов librdkafka.
    publish_max_attempts: int
    publish_retry_base_ms: int
    publish_retry_max_ms: int
    delivery_timeout_ms: int

    # Создание топика и регистрация схем (python -m app.bootstrap).
    topic_partitions: int
    topic_replication_factor: int
    topic_min_insync_replicas: int
    topic_retention_ms: int
    expected_brokers: int
    schema_dir: Path

    generator_enabled: bool
    generator_seed: int
    generator_backfill_days: int
    generator_new_users_per_day: int
    generator_movies: int

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            http_port=_int("PORT", 8000),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
            kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-1:19092,kafka-2:19092"),
            schema_registry_url=os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
            topic=os.environ.get("KAFKA_TOPIC", "movie-events"),
            schema_subject=os.environ.get("SCHEMA_SUBJECT", "movie-events-value"),
            schema_version=os.environ.get("SCHEMA_VERSION", "latest"),
            publish_max_attempts=_int("PUBLISH_MAX_ATTEMPTS", 5),
            publish_retry_base_ms=_int("PUBLISH_RETRY_BASE_MS", 200),
            publish_retry_max_ms=_int("PUBLISH_RETRY_MAX_MS", 5000),
            delivery_timeout_ms=_int("PUBLISH_DELIVERY_TIMEOUT_MS", 10000),
            topic_partitions=_int("TOPIC_PARTITIONS", 3),
            topic_replication_factor=_int("TOPIC_REPLICATION_FACTOR", 2),
            topic_min_insync_replicas=_int("TOPIC_MIN_INSYNC_REPLICAS", 1),
            topic_retention_ms=_int("TOPIC_RETENTION_MS", 7 * 24 * 3600 * 1000),
            expected_brokers=_int("KAFKA_EXPECTED_BROKERS", 2),
            schema_dir=Path(os.environ.get("SCHEMA_DIR", "/srv/schemas")),
            generator_enabled=_bool("GENERATOR_ENABLED", True),
            generator_seed=_int("GENERATOR_SEED", 42),
            generator_backfill_days=_int("GENERATOR_BACKFILL_DAYS", 10),
            generator_new_users_per_day=_int("GENERATOR_NEW_USERS_PER_DAY", 300),
            generator_movies=_int("GENERATOR_MOVIES", 40),
        )
