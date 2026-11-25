"""Настройки сервиса агрегации из переменных окружения."""

import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


@dataclass(frozen=True)
class Settings:
    http_port: int
    log_level: str

    clickhouse_host: str
    clickhouse_port: int
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_database: str

    postgres_dsn: str
    write_max_attempts: int
    write_retry_base_ms: int
    write_retry_max_ms: int

    # Расписание пересчёта: каждые AGG_INTERVAL_SECONDS пересчитываются дни [сегодня - AGG_LOOKBACK_DAYS, сегодня].
    agg_interval_seconds: int
    agg_initial_delay_seconds: int
    agg_lookback_days: int
    # Первый плановый цикл после старта захватывает больше дней, чтобы досчитать уже накопленную историю.
    agg_initial_lookback_days: int
    top_movies_limit: int

    # Экспорт: каждые EXPORT_INTERVAL_SECONDS выгружаются закрытые дни [сегодня - EXPORT_LOOKBACK_DAYS, вчера],
    # если файла ещё нет или значения в PostgreSQL изменились.
    export_interval_seconds: int
    export_initial_delay_seconds: int
    export_lookback_days: int
    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str
    s3_region: str
    s3_bucket: str
    s3_prefix: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            http_port=_int("PORT", 8001),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
            clickhouse_host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
            clickhouse_port=_int("CLICKHOUSE_PORT", 8123),
            clickhouse_user=os.environ.get("CLICKHOUSE_USER", "cinema"),
            clickhouse_password=os.environ.get("CLICKHOUSE_PASSWORD", "cinema"),
            clickhouse_database=os.environ.get("CLICKHOUSE_DATABASE", "cinema"),
            postgres_dsn=os.environ.get(
                "POSTGRES_DSN", "postgresql://analytics:analytics@postgres:5432/analytics"
            ),
            write_max_attempts=_int("WRITE_MAX_ATTEMPTS", 5),
            write_retry_base_ms=_int("WRITE_RETRY_BASE_MS", 500),
            write_retry_max_ms=_int("WRITE_RETRY_MAX_MS", 8000),
            agg_interval_seconds=_int("AGG_INTERVAL_SECONDS", 60),
            agg_initial_delay_seconds=_int("AGG_INITIAL_DELAY_SECONDS", 15),
            agg_lookback_days=_int("AGG_LOOKBACK_DAYS", 8),
            agg_initial_lookback_days=_int("AGG_INITIAL_LOOKBACK_DAYS", 30),
            top_movies_limit=_int("TOP_MOVIES_LIMIT", 10),
            export_interval_seconds=_int("EXPORT_INTERVAL_SECONDS", 300),
            export_initial_delay_seconds=_int("EXPORT_INITIAL_DELAY_SECONDS", 45),
            export_lookback_days=_int("EXPORT_LOOKBACK_DAYS", 7),
            s3_endpoint_url=os.environ.get("S3_ENDPOINT_URL", "http://minio:9000"),
            s3_access_key=os.environ.get("S3_ACCESS_KEY", "minioadmin"),
            s3_secret_key=os.environ.get("S3_SECRET_KEY", "minioadmin"),
            s3_region=os.environ.get("S3_REGION", "us-east-1"),
            s3_bucket=os.environ.get("S3_BUCKET", "movie-analytics"),
            s3_prefix=os.environ.get("S3_PREFIX", "daily"),
        )
