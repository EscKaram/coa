"""Однократная инициализация конвейера (контейнер pipeline-init): топик Kafka и схемы в Schema Registry.

Идемпотентно: повторный запуск ничего не меняет и только проверяет, что конфигурация совпадает.
1. Ждёт, пока в кластере появятся KAFKA_EXPECTED_BROKERS брокеров.
2. Создаёт топик movie-events: TOPIC_PARTITIONS партиций, replication.factor и min.insync.replicas из
   окружения; проверяет фактическую конфигурацию существующего топика.
3. Ставит субъекту movie-events-value режим совместимости BACKWARD и регистрирует схемы
   schemas/movie_event.v*.avsc по возрастанию номера; Schema Registry присваивает им версии 1, 2, ...
"""

import logging
import sys
import time
from pathlib import Path

from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from .config import Settings
from .logging_setup import setup_logging

logger = logging.getLogger("movie_service.bootstrap")


def wait_for_brokers(admin: AdminClient, expected: int, timeout_s: int = 180) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            brokers = admin.list_topics(timeout=5).brokers
            if len(brokers) >= expected:
                logger.info("kafka brokers available", extra={"brokers": sorted(b.id for b in brokers.values())})
                return
            logger.info("waiting for kafka brokers", extra={"available": len(brokers), "expected": expected})
        except Exception as exc:  # noqa: BLE001
            logger.info("waiting for kafka", extra={"error": str(exc)})
        if time.monotonic() > deadline:
            raise TimeoutError("kafka brokers did not become available")
        time.sleep(2)


def ensure_topic(admin: AdminClient, s: Settings) -> None:
    topics = admin.list_topics(timeout=10).topics
    if s.topic not in topics:
        new_topic = NewTopic(
            s.topic,
            num_partitions=s.topic_partitions,
            replication_factor=s.topic_replication_factor,
            config={
                "min.insync.replicas": str(s.topic_min_insync_replicas),
                "retention.ms": str(s.topic_retention_ms),
                "cleanup.policy": "delete",
            },
        )
        try:
            admin.create_topics([new_topic])[s.topic].result(timeout=30)
            logger.info("topic created", extra={"topic": s.topic})
        except Exception as exc:  # noqa: BLE001
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise
        time.sleep(1)

    metadata = admin.list_topics(s.topic, timeout=10).topics[s.topic]
    partitions = metadata.partitions
    rf = min(len(p.replicas) for p in partitions.values())
    resource = ConfigResource(ConfigResource.Type.TOPIC, s.topic)
    config = admin.describe_configs([resource])[resource].result(timeout=10)
    min_isr = int(config["min.insync.replicas"].value)
    logger.info(
        "topic configuration",
        extra={
            "topic": s.topic,
            "partitions": len(partitions),
            "replication_factor": rf,
            "min_insync_replicas": min_isr,
            "leaders": {pid: p.leader for pid, p in sorted(partitions.items())},
        },
    )
    if len(partitions) < s.topic_partitions or rf < s.topic_replication_factor or min_isr < s.topic_min_insync_replicas:
        raise RuntimeError(f"topic {s.topic} exists with weaker configuration than required")


def ensure_schemas(s: Settings) -> None:
    sr = SchemaRegistryClient({"url": s.schema_registry_url})
    deadline = time.monotonic() + 180
    while True:
        try:
            sr.get_subjects()
            break
        except Exception as exc:  # noqa: BLE001
            if time.monotonic() > deadline:
                raise
            logger.info("waiting for schema registry", extra={"error": str(exc)})
            time.sleep(2)

    sr.set_compatibility(subject_name=s.schema_subject, level="BACKWARD")
    files = sorted(s.schema_dir.glob("movie_event.v*.avsc"), key=lambda p: int(p.stem.rsplit(".v", 1)[1]))
    if not files:
        raise RuntimeError(f"no schema files in {s.schema_dir}")
    for path in files:
        schema = Schema(Path(path).read_text(encoding="utf-8"), "AVRO")
        if sr.get_subjects() and s.schema_subject in sr.get_subjects():
            if not sr.test_compatibility(s.schema_subject, schema):
                raise RuntimeError(f"{path.name} is not BACKWARD compatible with the registered versions")
        schema_id = sr.register_schema(s.schema_subject, schema)
        registered = sr.lookup_schema(s.schema_subject, schema)
        logger.info(
            "schema registered",
            extra={"file": path.name, "subject": s.schema_subject, "version": registered.version, "schema_id": schema_id},
        )
    logger.info(
        "schema subject state",
        extra={
            "subject": s.schema_subject,
            "versions": sr.get_versions(s.schema_subject),
            "compatibility": sr.get_compatibility(s.schema_subject),
        },
    )


def main() -> int:
    settings = Settings.from_env()
    setup_logging(settings.log_level)
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    wait_for_brokers(admin, settings.expected_brokers)
    ensure_topic(admin, settings)
    ensure_schemas(settings)
    logger.info("pipeline bootstrap finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())
