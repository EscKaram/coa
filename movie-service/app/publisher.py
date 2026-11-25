"""Публикация событий в Kafka: Avro + Schema Registry, acks=all, повторы с экспоненциальной задержкой.

Гарантии и их цена:
- acks=all и enable.idempotence=true: брокер подтверждает запись после репликации на все реплики из ISR,
  повторы librdkafka внутри delivery.timeout.ms не создают дубликатов и не меняют порядок в партиции.
- Поверх этого — повторы уровня приложения (PUBLISH_MAX_ATTEMPTS, задержка base * 2^n с джиттером).
  Повтор после таймаута доставки может записать событие второй раз; дубликат схлопывается в ClickHouse
  по event_id (ReplacingMergeTree + FINAL), поэтому семантика конвейера — at-least-once с дедупликацией.
"""

import asyncio
import json
import logging
import random
import threading
import time
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any

import fastavro
import fastavro.validation
from confluent_kafka import KafkaError, KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from .config import Settings

logger = logging.getLogger("movie_service.publisher")


class PublishError(Exception):
    """Событие не записано в Kafka после всех попыток."""


class SchemaValidationError(Exception):
    def __init__(self, details: list[dict[str, str]]):
        super().__init__("event does not match the registered schema")
        self.details = details


@dataclass(frozen=True)
class PublishResult:
    event_id: str
    topic: str
    partition: int
    offset: int
    attempts: int


class EventPublisher:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._topic = settings.topic
        self._sr = SchemaRegistryClient({"url": settings.schema_registry_url})

        if settings.schema_version == "latest":
            registered = self._sr.get_latest_version(settings.schema_subject)
        else:
            registered = self._sr.get_version(settings.schema_subject, int(settings.schema_version))
        self.schema_id: int = registered.schema_id
        self.schema_version: int = registered.version
        schema_str = registered.schema.schema_str
        self._parsed_schema = fastavro.parse_schema(json.loads(schema_str))
        self._schema_fields = {f["name"] for f in json.loads(schema_str)["fields"]}

        # auto.register.schemas=false: продюсер пишет только схемой, заранее зарегистрированной
        # в Schema Registry (app.bootstrap). Незарегистрированная схема — ошибка, а не новая версия.
        self._serializer = AvroSerializer(
            self._sr, schema_str, conf={"auto.register.schemas": False, "use.latest.version": False}
        )

        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "client.id": "movie-service",
                "acks": "all",
                "enable.idempotence": True,
                "max.in.flight.requests.per.connection": 5,
                "delivery.timeout.ms": settings.delivery_timeout_ms,
                "retry.backoff.ms": 100,
                "retry.backoff.max.ms": 1000,
                "linger.ms": 5,
                "compression.type": "zstd",
            }
        )
        self._stop = threading.Event()
        self._poll_thread = threading.Thread(target=self._poll_loop, name="kafka-poll", daemon=True)
        self._poll_thread.start()
        logger.info(
            "producer ready",
            extra={
                "topic": self._topic,
                "schema_subject": settings.schema_subject,
                "schema_version": self.schema_version,
                "schema_id": self.schema_id,
            },
        )

    @classmethod
    def create_with_retry(cls, settings: Settings, attempts: int = 30, delay_s: float = 2.0) -> "EventPublisher":
        """Schema Registry может подняться позже сервиса — ждём его, а не падаем."""
        for attempt in range(1, attempts + 1):
            try:
                return cls(settings)
            except Exception as exc:  # noqa: BLE001 — любая ошибка старта повторяется
                if attempt == attempts:
                    raise
                logger.warning("producer init failed, retrying", extra={"attempt": attempt, "error": str(exc)})
                time.sleep(delay_s)
        raise AssertionError("unreachable")

    # ---------- проверка по схеме ----------

    def validate(self, record: dict[str, Any]) -> dict[str, Any]:
        """Проверка записи по зарегистрированной схеме Avro (fastavro) до отправки."""
        extra = [k for k, v in record.items() if k not in self._schema_fields and v is not None]
        if extra:
            raise SchemaValidationError(
                [{"field": k, "message": f"not in schema version {self.schema_version}"} for k in extra]
            )
        data = {k: v for k, v in record.items() if k in self._schema_fields}
        try:
            fastavro.validation.validate(data, self._parsed_schema, raise_errors=True)
        except fastavro.validation.ValidationError as exc:
            raise SchemaValidationError([{"field": "event", "message": str(err)} for err in exc.errors]) from exc
        return data

    # ---------- отправка ----------

    def _poll_loop(self) -> None:
        # Колбэки доставки вызываются из poll(); отдельный поток не блокирует event loop.
        while not self._stop.is_set():
            self._producer.poll(0.1)

    def _produce(self, record: dict[str, Any]) -> Future:
        future: Future = Future()

        def on_delivery(err: KafkaError | None, msg) -> None:
            if err is not None:
                future.set_exception(KafkaException(err))
            else:
                future.set_result((msg.partition(), msg.offset()))

        value = self._serializer(record, SerializationContext(self._topic, MessageField.VALUE))
        # Ключ — user_id: события одного пользователя попадают в одну партицию и сохраняют порядок.
        self._producer.produce(self._topic, key=record["user_id"].encode(), value=value, on_delivery=on_delivery)
        return future

    def _backoff_ms(self, attempt: int) -> float:
        cap = min(self._settings.publish_retry_max_ms, self._settings.publish_retry_base_ms * 2 ** (attempt - 1))
        return cap * random.uniform(0.5, 1.0)

    async def publish(self, record: dict[str, Any], log_level: int = logging.INFO) -> PublishResult:
        data = self.validate(record)
        max_attempts = self._settings.publish_max_attempts
        for attempt in range(1, max_attempts + 1):
            try:
                partition, offset = await asyncio.wrap_future(self._produce(data))
            except Exception as exc:  # noqa: BLE001 — классификация ниже
                fatal = isinstance(exc, KafkaException) and exc.args and exc.args[0].fatal()
                if fatal or attempt == max_attempts:
                    logger.error(
                        "event publish failed",
                        extra={
                            "event_id": data["event_id"],
                            "event_type": data["event_type"],
                            "attempts": attempt,
                            "error": str(exc),
                        },
                    )
                    raise PublishError(str(exc)) from exc
                delay_ms = self._backoff_ms(attempt)
                logger.warning(
                    "event publish attempt failed, retrying",
                    extra={
                        "event_id": data["event_id"],
                        "attempt": attempt,
                        "retry_in_ms": round(delay_ms),
                        "error": str(exc),
                    },
                )
                await asyncio.sleep(delay_ms / 1000)
                continue
            logger.log(
                log_level,
                "event published",
                extra={
                    "event_id": data["event_id"],
                    "event_type": data["event_type"],
                    "timestamp": data["timestamp"].isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                    "user_id": data["user_id"],
                    "partition": partition,
                    "offset": offset,
                    "attempt": attempt,
                },
            )
            return PublishResult(data["event_id"], self._topic, partition, offset, attempt)
        raise AssertionError("unreachable")

    async def publish_many(self, records: list[dict[str, Any]], log_level: int = logging.DEBUG) -> tuple[int, int]:
        """Пакетная публикация (догрузка истории генератором). produce() вызывается в порядке списка,
        поэтому порядок событий одного пользователя в партиции сохраняется."""
        semaphore = asyncio.Semaphore(5000)

        async def one(record: dict[str, Any]) -> bool:
            async with semaphore:
                try:
                    await self.publish(record, log_level)
                    return True
                except PublishError:
                    return False

        results = await asyncio.gather(*(one(r) for r in records))
        ok = sum(results)
        return ok, len(results) - ok

    def is_ready(self, timeout: float = 2.0) -> bool:
        try:
            metadata = self._producer.list_topics(self._topic, timeout=timeout)
        except KafkaException:
            return False
        topic = metadata.topics.get(self._topic)
        return topic is not None and topic.error is None and len(topic.partitions) > 0

    def close(self) -> None:
        remaining = self._producer.flush(10)
        self._stop.set()
        self._poll_thread.join(timeout=2)
        logger.info("producer closed", extra={"undelivered": remaining})
