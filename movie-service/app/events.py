"""Модель события для HTTP API и проверки, которые не выражаются типами Avro."""

from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class EventType(str, Enum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


VIEW_EVENTS = frozenset(
    {EventType.VIEW_STARTED, EventType.VIEW_FINISHED, EventType.VIEW_PAUSED, EventType.VIEW_RESUMED}
)

# Допустимое расхождение часов клиента: событие не может быть из будущего дальше этого порога.
MAX_CLOCK_SKEW = timedelta(minutes=5)
AVRO_INT_MAX = 2**31 - 1


class MovieEventIn(BaseModel):
    """Тело POST /api/v1/events. event_id и timestamp необязательны: их проставит сервис."""

    model_config = ConfigDict(extra="forbid")

    event_id: UUID | None = Field(default=None, description="Если не задан, сервис сгенерирует UUIDv4")
    user_id: str = Field(min_length=1, max_length=128)
    movie_id: str = Field(min_length=1, max_length=128)
    event_type: EventType
    timestamp: AwareDatetime | None = Field(
        default=None, description="ISO 8601 с часовым поясом; хранится в UTC. Если не задан — время приёма"
    )
    device_type: DeviceType
    session_id: str = Field(min_length=1, max_length=128)
    progress_seconds: int | None = Field(default=None, ge=0, le=AVRO_INT_MAX)
    search_query: str | None = Field(default=None, min_length=1, max_length=512)


class PublishedEvent(BaseModel):
    event_id: UUID
    topic: str
    partition: int
    offset: int
    schema_version: int


def semantic_errors(event: MovieEventIn, now: datetime | None = None) -> list[dict[str, str]]:
    """Правила, которые схема Avro не проверяет: связь event_type с progress_seconds и search_query."""
    now = now or datetime.now(timezone.utc)
    errors: list[dict[str, str]] = []
    if event.event_type in VIEW_EVENTS and event.progress_seconds is None:
        errors.append({"field": "progress_seconds", "message": f"required for {event.event_type.value}"})
    if event.event_type not in VIEW_EVENTS and event.progress_seconds is not None:
        errors.append(
            {"field": "progress_seconds", "message": f"allowed only for VIEW_* events, got {event.event_type.value}"}
        )
    if event.search_query is not None and event.event_type is not EventType.SEARCHED:
        errors.append({"field": "search_query", "message": "allowed only for SEARCHED"})
    if event.timestamp is not None and event.timestamp - now > MAX_CLOCK_SKEW:
        errors.append({"field": "timestamp", "message": "is in the future"})
    return errors


def to_record(event: MovieEventIn, now: datetime | None = None) -> dict[str, Any]:
    """Запись для AvroSerializer: время в UTC (timestamp-millis), перечисления — строками."""
    ts = (event.timestamp or now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return {
        "event_id": str(event.event_id or uuid4()),
        "user_id": event.user_id,
        "movie_id": event.movie_id,
        "event_type": event.event_type.value,
        "timestamp": ts,
        "device_type": event.device_type.value,
        "session_id": event.session_id,
        "progress_seconds": event.progress_seconds,
        "search_query": event.search_query,
    }
