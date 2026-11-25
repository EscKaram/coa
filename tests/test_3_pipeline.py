"""П. 3–4: путь события от публикации в Kafka до строки в ClickHouse.

Изоляция: у каждого теста свои user_id (it-user-<uuid>), после теста их события удаляются (фикстура created_users).
"""

import json
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from conftest import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    SUBJECT,
    TOPIC,
    ch_query,
    make_event,
    publish,
    wait_until,
)

EVENT_COLUMNS = """
    toString(event_id) AS event_id, user_id, movie_id, toString(event_type) AS event_type,
    formatDateTime(timestamp, '%Y-%m-%dT%H:%i:%S.%fZ') AS ts, toString(event_date) AS event_date,
    toString(device_type) AS device_type, session_id, progress_seconds, search_query, kafka_partition, kafka_offset
"""


def _rows_for_user(user_id: str) -> list[dict]:
    return ch_query(
        f"SELECT {EVENT_COLUMNS} FROM cinema.movie_events FINAL WHERE user_id = {{u:String}} ORDER BY timestamp",
        {"u": user_id},
    )


def test_event_reaches_clickhouse_with_all_fields(created_users):
    ts = datetime.now(timezone.utc).replace(microsecond=123000)
    event = make_event(
        event_type="VIEW_PAUSED",
        progress_seconds=1834,
        device_type="TABLET",
        timestamp=ts.isoformat().replace("+00:00", "Z"),
    )
    created_users.append(event["user_id"])
    published = publish(event)

    rows = wait_until(lambda: _rows_for_user(event["user_id"]), timeout=60, what="event in ClickHouse")
    assert len(rows) == 1
    row = rows[0]
    assert row["event_id"] == event["event_id"]
    assert row["user_id"] == event["user_id"]
    assert row["movie_id"] == event["movie_id"]
    assert row["event_type"] == "VIEW_PAUSED"
    assert row["ts"] == ts.strftime("%Y-%m-%dT%H:%M:%S.123Z")
    assert row["event_date"] == ts.date().isoformat()
    assert row["device_type"] == "TABLET"
    assert row["session_id"] == event["session_id"]
    assert row["progress_seconds"] == 1834
    assert row["search_query"] is None
    assert row["kafka_partition"] == published["partition"]
    assert row["kafka_offset"] == published["offset"]


def test_search_event_keeps_query(created_users):
    event = make_event(event_type="SEARCHED", progress_seconds=None, search_query="northern orbit")
    created_users.append(event["user_id"])
    publish(event)
    rows = wait_until(lambda: _rows_for_user(event["user_id"]), timeout=60, what="event in ClickHouse")
    assert rows[0]["search_query"] == "northern orbit"
    assert rows[0]["progress_seconds"] is None


def test_user_events_share_partition_and_keep_order(created_users):
    """Ключ партиционирования user_id: сессия пользователя в одной партиции, смещения растут по порядку событий."""
    user = f"it-user-{uuid.uuid4().hex[:12]}"
    session = f"it-session-{uuid.uuid4().hex[:12]}"
    created_users.append(user)
    start = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(minutes=30)
    flow = [("VIEW_STARTED", 0), ("VIEW_PAUSED", 400), ("VIEW_RESUMED", 400), ("VIEW_FINISHED", 1500), ("LIKED", None)]
    partitions = set()
    for i, (event_type, progress) in enumerate(flow):
        body = publish(
            make_event(
                user_id=user,
                session_id=session,
                event_type=event_type,
                progress_seconds=progress,
                timestamp=(start + timedelta(minutes=i * 5)).isoformat().replace("+00:00", "Z"),
            )
        )
        partitions.add(body["partition"])
    assert len(partitions) == 1

    rows = wait_until(lambda: (r := _rows_for_user(user)) and len(r) == len(flow) and r, timeout=60, what="session")
    assert [r["event_type"] for r in rows] == [e for e, _ in flow]
    offsets = [r["kafka_offset"] for r in rows]
    assert offsets == sorted(offsets)


def test_duplicate_delivery_is_collapsed(created_users):
    """Повторная публикация того же event_id (at-least-once) даёт одну строку в ClickHouse.

    Обе доставки лежат в Kafka под разными смещениями. Контрольное событие того же пользователя публикуется
    после них в ту же партицию; когда оно появилось в ClickHouse, обе доставки точно прочитаны.
    """
    event = make_event()
    created_users.append(event["user_id"])
    first, second = publish(event), publish(event)
    assert first["partition"] == second["partition"]
    assert second["offset"] > first["offset"]

    sentinel = make_event(user_id=event["user_id"], event_type="LIKED", progress_seconds=None)
    publish(sentinel)
    wait_until(
        lambda: ch_query(
            "SELECT count() AS n FROM cinema.movie_events WHERE event_id = {e:UUID}", {"e": sentinel["event_id"]}
        )[0]["n"],
        timeout=60,
        what="sentinel event",
    )
    rows = ch_query(
        "SELECT count() AS n FROM cinema.movie_events FINAL WHERE event_id = {e:UUID}", {"e": event["event_id"]}
    )
    assert rows[0]["n"] == 1


def test_message_written_with_schema_v1_is_readable(created_users):
    """Продюсер старой версии (схема v1, без search_query): ClickHouse принимает событие, search_query = NULL."""
    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    v1 = sr.get_version(SUBJECT, 1).schema.schema_str
    serializer = AvroSerializer(sr, v1, conf={"auto.register.schemas": False})
    event = make_event(event_type="VIEW_RESUMED", progress_seconds=77)
    created_users.append(event["user_id"])
    record = {**event, "timestamp": datetime.now(timezone.utc)}
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "acks": "all"})
    producer.produce(
        TOPIC,
        key=event["user_id"].encode(),
        value=serializer(record, SerializationContext(TOPIC, MessageField.VALUE)),
    )
    assert producer.flush(15) == 0

    rows = wait_until(lambda: _rows_for_user(event["user_id"]), timeout=60, what="v1 event in ClickHouse")
    assert rows[0]["event_id"] == event["event_id"]
    assert rows[0]["progress_seconds"] == 77
    assert rows[0]["search_query"] is None


def test_malformed_message_goes_to_dead_letter_table(created_users):
    """Сообщение не в формате Avro Confluent не останавливает приём: оно попадает в movie_events_dlq."""
    marker = f"it-garbage-{uuid.uuid4().hex}"
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "acks": "all"})
    producer.produce(TOPIC, key=marker.encode(), value=json.dumps({"marker": marker}).encode())
    assert producer.flush(15) == 0

    dlq = wait_until(
        lambda: ch_query(
            "SELECT kafka_key, raw_message, error FROM cinema.movie_events_dlq WHERE kafka_key = {k:String}",
            {"k": marker},
        ),
        timeout=60,
        what="message in DLQ",
    )
    assert marker in dlq[0]["raw_message"]
    assert dlq[0]["error"]

    # Следующее корректное событие проходит как обычно.
    event = make_event()
    created_users.append(event["user_id"])
    publish(event)
    wait_until(lambda: _rows_for_user(event["user_id"]), timeout=60, what="event after malformed message")


def test_clickhouse_schema_matches_avro_schema():
    """Столбцы Kafka-таблицы совпадают с полями последней версии схемы."""
    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_fields = [f["name"] for f in json.loads(sr.get_latest_version(SUBJECT).schema.schema_str)["fields"]]
    columns = ch_query(
        "SELECT name, type FROM system.columns WHERE database = 'cinema' AND table = 'movie_events_queue' ORDER BY position"
    )
    assert [c["name"] for c in columns] == avro_fields
    types = {c["name"]: c["type"] for c in columns}
    assert types["event_id"] == "UUID"
    assert types["timestamp"] == "DateTime64(3, 'UTC')"
    assert types["progress_seconds"] == "Nullable(Int32)"
    assert types["event_type"].startswith("Enum8('VIEW_STARTED' = 1")
