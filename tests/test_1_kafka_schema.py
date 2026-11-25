"""П. 1 и 8: топик movie-events, схема в Schema Registry, кластер Kafka с репликацией."""

import json
import struct

import requests
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from conftest import KAFKA_BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, SUBJECT, TOPIC, make_event, publish

REQUIRED_FIELDS = {
    "event_id", "user_id", "movie_id", "event_type", "timestamp", "device_type", "session_id", "progress_seconds",
}


def _admin() -> AdminClient:
    return AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def test_cluster_has_two_brokers():
    brokers = _admin().list_topics(timeout=10).brokers
    assert sorted(brokers) == [1, 2]


def test_topic_partitions_and_replication():
    admin = _admin()
    topic = admin.list_topics(TOPIC, timeout=10).topics[TOPIC]
    assert topic.error is None
    assert len(topic.partitions) >= 3
    for partition in topic.partitions.values():
        assert len(partition.replicas) >= 2, partition
        assert sorted(partition.isrs) == sorted(partition.replicas), "все реплики в ISR"

    resource = ConfigResource(ConfigResource.Type.TOPIC, TOPIC)
    config = admin.describe_configs([resource])[resource].result(timeout=10)
    assert int(config["min.insync.replicas"].value) >= 1


def test_schema_registered_with_versions():
    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    assert sr.get_versions(SUBJECT) == [1, 2]
    assert sr.get_compatibility(SUBJECT) == "BACKWARD"

    v1 = json.loads(sr.get_version(SUBJECT, 1).schema.schema_str)
    v2 = json.loads(sr.get_version(SUBJECT, 2).schema.schema_str)
    assert {f["name"] for f in v1["fields"]} == REQUIRED_FIELDS
    assert {f["name"] for f in v2["fields"]} == REQUIRED_FIELDS | {"search_query"}

    fields = {f["name"]: f["type"] for f in v1["fields"]}
    assert fields["event_id"] == {"type": "string", "logicalType": "uuid"}
    assert fields["timestamp"] == {"type": "long", "logicalType": "timestamp-millis"}
    assert fields["event_type"]["symbols"] == [
        "VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED", "VIEW_RESUMED", "LIKED", "SEARCHED",
    ]
    assert fields["device_type"]["symbols"] == ["MOBILE", "DESKTOP", "TV", "TABLET"]
    assert fields["progress_seconds"] == ["null", "int"]


def test_incompatible_schema_change_is_rejected():
    """BACKWARD: новая версия должна читать старые данные. Новое обязательное поле без default — нарушение."""
    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    latest = json.loads(sr.get_latest_version(SUBJECT).schema.schema_str)
    latest["fields"].append({"name": "rating", "type": "int"})
    assert sr.test_compatibility(SUBJECT, Schema(json.dumps(latest), "AVRO")) is False

    response = requests.post(
        f"{SCHEMA_REGISTRY_URL}/subjects/{SUBJECT}/versions",
        json={"schema": json.dumps(latest)},
        timeout=10,
    )
    assert response.status_code == 409
    assert sr.get_versions(SUBJECT) == [1, 2]


def test_message_is_avro_confluent_keyed_by_user(created_users):
    """Сообщение в топике: ключ = user_id, значение = магический байт + id схемы + Avro."""
    event = make_event(event_type="SEARCHED", progress_seconds=None, search_query="silent river")
    created_users.append(event["user_id"])
    result = publish(event)

    consumer = Consumer(
        {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "group.id": "it-reader", "enable.auto.commit": False}
    )
    try:
        consumer.assign([TopicPartition(TOPIC, result["partition"], result["offset"])])
        message = consumer.poll(15)
    finally:
        consumer.close()
    assert message is not None and message.error() is None
    assert message.key() == event["user_id"].encode()

    magic, schema_id = struct.unpack(">bI", message.value()[:5])
    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    assert magic == 0
    assert schema_id == sr.get_latest_version(SUBJECT).schema_id

    decoded = AvroDeserializer(sr)(message.value(), SerializationContext(TOPIC, MessageField.VALUE))
    assert str(decoded["event_id"]) == event["event_id"]
    assert decoded["event_type"] == "SEARCHED"
    assert decoded["search_query"] == "silent river"
    assert decoded["progress_seconds"] is None
