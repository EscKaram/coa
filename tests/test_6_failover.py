"""П. 8: отказ брокера. Запуск: make test-failover — останавливает kafka-1, запускает эти тесты, возвращает kafka-1.

С replication.factor = 2 и min.insync.replicas = 1 оставшийся брокер становится лидером всех партиций,
а продюсер с acks=all продолжает писать (ISR из одной реплики достаточно). Кворум метаданных KRaft
(kafka-2 + kafka-controller, 2 из 3 голосов) сохраняется.
"""

import pytest
from confluent_kafka.admin import AdminClient

from conftest import KAFKA_BOOTSTRAP_SERVERS, TOPIC, ch_query, make_event, publish, wait_until

pytestmark = pytest.mark.failover


def test_partitions_failed_over_to_surviving_broker():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    def leaders_moved():
        metadata = admin.list_topics(TOPIC, timeout=10)
        partitions = metadata.topics[TOPIC].partitions.values()
        return sorted(metadata.brokers) == [2] and all(p.leader == 2 for p in partitions) and metadata

    metadata = wait_until(leaders_moved, timeout=60, what="leaders on broker 2")
    for partition in metadata.topics[TOPIC].partitions.values():
        assert partition.isrs == [2]


def test_pipeline_works_with_one_broker(created_users):
    events = [make_event() for _ in range(10)]
    created_users.extend(e["user_id"] for e in events)
    for event in events:
        publish(event)
    wait_until(
        lambda: ch_query(
            "SELECT count() AS n FROM cinema.movie_events FINAL WHERE user_id IN {ids:Array(String)}",
            {"ids": [e["user_id"] for e in events]},
        )[0]["n"]
        == len(events),
        timeout=120,
        what="events in ClickHouse with one broker",
    )
