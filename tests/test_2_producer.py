"""П. 2: HTTP API продюсера (валидация, ответ с event_id) и генератор событий."""

import uuid

import pytest
import requests

from conftest import MOVIE_SERVICE_URL, ch_query, make_event, post_event, publish


def test_valid_event_is_published(created_users):
    event = make_event()
    created_users.append(event["user_id"])
    body = publish(event)
    assert body["event_id"] == event["event_id"]
    assert body["topic"] == "movie-events"
    assert body["partition"] in (0, 1, 2)
    assert body["offset"] >= 0
    assert body["schema_version"] == 2


def test_event_id_and_timestamp_are_generated(created_users):
    event = make_event()
    del event["event_id"], event["timestamp"]
    created_users.append(event["user_id"])
    body = publish(event)
    assert uuid.UUID(body["event_id"])


@pytest.mark.parametrize(
    "overrides, field",
    [
        ({"user_id": None}, "user_id"),
        ({"user_id": ""}, "user_id"),
        ({"event_type": "VIEW_REWOUND"}, "event_type"),
        ({"device_type": "WATCH"}, "device_type"),
        ({"event_id": "not-a-uuid"}, "event_id"),
        ({"timestamp": "2026-11-25T10:00:00"}, "timestamp"),  # без часового пояса
        ({"timestamp": "2099-01-01T00:00:00Z"}, "timestamp"),  # из будущего
        ({"progress_seconds": -5}, "progress_seconds"),
        ({"progress_seconds": 2**31}, "progress_seconds"),  # не помещается в Avro int
        ({"event_type": "VIEW_FINISHED", "progress_seconds": None}, "progress_seconds"),
        ({"event_type": "LIKED", "progress_seconds": 10}, "progress_seconds"),
        ({"search_query": "abc"}, "search_query"),  # только для SEARCHED
        ({"rating": 5}, "rating"),  # поля нет в схеме
    ],
)
def test_invalid_event_is_rejected(overrides, field):
    # make_event отбрасывает поля со значением None, так моделируется отсутствующее поле.
    response = post_event(make_event(**overrides))
    assert response.status_code == 422, response.text
    body = response.json()
    assert body["error"] == "VALIDATION_ERROR"
    assert any(d["field"] == field for d in body["details"]), body


def test_not_json_body_is_rejected():
    response = requests.post(
        f"{MOVIE_SERVICE_URL}/api/v1/events", data="not json", headers={"Content-Type": "application/json"}, timeout=10
    )
    assert response.status_code == 422


def test_generator_is_running():
    status = requests.get(f"{MOVIE_SERVICE_URL}/api/v1/generator", timeout=10).json()
    assert status["enabled"] is True
    assert status["mode"] == "realtime"
    assert status["published"] > 0
    assert status["failed"] == 0


def test_generator_sessions_are_realistic():
    """В сессиях генератора VIEW_FINISHED всегда после VIEW_STARTED, progress_seconds не убывает."""
    sessions = ch_query("SELECT uniqExact(session_id) AS n FROM cinema.movie_events WHERE user_id LIKE 'u-%'")
    assert sessions[0]["n"] > 100

    finished_without_start = ch_query(
        """
        SELECT count() AS n FROM
        (
            SELECT
                session_id,
                countIf(event_type = 'VIEW_STARTED') AS starts,
                minIf(timestamp, event_type = 'VIEW_STARTED') AS started_at,
                maxIf(timestamp, event_type = 'VIEW_FINISHED') AS finished_at,
                countIf(event_type = 'VIEW_FINISHED') AS finishes
            FROM cinema.movie_events FINAL
            WHERE user_id LIKE 'u-%'
            GROUP BY session_id
        )
        WHERE finishes > 0 AND (starts = 0 OR started_at > finished_at)
        """
    )
    assert finished_without_start[0]["n"] == 0

    decreasing_progress = ch_query(
        """
        SELECT count() AS n FROM
        (
            SELECT arrayMap(x -> x.2, arraySort(x -> x.1,
                       groupArrayIf((timestamp, assumeNotNull(progress_seconds)), progress_seconds IS NOT NULL))) AS p
            FROM cinema.movie_events FINAL
            WHERE user_id LIKE 'u-%'
            GROUP BY session_id
        )
        WHERE p != arraySort(p)
        """
    )
    assert decreasing_progress[0]["n"] == 0

    started_not_at_zero = ch_query(
        """
        SELECT count() AS n FROM cinema.movie_events
        WHERE user_id LIKE 'u-%' AND event_type = 'VIEW_STARTED' AND progress_seconds != 0
        """
    )
    assert started_not_at_zero[0]["n"] == 0
