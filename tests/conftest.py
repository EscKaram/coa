"""Общие помощники интеграционных тестов. Тесты работают в сети docker compose и обращаются к сервисам
по именам контейнеров; адреса приходят из окружения (см. сервис tests в docker-compose.yml)."""

import os
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

import boto3
import psycopg
import pytest
import requests
from botocore.config import Config

MOVIE_SERVICE_URL = os.environ["MOVIE_SERVICE_URL"]
AGGREGATION_SERVICE_URL = os.environ["AGGREGATION_SERVICE_URL"]
CLICKHOUSE_URL = os.environ["CLICKHOUSE_URL"]
CLICKHOUSE_AUTH = {"X-ClickHouse-User": os.environ["CLICKHOUSE_USER"], "X-ClickHouse-Key": os.environ["CLICKHOUSE_PASSWORD"]}
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL"]
POSTGRES_DSN = os.environ["POSTGRES_DSN"]
S3_BUCKET = os.environ["S3_BUCKET"]
GRAFANA_URL = os.environ["GRAFANA_URL"]
TOPIC = "movie-events"
SUBJECT = "movie-events-value"


# ---------- ClickHouse по HTTP ----------

def _ch_params(params: dict[str, Any] | None) -> dict[str, str]:
    out = {}
    for name, value in (params or {}).items():
        if isinstance(value, (list, tuple)):
            value = "[" + ",".join("'" + str(v).replace("'", "\\'") + "'" for v in value) + "]"
        out[f"param_{name}"] = str(value)
    return out


def ch_query(sql: str, params: dict[str, Any] | None = None, auth: dict[str, str] | None = None) -> list[dict]:
    response = requests.post(
        CLICKHOUSE_URL,
        params={**_ch_params(params), "output_format_json_quote_64bit_integers": "0"},
        data=(sql + "\nFORMAT JSON").encode(),
        headers=auth or CLICKHOUSE_AUTH,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["data"]


def ch_exec(sql: str, params: dict[str, Any] | None = None) -> None:
    response = requests.post(
        CLICKHOUSE_URL,
        params={**_ch_params(params), "mutations_sync": "2"},
        data=sql.encode(),
        headers=CLICKHOUSE_AUTH,
        timeout=60,
    )
    assert response.status_code == 200, response.text


def wait_until(check: Callable[[], Any], timeout: float = 60, interval: float = 1, what: str = "condition") -> Any:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = check()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"timed out after {timeout}s waiting for {what}; last value: {last!r}")


# ---------- события ----------

def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def make_event(**overrides: Any) -> dict[str, Any]:
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": f"it-user-{uuid.uuid4().hex[:12]}",
        "movie_id": "it-movie-1",
        "event_type": "VIEW_STARTED",
        "timestamp": iso(datetime.now(timezone.utc).replace(microsecond=0)),
        "device_type": "TV",
        "session_id": f"it-session-{uuid.uuid4().hex[:12]}",
        "progress_seconds": 0,
    }
    event.update(overrides)
    return {k: v for k, v in event.items() if v is not None}


def post_event(event: dict[str, Any]) -> requests.Response:
    return requests.post(f"{MOVIE_SERVICE_URL}/api/v1/events", json=event, timeout=30)


def publish(event: dict[str, Any]) -> dict[str, Any]:
    response = post_event(event)
    assert response.status_code == 202, response.text
    return response.json()


def delete_users(user_ids: list[str]) -> None:
    if user_ids:
        ch_exec("DELETE FROM cinema.movie_events WHERE user_id IN {ids:Array(String)}", {"ids": user_ids})
        ch_exec("DELETE FROM cinema.user_daily_activity WHERE user_id IN {ids:Array(String)}", {"ids": user_ids})


@pytest.fixture
def created_users():
    """Пользователи, созданные тестом; их события удаляются из ClickHouse после теста."""
    users: list[str] = []
    yield users
    delete_users(users)


# ---------- клиенты ----------

def pg_connect() -> psycopg.Connection:
    return psycopg.connect(POSTGRES_DSN, autocommit=True)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        region_name="us-east-1",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


# ---------- детерминированный набор данных для агрегатов (п. 5, 7) ----------
# Изолированные даты в прошлом: генератор и плановые циклы их не трогают. Перед загрузкой и после тестов
# данные этих дат удаляются из ClickHouse, PostgreSQL и S3, поэтому тесты можно запускать многократно.

T0 = date(2001, 3, 1)
TEST_RANGE = (T0 - timedelta(days=7), T0 + timedelta(days=10))


def _ts(day: int, hh: int, mm: int = 0) -> str:
    return iso(datetime(2001, 3, 1, hh, mm, tzinfo=timezone.utc) + timedelta(days=day))


# (пользователь, фильм, тип, время, сессия, progress_seconds, search_query)
AGG_EVENTS = [
    # День 0: когорта из A, B, C, D.
    ("A", "it-m1", "VIEW_STARTED", _ts(0, 10, 0), "s-A0", 0, None),
    ("A", "it-m1", "VIEW_PAUSED", _ts(0, 10, 10), "s-A0", 600, None),
    ("A", "it-m1", "VIEW_RESUMED", _ts(0, 10, 15), "s-A0", 600, None),
    ("A", "it-m1", "VIEW_FINISHED", _ts(0, 10, 25), "s-A0", 1200, None),
    ("A", "it-m1", "LIKED", _ts(0, 10, 26), "s-A0", None, None),
    ("B", "it-m1", "VIEW_STARTED", _ts(0, 11, 0), "s-B0", 0, None),
    ("B", "it-m1", "VIEW_FINISHED", _ts(0, 11, 30), "s-B0", 1800, None),
    ("C", "it-m2", "VIEW_STARTED", _ts(0, 12, 0), "s-C0", 0, None),
    ("C", "it-m2", "VIEW_PAUSED", _ts(0, 12, 5), "s-C0", 300, None),
    ("D", "it-m3", "SEARCHED", _ts(0, 13, 0), "s-D0", None, "golden"),
    # День 1: вернулись A и C; новый пользователь E (когорта дня 1).
    ("A", "it-m2", "VIEW_STARTED", _ts(1, 9, 0), "s-A1", 0, None),
    ("A", "it-m2", "VIEW_FINISHED", _ts(1, 9, 40), "s-A1", 2400, None),
    ("C", "it-m1", "SEARCHED", _ts(1, 20, 0), "s-C1", None, "river"),
    ("E", "it-m1", "VIEW_STARTED", _ts(1, 21, 0), "s-E1", 0, None),
    # День 7: вернулись B (день 7 когорты 0) и E (день 6 когорты 1).
    ("B", "it-m3", "VIEW_STARTED", _ts(7, 18, 0), "s-B7", 0, None),
    ("E", "it-m3", "VIEW_STARTED", _ts(7, 19, 0), "s-E7", 0, None),
]
AGG_USERS = sorted({f"it-agg-{e[0]}" for e in AGG_EVENTS})


def clean_aggregation_dataset() -> None:
    lo, hi = TEST_RANGE
    params = {"lo": lo.isoformat(), "hi": hi.isoformat()}
    ch_exec("DELETE FROM cinema.movie_events WHERE event_date BETWEEN {lo:Date} AND {hi:Date}", params)
    ch_exec("DELETE FROM cinema.user_daily_activity WHERE activity_date BETWEEN {lo:Date} AND {hi:Date}", params)
    ch_exec("DELETE FROM cinema.agg_daily_metrics WHERE metric_date BETWEEN {lo:Date} AND {hi:Date}", params)
    ch_exec("DELETE FROM cinema.agg_top_movies WHERE metric_date BETWEEN {lo:Date} AND {hi:Date}", params)
    ch_exec("DELETE FROM cinema.agg_retention_cohort WHERE cohort_date BETWEEN {lo:Date} AND {hi:Date}", params)
    with pg_connect() as conn:
        conn.execute("DELETE FROM daily_metrics WHERE metric_date BETWEEN %s AND %s", (lo, hi))
        conn.execute("DELETE FROM daily_top_movies WHERE metric_date BETWEEN %s AND %s", (lo, hi))
        conn.execute("DELETE FROM retention_cohorts WHERE cohort_date BETWEEN %s AND %s", (lo, hi))
    s3 = s3_client()
    for day in range((hi - lo).days + 1):
        s3.delete_object(Bucket=S3_BUCKET, Key=f"daily/{(lo + timedelta(days=day)).isoformat()}/aggregates.json")


def recompute(day: date) -> dict:
    response = requests.post(
        f"{AGGREGATION_SERVICE_URL}/api/v1/aggregations/recompute", params={"date": day.isoformat()}, timeout=60
    )
    assert response.status_code == 200, response.text
    return response.json()


@pytest.fixture(scope="session")
def aggregation_dataset():
    """Публикует AGG_EVENTS через HTTP API продюсера, ждёт их в ClickHouse и пересчитывает дни 0, 1, 7."""
    clean_aggregation_dataset()
    for user, movie, event_type, ts, session, progress, query in AGG_EVENTS:
        publish(
            make_event(
                user_id=f"it-agg-{user}",
                movie_id=movie,
                event_type=event_type,
                timestamp=ts,
                session_id=f"it-agg-{session}",
                progress_seconds=progress,
                search_query=query,
                device_type="MOBILE",
            )
        )
    lo, hi = TEST_RANGE
    wait_until(
        lambda: ch_query(
            "SELECT count() AS n FROM cinema.movie_events FINAL WHERE event_date BETWEEN {lo:Date} AND {hi:Date}",
            {"lo": lo.isoformat(), "hi": hi.isoformat()},
        )[0]["n"]
        == len(AGG_EVENTS),
        timeout=60,
        what="test events in ClickHouse",
    )
    runs = {offset: recompute(T0 + timedelta(days=offset)) for offset in (0, 1, 7)}
    yield runs
    clean_aggregation_dataset()
