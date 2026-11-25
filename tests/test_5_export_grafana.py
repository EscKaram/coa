"""П. 6–7: дашборд Grafana поверх агрегатов и экспорт агрегатов в S3 (MinIO)."""

import json
import time

import pytest
import requests

from conftest import (
    AGGREGATION_SERVICE_URL,
    GRAFANA_URL,
    S3_BUCKET,
    T0,
    ch_query,
    s3_client,
)

GRAFANA_AUTH = ("admin", "admin")


def _export(day):
    return requests.post(f"{AGGREGATION_SERVICE_URL}/api/v1/exports", params={"date": day.isoformat()}, timeout=30)


# ---------- S3 ----------

def test_export_writes_daily_file(aggregation_dataset):
    response = _export(T0)
    assert response.status_code == 200, response.text
    body = response.json()
    key = f"daily/{T0.isoformat()}/aggregates.json"
    assert body["uri"] == f"s3://{S3_BUCKET}/{key}"
    assert body["status"] == "exported"

    obj = s3_client().get_object(Bucket=S3_BUCKET, Key=key)
    assert obj["ContentType"] == "application/json"
    document = json.loads(obj["Body"].read())
    assert document["metric_date"] == T0.isoformat()
    assert document["exported_at"].endswith("Z") and document["computed_at"].endswith("Z")
    metrics = document["metrics"]
    assert metrics["dau"]["value"] == 4
    assert metrics["view_conversion"]["value"] == pytest.approx(2 / 3)
    assert metrics["avg_watch_time_seconds"]["value"] == 1500
    assert metrics["avg_watch_time_seconds"]["unit"] == "seconds"
    assert all(m["computed_at"].endswith("Z") for m in metrics.values())
    assert [m["movie_id"] for m in document["top_movies"]] == ["it-m1", "it-m2"]
    assert [d["retained_users"] for d in document["retention_cohort"]["days"]] == [4, 2, 0, 0, 0, 0, 0, 1]


def test_repeated_export_overwrites_file(aggregation_dataset):
    s3 = s3_client()
    prefix = f"daily/{T0.isoformat()}/"
    assert _export(T0).status_code == 200
    first = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=prefix + "aggregates.json")["Body"].read())
    time.sleep(0.01)
    assert _export(T0).status_code == 200
    second = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=prefix + "aggregates.json")["Body"].read())

    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix).get("Contents", [])]
    assert keys == [prefix + "aggregates.json"]
    assert second["exported_at"] > first["exported_at"]
    assert second["metrics"] == first["metrics"]


def test_export_without_aggregates_returns_404():
    response = _export(type(T0)(1999, 1, 1))
    assert response.status_code == 404
    assert response.json()["error"] == "NO_AGGREGATES"


def test_scheduled_export_created_files_for_closed_days():
    """Плановый экспорт (EXPORT_INTERVAL_SECONDS) выгружает закрытые дни, по которым есть агрегаты."""
    keys = [o["Key"] for o in s3_client().list_objects_v2(Bucket=S3_BUCKET, Prefix="daily/").get("Contents", [])]
    generated_days = [k for k in keys if not k.startswith("daily/2001-")]
    assert generated_days, keys
    assert all(k.endswith("/aggregates.json") for k in generated_days)


# ---------- Grafana ----------

def _dashboard():
    response = requests.get(f"{GRAFANA_URL}/api/dashboards/uid/online-cinema", auth=GRAFANA_AUTH, timeout=10)
    assert response.status_code == 200, response.text
    return response.json()["dashboard"]


def test_grafana_datasource_is_clickhouse():
    response = requests.get(f"{GRAFANA_URL}/api/datasources/uid/clickhouse", auth=GRAFANA_AUTH, timeout=10)
    assert response.status_code == 200
    assert response.json()["type"] == "grafana-clickhouse-datasource"
    health = requests.get(f"{GRAFANA_URL}/api/datasources/uid/clickhouse/health", auth=GRAFANA_AUTH, timeout=30)
    assert health.status_code == 200, health.text


def test_dashboard_panels_use_only_aggregates():
    panels = _dashboard()["panels"]
    titles = [p["title"] for p in panels]
    assert "Retention Cohort Heatmap" in titles
    assert len(panels) >= 3
    for panel in panels:
        sql = panel["targets"][0]["rawSql"]
        assert "cinema.agg_" in sql, panel["title"]
        assert "movie_events" not in sql, panel["title"]


def test_dashboard_queries_return_data():
    now = int(time.time() * 1000)
    for panel in _dashboard()["panels"]:
        query = {**panel["targets"][0], "datasource": {"uid": "clickhouse", "type": "grafana-clickhouse-datasource"}}
        response = requests.post(
            f"{GRAFANA_URL}/api/ds/query",
            json={"queries": [query], "from": str(now - 14 * 86_400_000), "to": str(now)},
            auth=GRAFANA_AUTH,
            timeout=30,
        )
        assert response.status_code == 200, (panel["title"], response.text)
        frame = response.json()["results"]["A"]["frames"][0]
        assert frame["data"]["values"] and frame["data"]["values"][0], panel["title"]
        if panel["title"] == "Retention Cohort Heatmap":
            names = [f["name"] for f in frame["schema"]["fields"]]
            assert names[:2] == ["Когорта", "Пользователей"]
            assert names[2:] == [f"Day {k}" for k in range(8)]
            assert all(v == 1 for v in frame["data"]["values"][2])  # Day 0 = 100%


def test_grafana_user_cannot_read_raw_events():
    auth = {"X-ClickHouse-User": "grafana", "X-ClickHouse-Key": "grafana"}
    assert ch_query("SELECT count() AS n FROM cinema.agg_daily_metrics", auth=auth)[0]["n"] >= 0
    with pytest.raises(requests.HTTPError):
        ch_query("SELECT count() FROM cinema.movie_events", auth=auth)
