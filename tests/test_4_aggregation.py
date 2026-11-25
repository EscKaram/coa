"""П. 5: сервис агрегации — метрики на детерминированном наборе событий (conftest.AGG_EVENTS),
материализация в ClickHouse, идемпотентная выгрузка в PostgreSQL."""

from datetime import timedelta

import pytest
import requests

from conftest import AGGREGATION_SERVICE_URL, T0, ch_query, pg_connect, recompute

D0, D1, D7 = T0, T0 + timedelta(days=1), T0 + timedelta(days=7)

# Ожидаемые значения выведены вручную из AGG_EVENTS (см. комментарии в conftest.py).
EXPECTED = {
    D0: {
        "dau": 4, "events_total": 10, "sessions": 4, "views_started": 3, "views_finished": 2,
        "likes": 1, "searches": 1, "view_conversion": 2 / 3, "avg_watch_time_seconds": 1500,
    },
    D1: {
        "dau": 3, "events_total": 4, "sessions": 3, "views_started": 2, "views_finished": 1,
        "likes": 0, "searches": 1, "view_conversion": 0.5, "avg_watch_time_seconds": 2400,
        "retention_d1": 0.5,  # когорта D0 = {A, B, C, D}, в D1 вернулись A и C
    },
    D7: {
        "dau": 2, "events_total": 2, "sessions": 2, "views_started": 2, "views_finished": 0,
        "likes": 0, "searches": 0, "view_conversion": 0.0, "avg_watch_time_seconds": None,
        "retention_d7": 0.25,  # из когорты D0 в D7 вернулся только B
    },
}
EXPECTED_COHORTS = {
    D0: [4, 2, 0, 0, 0, 0, 0, 1],  # размер 4
    D1: [1, 0, 0, 0, 0, 0, 1, 0],  # размер 1: E вернулся на 6-й день (D7)
}


def _pg_metrics(day):
    with pg_connect() as conn:
        rows = conn.execute(
            "SELECT metric_name, value, computed_at FROM daily_metrics WHERE metric_date = %s", (day,)
        ).fetchall()
    return {name: (value, computed_at) for name, value, computed_at in rows}


@pytest.mark.parametrize("day", [D0, D1, D7])
def test_daily_metrics_in_postgres(aggregation_dataset, day):
    metrics = _pg_metrics(day)
    assert set(metrics) == set(EXPECTED[day])
    for name, expected in EXPECTED[day].items():
        value = metrics[name][0]
        if expected is None:
            assert value is None, name
        else:
            assert value == pytest.approx(expected), name


def test_cycle_reports_processed_events(aggregation_dataset):
    run = aggregation_dataset[0]
    assert run["raw_events"] == 10
    assert run["postgres_rows_written"] > 0
    assert run["duration_seconds"] >= 0


def test_top_movies(aggregation_dataset):
    with pg_connect() as conn:
        rows = conn.execute(
            "SELECT rank, movie_id, views, unique_viewers, finishes, likes FROM daily_top_movies "
            "WHERE metric_date = %s ORDER BY rank",
            (D0,),
        ).fetchall()
    # it-m3 только искали (SEARCHED) — в рейтинг просмотров он не входит.
    assert rows == [(1, "it-m1", 2, 2, 2, 1), (2, "it-m2", 1, 1, 0, 0)]


def test_retention_cohorts(aggregation_dataset):
    with pg_connect() as conn:
        for cohort, retained in EXPECTED_COHORTS.items():
            rows = conn.execute(
                "SELECT day_number, cohort_size, retained_users, retention_rate FROM retention_cohorts "
                "WHERE cohort_date = %s ORDER BY day_number",
                (cohort,),
            ).fetchall()
            assert [r[0] for r in rows] == list(range(8))
            assert {r[1] for r in rows} == {retained[0]}
            assert [r[2] for r in rows] == retained
            assert [r[3] for r in rows] == pytest.approx([n / retained[0] for n in retained])


def test_aggregates_materialized_in_clickhouse(aggregation_dataset):
    rows = ch_query(
        "SELECT metric_name, value FROM cinema.agg_daily_metrics FINAL WHERE metric_date = {d:Date}",
        {"d": D1.isoformat()},
    )
    assert {r["metric_name"]: r["value"] for r in rows}["retention_d1"] == pytest.approx(0.5)
    heatmap = ch_query(
        "SELECT day_number, retention_rate FROM cinema.agg_retention_cohort FINAL "
        "WHERE cohort_date = {d:Date} ORDER BY day_number",
        {"d": D0.isoformat()},
    )
    assert [r["retention_rate"] for r in heatmap] == pytest.approx([1, 0.5, 0, 0, 0, 0, 0, 0.25])


def test_recompute_is_idempotent(aggregation_dataset):
    """Повторный пересчёт за ту же дату обновляет значения и время вычисления, а не добавляет строки."""
    before = _pg_metrics(D0)
    with pg_connect() as conn:
        counts_before = conn.execute(
            "SELECT (SELECT count(*) FROM daily_metrics WHERE metric_date = %s), "
            "(SELECT count(*) FROM daily_top_movies WHERE metric_date = %s)",
            (D0, D0),
        ).fetchone()

    recompute(D0)
    recompute(D0)

    after = _pg_metrics(D0)
    with pg_connect() as conn:
        counts_after = conn.execute(
            "SELECT (SELECT count(*) FROM daily_metrics WHERE metric_date = %s), "
            "(SELECT count(*) FROM daily_top_movies WHERE metric_date = %s)",
            (D0, D0),
        ).fetchone()
    assert counts_after == counts_before
    assert {k: v[0] for k, v in after.items()} == {k: v[0] for k, v in before.items()}
    assert all(after[k][1] > before[k][1] for k in after)

    ch_rows = ch_query(
        "SELECT count() AS n FROM cinema.agg_daily_metrics FINAL WHERE metric_date = {d:Date}", {"d": D0.isoformat()}
    )
    assert ch_rows[0]["n"] == len(EXPECTED[D0])


def test_recompute_rejects_bad_date():
    response = requests.post(
        f"{AGGREGATION_SERVICE_URL}/api/v1/aggregations/recompute", params={"date": "2001-02-30"}, timeout=10
    )
    assert response.status_code == 422
