"""PostgreSQL — хранилище готовых метрик. Запись идемпотентна: пересчёт за ту же дату обновляет строки."""

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

logger = logging.getLogger("aggregation.storage")


@dataclass
class Aggregates:
    """Результат одного цикла для диапазона дат."""

    date_from: date
    date_to: date
    cohort_from: date
    cohort_to: date
    computed_at: datetime
    daily_metrics: list[tuple[date, str, float | None]]
    top_movies: list[tuple[date, int, str, int, int, int, int]]
    cohorts: list[tuple[date, int, int, int]]

    @property
    def rows(self) -> int:
        return len(self.daily_metrics) + len(self.top_movies) + len(self.cohorts)


UPSERT_METRIC = """
INSERT INTO daily_metrics (metric_date, metric_name, value, computed_at)
VALUES (%s, %s, %s, %s)
ON CONFLICT (metric_date, metric_name)
DO UPDATE SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at
"""

INSERT_TOP_MOVIE = """
INSERT INTO daily_top_movies (metric_date, rank, movie_id, views, unique_viewers, finishes, likes, computed_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

UPSERT_COHORT = """
INSERT INTO retention_cohorts (cohort_date, day_number, cohort_size, retained_users, computed_at)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (cohort_date, day_number)
DO UPDATE SET cohort_size = EXCLUDED.cohort_size,
              retained_users = EXCLUDED.retained_users,
              computed_at = EXCLUDED.computed_at
"""


class MetricsStore:
    def __init__(self, dsn: str):
        self._dsn = dsn

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self._dsn, connect_timeout=5, autocommit=False)

    def save(self, agg: Aggregates) -> None:
        """Одна транзакция на цикл: либо все результаты цикла видны, либо ни один."""
        with self._connect() as conn, conn.cursor() as cur:
            ts = agg.computed_at
            cur.executemany(UPSERT_METRIC, [(d, name, value, ts) for d, name, value in agg.daily_metrics])
            # Топ дня заменяется целиком: состав и порядок фильмов между пересчётами могут поменяться.
            cur.execute(
                "DELETE FROM daily_top_movies WHERE metric_date BETWEEN %s AND %s", (agg.date_from, agg.date_to)
            )
            cur.executemany(INSERT_TOP_MOVIE, [(*row, ts) for row in agg.top_movies])
            cur.executemany(UPSERT_COHORT, [(*row, ts) for row in agg.cohorts if row[2] > 0])
            conn.commit()

    def load_day(self, day: date) -> dict[str, Any]:
        with self._connect() as conn, conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT dm.metric_name AS name, dm.value, m.unit, dm.computed_at
                FROM daily_metrics dm JOIN metrics m ON m.name = dm.metric_name
                WHERE dm.metric_date = %s
                ORDER BY dm.metric_name
                """,
                (day,),
            )
            metrics = cur.fetchall()
            cur.execute(
                """
                SELECT rank, movie_id, views, unique_viewers, finishes, likes, computed_at
                FROM daily_top_movies WHERE metric_date = %s ORDER BY rank
                """,
                (day,),
            )
            top = cur.fetchall()
            cur.execute(
                """
                SELECT day_number, cohort_size, retained_users, retention_rate, computed_at
                FROM retention_cohorts WHERE cohort_date = %s ORDER BY day_number
                """,
                (day,),
            )
            cohort = cur.fetchall()
        return {"metrics": metrics, "top_movies": top, "retention_cohort": cohort}

    def dates_with_metrics(self, date_from: date, date_to: date) -> list[date]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT metric_date FROM daily_metrics WHERE metric_date BETWEEN %s AND %s ORDER BY 1",
                (date_from, date_to),
            )
            return [row[0] for row in cur.fetchall()]
