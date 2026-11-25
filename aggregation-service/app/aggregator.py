"""Цикл агрегации: SELECT из ClickHouse (сырые события) → агрегаты в ClickHouse → UPSERT в PostgreSQL."""

import logging
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import clickhouse_connect

from . import queries
from .config import Settings
from .retry import with_retry
from .storage import Aggregates, MetricsStore

logger = logging.getLogger("aggregation.cycle")


@dataclass(frozen=True)
class CycleResult:
    trigger: str
    date_from: date
    date_to: date
    raw_events: int
    rows_read: int
    rows_written: int
    computed_at: datetime
    duration_seconds: float

    def as_dict(self) -> dict:
        return {
            "trigger": self.trigger,
            "date_from": self.date_from.isoformat(),
            "date_to": self.date_to.isoformat(),
            "raw_events": self.raw_events,
            "clickhouse_rows_read": self.rows_read,
            "postgres_rows_written": self.rows_written,
            "computed_at": self.computed_at.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "duration_seconds": self.duration_seconds,
        }


class Aggregator:
    def __init__(self, settings: Settings, store: MetricsStore):
        self._s = settings
        self._store = store
        # Плановый и ручной пересчёты не выполняются одновременно.
        self.lock = threading.Lock()
        self._first_scheduled_run = True

    def _client(self):
        return clickhouse_connect.get_client(
            host=self._s.clickhouse_host,
            port=self._s.clickhouse_port,
            username=self._s.clickhouse_user,
            password=self._s.clickhouse_password,
            database=self._s.clickhouse_database,
            connect_timeout=5,
            send_receive_timeout=120,
        )

    def run(self, date_from: date, date_to: date, trigger: str) -> CycleResult:
        with self.lock:
            return self._run(date_from, date_to, trigger)

    def _run(self, date_from: date, date_to: date, trigger: str) -> CycleResult:
        started = time.monotonic()
        # Точность computed_at — миллисекунды, как у столбца DateTime64(3) в ClickHouse.
        now = datetime.now(timezone.utc)
        computed_at = now.replace(microsecond=now.microsecond // 1000 * 1000)
        today = now.date()
        # Retention D7 за день D зависит от когорты D-7, поэтому когорты пересчитываются на 7 дней раньше.
        cohort_from, cohort_to = date_from - timedelta(days=7), date_to
        logger.info(
            "aggregation cycle started",
            extra={"trigger": trigger, "date_from": date_from.isoformat(), "date_to": date_to.isoformat()},
        )

        client = self._client()
        try:
            params = {
                "date_from": date_from,
                "date_to": date_to,
                "cohort_from": cohort_from,
                "cohort_to": cohort_to,
                "today": today,
                "computed_at": computed_at,
                "limit": self._s.top_movies_limit,
            }
            raw_events = int(client.command(queries.RAW_EVENTS_IN_RANGE, parameters=params))
            rows_read = 0
            for sql in (queries.DAILY_METRICS, queries.TOP_MOVIES, queries.RETENTION_COHORTS, queries.RETENTION_METRICS):
                summary = client.command(sql, parameters=params)
                rows_read += int(getattr(summary, "summary", {}).get("read_rows", 0) or 0)

            agg = Aggregates(
                date_from=date_from,
                date_to=date_to,
                cohort_from=cohort_from,
                cohort_to=cohort_to,
                computed_at=computed_at,
                daily_metrics=[tuple(r) for r in client.query(queries.READ_DAILY_METRICS, parameters=params).result_rows],
                top_movies=[tuple(r) for r in client.query(queries.READ_TOP_MOVIES, parameters=params).result_rows],
                cohorts=[tuple(r) for r in client.query(queries.READ_RETENTION_COHORTS, parameters=params).result_rows],
            )
        finally:
            client.close()

        with_retry(
            lambda: self._store.save(agg),
            what="postgres write",
            max_attempts=self._s.write_max_attempts,
            base_ms=self._s.write_retry_base_ms,
            max_ms=self._s.write_retry_max_ms,
        )

        result = CycleResult(
            trigger=trigger,
            date_from=date_from,
            date_to=date_to,
            raw_events=raw_events,
            rows_read=rows_read,
            rows_written=agg.rows,
            computed_at=computed_at,
            duration_seconds=round(time.monotonic() - started, 3),
        )
        logger.info(
            f"aggregation cycle finished: processed {raw_events} events, "
            f"wrote {agg.rows} rows in {result.duration_seconds} s",
            extra=result.as_dict(),
        )
        return result

    def scheduled(self) -> CycleResult:
        today = datetime.now(timezone.utc).date()
        lookback = self._s.agg_initial_lookback_days if self._first_scheduled_run else self._s.agg_lookback_days
        result = self.run(today - timedelta(days=lookback), today, trigger="schedule")
        self._first_scheduled_run = False
        return result
