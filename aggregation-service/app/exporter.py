"""Экспорт дневных агрегатов из PostgreSQL в S3 (MinIO): s3://<bucket>/daily/YYYY-MM-DD/aggregates.json.

Один файл на день содержит все метрики дня, топ фильмов и строку когорты этого дня с временем вычисления.
Повторный экспорт пишет по тому же ключу и перезаписывает объект (версионирование бакета выключено).
Плановый экспорт пропускает день, если значения не изменились: в метаданных объекта хранится
SHA-256 значений без отметок времени.
"""

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from .config import Settings
from .storage import MetricsStore

logger = logging.getLogger("aggregation.export")


class NoDataError(Exception):
    """В PostgreSQL нет агрегатов за дату."""


@dataclass(frozen=True)
class ExportResult:
    metric_date: date
    uri: str
    size_bytes: int
    status: str  # "exported" | "unchanged"

    def as_dict(self) -> dict:
        return {
            "metric_date": self.metric_date.isoformat(),
            "uri": self.uri,
            "size_bytes": self.size_bytes,
            "status": self.status,
        }


def _iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class S3Exporter:
    def __init__(self, settings: Settings, store: MetricsStore):
        self._s = settings
        self._store = store
        self._s3 = boto3.client(
            "s3",
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.s3_access_key,
            aws_secret_access_key=settings.s3_secret_key,
            region_name=settings.s3_region,
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                connect_timeout=3,
                read_timeout=10,
                retries={"max_attempts": 2, "mode": "standard"},
            ),
        )

    def key_for(self, day: date) -> str:
        return f"{self._s.s3_prefix}/{day.isoformat()}/aggregates.json"

    def _ensure_bucket(self) -> None:
        try:
            self._s3.head_bucket(Bucket=self._s.s3_bucket)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in {"404", "NoSuchBucket", "NotFound"}:
                raise
            self._s3.create_bucket(Bucket=self._s.s3_bucket)
            logger.info("bucket created", extra={"bucket": self._s.s3_bucket})

    def _document(self, day: date, data: dict[str, Any]) -> tuple[dict[str, Any], str]:
        metrics = {
            m["name"]: {"value": m["value"], "unit": m["unit"], "computed_at": _iso(m["computed_at"])}
            for m in data["metrics"]
        }
        top_movies = [{**row, "computed_at": _iso(row["computed_at"])} for row in data["top_movies"]]
        cohort = [{**row, "computed_at": _iso(row["computed_at"])} for row in data["retention_cohort"]]
        values = {
            "metrics": {k: v["value"] for k, v in metrics.items()},
            "top_movies": [{k: v for k, v in r.items() if k != "computed_at"} for r in top_movies],
            "retention_cohort": [{k: v for k, v in r.items() if k != "computed_at"} for r in cohort],
        }
        values_hash = hashlib.sha256(json.dumps(values, sort_keys=True, default=str).encode()).hexdigest()
        document = {
            "metric_date": day.isoformat(),
            "exported_at": _iso(datetime.now(timezone.utc)),
            "computed_at": max(v["computed_at"] for v in metrics.values()),
            "source": "postgresql: daily_metrics, daily_top_movies, retention_cohorts",
            "metrics": metrics,
            "top_movies": top_movies,
            "retention_cohort": {"cohort_date": day.isoformat(), "days": cohort},
        }
        return document, values_hash

    def export(self, day: date, force: bool) -> ExportResult:
        data = self._store.load_day(day)
        if not data["metrics"]:
            raise NoDataError(f"no aggregates in PostgreSQL for {day.isoformat()}")
        document, values_hash = self._document(day, data)
        key = self.key_for(day)
        uri = f"s3://{self._s.s3_bucket}/{key}"
        self._ensure_bucket()

        if not force:
            try:
                head = self._s3.head_object(Bucket=self._s.s3_bucket, Key=key)
                if head.get("Metadata", {}).get("values-sha256") == values_hash:
                    return ExportResult(day, uri, head["ContentLength"], "unchanged")
            except ClientError as exc:
                if exc.response.get("Error", {}).get("Code") not in {"404", "NoSuchKey", "NotFound"}:
                    raise

        body = json.dumps(document, ensure_ascii=False, indent=2, default=str).encode("utf-8")
        self._s3.put_object(
            Bucket=self._s.s3_bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
            Metadata={"values-sha256": values_hash, "metric-date": day.isoformat()},
        )
        logger.info("aggregates exported", extra={"metric_date": day.isoformat(), "uri": uri, "bytes": len(body)})
        return ExportResult(day, uri, len(body), "exported")

    def scheduled(self) -> list[ExportResult]:
        """Выгрузка закрытых дней. Ошибка по дню логируется; следующий запуск повторит этот день."""
        today = datetime.now(timezone.utc).date()
        date_from, date_to = today - timedelta(days=self._s.export_lookback_days), today - timedelta(days=1)
        results: list[ExportResult] = []
        failed: list[str] = []
        for day in self._store.dates_with_metrics(date_from, date_to):
            try:
                results.append(self.export(day, force=False))
            except Exception as exc:  # noqa: BLE001 — день повторится в следующем запуске
                failed.append(day.isoformat())
                logger.error("export failed, will retry on next run", extra={"metric_date": day.isoformat(), "error": repr(exc)})
        logger.info(
            "export run finished",
            extra={
                "exported": [r.metric_date.isoformat() for r in results if r.status == "exported"],
                "unchanged": [r.metric_date.isoformat() for r in results if r.status == "unchanged"],
                "failed": failed,
            },
        )
        return results
