"""Aggregation Service: пересчёт бизнес-метрик по расписанию и по HTTP, выгрузка в PostgreSQL и S3."""

import asyncio
import logging
from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import date

import uvicorn
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from .aggregator import Aggregator
from .config import Settings
from .exporter import NoDataError, S3Exporter
from .logging_setup import setup_logging
from .storage import MetricsStore

logger = logging.getLogger("aggregation.api")
settings = Settings.from_env()
setup_logging(settings.log_level)

store = MetricsStore(settings.postgres_dsn)
aggregator = Aggregator(settings, store)
exporter = S3Exporter(settings, store)


async def periodic(name: str, interval_s: int, initial_delay_s: int, job: Callable[[], object]) -> None:
    """Планировщик: запуск job каждые interval_s секунд. Ошибка цикла логируется, следующий цикл выполнится."""
    await asyncio.sleep(initial_delay_s)
    while True:
        try:
            await asyncio.to_thread(job)
        except Exception:  # noqa: BLE001
            logger.exception(f"{name} run failed; will retry on next run")
        await asyncio.sleep(interval_s)


@asynccontextmanager
async def lifespan(_: FastAPI):
    logger.info(
        "scheduler configured",
        extra={
            "aggregation_interval_s": settings.agg_interval_seconds,
            "aggregation_lookback_days": settings.agg_lookback_days,
            "export_interval_s": settings.export_interval_seconds,
            "export_lookback_days": settings.export_lookback_days,
        },
    )
    tasks = [
        asyncio.create_task(
            periodic("aggregation", settings.agg_interval_seconds, settings.agg_initial_delay_seconds, aggregator.scheduled)
        ),
        asyncio.create_task(
            periodic("export", settings.export_interval_seconds, settings.export_initial_delay_seconds, exporter.scheduled)
        ),
    ]
    yield
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


app = FastAPI(title="Aggregation Service", version="1.0.0", lifespan=lifespan)


def _error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": code, "message": message})


@app.post("/api/v1/aggregations/recompute")
async def recompute(day: date = Query(alias="date", description="День (UTC), YYYY-MM-DD")):
    """Ручной пересчёт метрик за день, не дожидаясь расписания."""
    try:
        result = await asyncio.to_thread(aggregator.run, day, day, "http")
    except Exception as exc:  # noqa: BLE001
        logger.exception("manual aggregation failed")
        return _error(503, "AGGREGATION_FAILED", repr(exc))
    return result.as_dict()


@app.post("/api/v1/exports")
async def export(day: date = Query(alias="date", description="День (UTC), YYYY-MM-DD")):
    """Ручной экспорт агрегатов за день в S3; файл перезаписывается."""
    try:
        result = await asyncio.to_thread(exporter.export, day, True)
    except NoDataError as exc:
        return _error(404, "NO_AGGREGATES", str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.exception("manual export failed")
        return _error(503, "EXPORT_FAILED", repr(exc))
    return result.as_dict()


@app.get("/api/v1/metrics")
async def metrics(day: date = Query(alias="date")):
    """Готовые метрики за день из PostgreSQL."""
    try:
        data = await asyncio.to_thread(store.load_day, day)
    except Exception as exc:  # noqa: BLE001
        return _error(503, "POSTGRES_UNAVAILABLE", repr(exc))
    return {"metric_date": day.isoformat(), **data}


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=settings.http_port, log_config=None, access_log=False)
