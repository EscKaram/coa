"""Movie Service: HTTP API приёма событий и генератор синтетического потока."""

import asyncio
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from .config import Settings
from .events import MovieEventIn, PublishedEvent, semantic_errors, to_record
from .generator import EventGenerator
from .logging_setup import setup_logging
from .publisher import EventPublisher, PublishError, SchemaValidationError

logger = logging.getLogger("movie_service.api")
settings = Settings.from_env()
setup_logging(settings.log_level)


@asynccontextmanager
async def lifespan(app: FastAPI):
    publisher = await asyncio.to_thread(EventPublisher.create_with_retry, settings)
    app.state.publisher = publisher
    app.state.generator = None
    task = None
    if settings.generator_enabled:
        generator = EventGenerator(settings, publisher)
        app.state.generator = generator
        task = asyncio.create_task(generator.run(), name="event-generator")
    yield
    if task is not None:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    await asyncio.to_thread(publisher.close)


app = FastAPI(title="Movie Service", version="1.0.0", lifespan=lifespan)


def _error(status: int, code: str, message: str, details: list[dict[str, str]] | None = None) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": code, "message": message, "details": details or []})


@app.exception_handler(RequestValidationError)
async def on_validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
    details = [
        {"field": ".".join(str(p) for p in err["loc"] if p != "body") or "body", "message": err["msg"]}
        for err in exc.errors()
    ]
    return _error(422, "VALIDATION_ERROR", "event does not match the schema", details)


@app.post("/api/v1/events", status_code=202, response_model=PublishedEvent)
async def publish_event(event: MovieEventIn, request: Request):
    """Проверяет событие и публикует его в Kafka; отвечает после подтверждения записи брокером (acks=all)."""
    errors = semantic_errors(event)
    if errors:
        return _error(422, "VALIDATION_ERROR", "event does not match the schema", errors)
    publisher: EventPublisher = request.app.state.publisher
    try:
        result = await publisher.publish(to_record(event))
    except SchemaValidationError as exc:
        return _error(422, "VALIDATION_ERROR", str(exc), exc.details)
    except PublishError as exc:
        return _error(503, "PUBLISH_FAILED", f"event was not written to Kafka: {exc}")
    return PublishedEvent(
        event_id=result.event_id,
        topic=result.topic,
        partition=result.partition,
        offset=result.offset,
        schema_version=publisher.schema_version,
    )


@app.get("/api/v1/generator")
async def generator_status(request: Request):
    generator: EventGenerator | None = request.app.state.generator
    if generator is None:
        return {"enabled": False}
    return {"enabled": True, **generator.stats}


@app.get("/health")
async def health(request: Request):
    publisher: EventPublisher = request.app.state.publisher
    ready = await asyncio.to_thread(publisher.is_ready)
    body = {"status": "ok" if ready else "degraded", "kafka": ready, "schema_version": publisher.schema_version}
    return JSONResponse(status_code=200 if ready else 503, content=body)


if __name__ == "__main__":
    # log_config=None: логи uvicorn идут через корневой логгер в JSON.
    uvicorn.run(app, host="0.0.0.0", port=settings.http_port, log_config=None, access_log=False)
