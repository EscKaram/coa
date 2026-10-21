"""JSON-логирование каждого API-запроса и проброс X-Request-Id."""

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

logger = logging.getLogger("api.request")

MUTATING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
SENSITIVE_KEYS = {"password", "access_token", "refresh_token"}
MASK = "***"
# Тело запроса буферизуется для лога не больше этого объёма.
MAX_LOGGED_BODY_BYTES = 16 * 1024


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = getattr(record, "payload", None)
        if payload is None:
            payload = {
                "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }
            if record.exc_info:
                payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)


def mask_sensitive(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: MASK if k.lower() in SENSITIVE_KEYS else mask_sensitive(v) for k, v in value.items()}
    if isinstance(value, list):
        return [mask_sensitive(v) for v in value]
    return value


def _body_for_log(raw: bytes, truncated: bool) -> Any:
    if not raw:
        return None
    if truncated:
        return {"truncated": True, "size_bytes_logged": len(raw)}
    try:
        return mask_sensitive(json.loads(raw))
    except ValueError:
        return raw.decode("utf-8", errors="replace")


class RequestLoggingMiddleware:
    """Чистый ASGI-middleware: не мешает обработчику читать тело и видит итоговый статус ответа."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = str(uuid.uuid4())
        # request.state в обработчиках указывает на этот же словарь; туда authenticate кладёт user_id.
        state = scope.setdefault("state", {})
        state["request_id"] = request_id
        method = scope["method"]
        capture_body = method in MUTATING_METHODS
        body = bytearray()
        truncated = False
        status_code = 500
        timestamp = datetime.now(timezone.utc)
        started = time.perf_counter()

        async def receive_wrapper() -> Message:
            nonlocal truncated
            message = await receive()
            if capture_body and message["type"] == "http.request" and not truncated:
                chunk = message.get("body", b"")
                if len(body) + len(chunk) > MAX_LOGGED_BODY_BYTES:
                    truncated = True
                else:
                    body.extend(chunk)
            return message

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
                MutableHeaders(scope=message).append("X-Request-Id", request_id)
            await send(message)

        try:
            await self.app(scope, receive_wrapper, send_wrapper)
        finally:
            record: dict[str, Any] = {
                "timestamp": timestamp.isoformat(),
                "request_id": request_id,
                "method": method,
                "endpoint": scope["path"],
                "status_code": status_code,
                "duration_ms": round((time.perf_counter() - started) * 1000, 3),
                "user_id": state.get("user_id"),
            }
            if capture_body:
                record["request_body"] = _body_for_log(bytes(body), truncated)
            logger.info("request", extra={"payload": record})
