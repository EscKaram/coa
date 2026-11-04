"""Аутентификация межсервисных вызовов: API-ключ в metadata "x-api-key"."""

import hmac
import logging
from collections.abc import Awaitable, Callable

import grpc

from app.logging_setup import log_event

logger = logging.getLogger("flight.auth")

API_KEY_HEADER = "x-api-key"
# Health check доступен без ключа: его вызывает healthcheck Docker. Бизнес-методы и reflection — только с ключом.
PUBLIC_METHOD_PREFIXES = ("/grpc.health.v1.Health/",)


def _deny_handler(handler: grpc.RpcMethodHandler | None) -> grpc.RpcMethodHandler:
    """Обработчик того же типа (unary/stream), что и исходный, который сразу отвечает UNAUTHENTICATED."""

    async def deny(_request, context: grpc.aio.ServicerContext) -> None:
        await context.abort(grpc.StatusCode.UNAUTHENTICATED, f"missing or invalid {API_KEY_HEADER}")

    if handler is None:
        return grpc.unary_unary_rpc_method_handler(deny)
    kwargs = {
        "request_deserializer": handler.request_deserializer,
        "response_serializer": handler.response_serializer,
    }
    if handler.request_streaming and handler.response_streaming:
        return grpc.stream_stream_rpc_method_handler(deny, **kwargs)
    if handler.request_streaming:
        return grpc.stream_unary_rpc_method_handler(deny, **kwargs)
    if handler.response_streaming:
        return grpc.unary_stream_rpc_method_handler(deny, **kwargs)
    return grpc.unary_unary_rpc_method_handler(deny, **kwargs)


class ApiKeyAuthInterceptor(grpc.aio.ServerInterceptor):
    def __init__(self, api_keys: tuple[str, ...]) -> None:
        self._keys = tuple(k.encode() for k in api_keys)

    def _is_valid(self, presented: str) -> bool:
        # Сравнение за постоянное время и со всеми ключами, без раннего выхода.
        valid = False
        for key in self._keys:
            valid |= hmac.compare_digest(presented.encode(), key)
        return valid

    async def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        method = handler_call_details.method
        if method.startswith(PUBLIC_METHOD_PREFIXES):
            return await continuation(handler_call_details)

        presented = None
        for key, value in handler_call_details.invocation_metadata or ():
            if key == API_KEY_HEADER:
                presented = value
                break
        if presented is not None and self._is_valid(presented):
            return await continuation(handler_call_details)

        log_event(
            logger, logging.WARNING, "auth_rejected", method=method,
            reason="missing api key" if presented is None else "invalid api key",
        )
        return _deny_handler(await continuation(handler_call_details))
