"""Клиентские gRPC-интерцепторы Booking Service для вызовов Flight Service.

Порядок в канале (первый — внешний):
    MetadataInterceptor        x-api-key и x-request-id в metadata (один раз на логический вызов)
    CircuitBreakerInterceptor  один исход на логический вызов, уже после всех повторов
    RetryInterceptor           повторы с exponential backoff, дедлайн на каждую попытку

Retry стоит внутри Circuit Breaker: в OPEN вызов отклоняется сразу, без повторов и пауз,
а исчерпанные повторы считаются одним отказом.
"""

import asyncio
import logging
import time

import grpc

from app.circuit_breaker import FAILURE_CODES, CircuitBreaker
from app.context import request_id_var
from app.logging_setup import log_event

logger = logging.getLogger("booking.grpc")

RETRYABLE_CODES = frozenset({grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED})


def _method_name(details: grpc.aio.ClientCallDetails) -> str:
    method = details.method
    return (method.decode() if isinstance(method, bytes) else method).rsplit("/", 1)[-1]


class MetadataInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    def __init__(self, api_key: str) -> None:
        self._api_key = api_key

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        metadata = grpc.aio.Metadata(*(client_call_details.metadata or ()))
        metadata.add("x-api-key", self._api_key)
        request_id = request_id_var.get()
        if request_id:
            metadata.add("x-request-id", request_id)
        return await continuation(client_call_details._replace(metadata=metadata), request)


class CircuitBreakerInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    def __init__(self, breaker: CircuitBreaker) -> None:
        self._breaker = breaker

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        probe = self._breaker.acquire()  # в OPEN бросает CircuitOpenError: сети не касаемся
        try:
            call = await continuation(client_call_details, request)
            response = await call
        except grpc.aio.AioRpcError as exc:
            if exc.code() in FAILURE_CODES:
                self._breaker.on_failure(probe, f"{_method_name(client_call_details)}: {exc.code().name}")
            else:
                self._breaker.on_success(probe)
            raise
        except BaseException:
            self._breaker.on_abandoned(probe)
            raise
        self._breaker.on_success(probe)
        return response


class RetryInterceptor(grpc.aio.UnaryUnaryClientInterceptor):
    """Не больше max_attempts попыток; пауза перед попыткой n+1 = initial_backoff * multiplier^(n-1).

    При max_attempts=3, 100 мс, x2: попытка, 100 мс, попытка, 200 мс, попытка.
    Повторяются только UNAVAILABLE и DEADLINE_EXCEEDED. Мутирующие методы безопасно повторять:
    ReserveSeats идемпотентен по booking_id, ReleaseReservation — по состоянию резервации.
    Худшее время логического вызова: max_attempts * attempt_timeout + сумма пауз.
    """

    def __init__(self, max_attempts: int, initial_backoff_s: float, multiplier: float, attempt_timeout_s: float) -> None:
        self._max_attempts = max_attempts
        self._initial_backoff_s = initial_backoff_s
        self._multiplier = multiplier
        self._attempt_timeout_s = attempt_timeout_s

    def backoff_s(self, attempt: int) -> float:
        """Пауза после неудачной попытки номер attempt (с 1)."""
        return self._initial_backoff_s * self._multiplier ** (attempt - 1)

    async def intercept_unary_unary(self, continuation, client_call_details, request):
        method = _method_name(client_call_details)
        details = client_call_details._replace(timeout=self._attempt_timeout_s)
        for attempt in range(1, self._max_attempts + 1):
            started_ns = time.perf_counter_ns()
            try:
                call = await continuation(details, request)
                return await call
            except grpc.aio.AioRpcError as exc:
                elapsed_us = (time.perf_counter_ns() - started_ns) // 1000
                if exc.code() not in RETRYABLE_CODES:
                    raise
                if attempt == self._max_attempts:
                    log_event(
                        logger, logging.WARNING, "grpc_retry_exhausted", method=method, attempts=attempt,
                        code=exc.code().name, attempt_duration_us=elapsed_us, request_id=request_id_var.get(),
                    )
                    raise
                delay_s = self.backoff_s(attempt)
                log_event(
                    logger, logging.WARNING, "grpc_retry", method=method, attempt=attempt,
                    max_attempts=self._max_attempts, code=exc.code().name, attempt_duration_us=elapsed_us,
                    backoff_ms=round(delay_s * 1000), request_id=request_id_var.get(),
                )
                await asyncio.sleep(delay_s)
        raise AssertionError("unreachable")  # pragma: no cover
