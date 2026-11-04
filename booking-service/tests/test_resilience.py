"""Unit-тесты Circuit Breaker и интерцепторов retry/metadata на настоящем grpc.aio-сервере в процессе.

Запуск (нужны зависимости сервиса и сгенерированные стабы, поэтому — в контейнере):
    docker compose run --rm --no-deps -v ./booking-service/tests:/srv/tests booking-service \
        python -m unittest discover -s tests -v
"""

import unittest
from unittest import mock

import grpc

from app import interceptors
from app.circuit_breaker import CircuitBreaker, CircuitOpenError, State
from app.interceptors import CircuitBreakerInterceptor, MetadataInterceptor, RetryInterceptor
from flight.v1 import flight_service_pb2 as pb
from flight.v1 import flight_service_pb2_grpc as pb_grpc


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class CircuitBreakerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = FakeClock()
        self.breaker = CircuitBreaker("test", failure_threshold=3, window_seconds=10, open_timeout_seconds=5, clock=self.clock)

    def fail(self, times: int = 1) -> None:
        for _ in range(times):
            probe = self.breaker.acquire()
            self.breaker.on_failure(probe, "UNAVAILABLE")

    def test_opens_after_threshold_consecutive_failures(self) -> None:
        self.fail(2)
        self.assertIs(self.breaker.state, State.CLOSED)
        self.fail()
        self.assertIs(self.breaker.state, State.OPEN)
        with self.assertRaises(CircuitOpenError) as ctx:
            self.breaker.acquire()
        self.assertEqual(ctx.exception.code(), grpc.StatusCode.UNAVAILABLE)
        self.assertAlmostEqual(ctx.exception.retry_after_seconds, 5)

    def test_success_resets_consecutive_failures(self) -> None:
        self.fail(2)
        self.breaker.on_success(self.breaker.acquire())
        self.fail(2)
        self.assertIs(self.breaker.state, State.CLOSED)

    def test_failures_outside_window_do_not_open(self) -> None:
        self.fail(2)
        self.clock.now += 11
        self.fail()
        self.assertIs(self.breaker.state, State.CLOSED)
        self.fail()
        self.assertIs(self.breaker.state, State.CLOSED)
        self.fail()
        self.assertIs(self.breaker.state, State.OPEN)

    def test_half_open_lets_single_probe_and_closes_on_success(self) -> None:
        self.fail(3)
        self.clock.now += 5
        probe = self.breaker.acquire()
        self.assertTrue(probe)
        self.assertIs(self.breaker.state, State.HALF_OPEN)
        with self.assertRaises(CircuitOpenError):
            self.breaker.acquire()  # пробный вызов ещё идёт
        self.breaker.on_success(probe)
        self.assertIs(self.breaker.state, State.CLOSED)
        self.assertFalse(self.breaker.acquire())

    def test_half_open_reopens_on_probe_failure(self) -> None:
        self.fail(3)
        self.clock.now += 5
        self.breaker.on_failure(self.breaker.acquire(), "UNAVAILABLE")
        self.assertIs(self.breaker.state, State.OPEN)
        self.clock.now += 4.9
        with self.assertRaises(CircuitOpenError):
            self.breaker.acquire()

    def test_abandoned_probe_frees_the_slot(self) -> None:
        self.fail(3)
        self.clock.now += 5
        self.breaker.on_abandoned(self.breaker.acquire())
        self.assertIs(self.breaker.state, State.HALF_OPEN)
        self.assertTrue(self.breaker.acquire())

    def test_late_failure_of_call_started_before_open_is_ignored(self) -> None:
        self.fail(3)
        self.breaker.on_failure(False, "UNAVAILABLE")
        self.clock.now += 5
        self.assertTrue(self.breaker.acquire())


class FlakyFlightService(pb_grpc.FlightServiceServicer):
    """GetFlight отвечает кодами из очереди, затем OK. Запоминает metadata каждого вызова."""

    def __init__(self) -> None:
        self.codes: list[grpc.StatusCode] = []
        self.calls: list[dict[str, str]] = []

    async def GetFlight(self, request, context):
        self.calls.append(dict(context.invocation_metadata()))
        if self.codes:
            await context.abort(self.codes.pop(0), "injected failure")
        return pb.GetFlightResponse(flight=pb.Flight(id=request.flight_id))


class InterceptorChainTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.service = FlakyFlightService()
        self.server = grpc.aio.server()
        pb_grpc.add_FlightServiceServicer_to_server(self.service, self.server)
        port = self.server.add_insecure_port("127.0.0.1:0")
        await self.server.start()

        self.breaker = CircuitBreaker("test", failure_threshold=2, window_seconds=60, open_timeout_seconds=30)
        self.channel = grpc.aio.insecure_channel(
            f"127.0.0.1:{port}",
            interceptors=[
                MetadataInterceptor("secret"),
                CircuitBreakerInterceptor(self.breaker),
                RetryInterceptor(max_attempts=3, initial_backoff_s=0.1, multiplier=2, attempt_timeout_s=2),
            ],
        )
        self.stub = pb_grpc.FlightServiceStub(self.channel)
        self.sleeps: list[float] = []

        async def fake_sleep(delay: float) -> None:
            self.sleeps.append(delay)

        patcher = mock.patch.object(interceptors.asyncio, "sleep", fake_sleep)
        patcher.start()
        self.addCleanup(patcher.stop)

    async def asyncTearDown(self) -> None:
        await self.channel.close()
        await self.server.stop(None)

    async def get(self):
        return await self.stub.GetFlight(pb.GetFlightRequest(flight_id="f-1"))

    async def test_retries_unavailable_with_exponential_backoff_and_metadata_on_every_attempt(self) -> None:
        self.service.codes = [grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED]
        response = await self.get()
        self.assertEqual(response.flight.id, "f-1")
        self.assertEqual(len(self.service.calls), 3)
        self.assertEqual(self.sleeps, [0.1, 0.2])
        self.assertTrue(all(call.get("x-api-key") == "secret" for call in self.service.calls))
        self.assertIs(self.breaker.state, State.CLOSED)

    async def test_no_retry_for_business_errors(self) -> None:
        for code in (grpc.StatusCode.NOT_FOUND, grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.RESOURCE_EXHAUSTED):
            self.service.calls.clear()
            self.service.codes = [code]
            with self.assertRaises(grpc.aio.AioRpcError) as ctx:
                await self.get()
            self.assertEqual(ctx.exception.code(), code)
            self.assertEqual(len(self.service.calls), 1, code)
        self.assertEqual(self.sleeps, [])
        self.assertIs(self.breaker.state, State.CLOSED)

    async def test_exhausted_retries_count_as_one_failure_and_open_breaker(self) -> None:
        self.service.codes = [grpc.StatusCode.UNAVAILABLE] * 3
        with self.assertRaises(grpc.aio.AioRpcError):
            await self.get()
        self.assertEqual(len(self.service.calls), 3)
        self.assertIs(self.breaker.state, State.CLOSED)  # один логический отказ из двух

        self.service.codes = [grpc.StatusCode.UNAVAILABLE] * 3
        with self.assertRaises(grpc.aio.AioRpcError):
            await self.get()
        self.assertIs(self.breaker.state, State.OPEN)

        calls_before = len(self.service.calls)
        with self.assertRaises(CircuitOpenError):
            await self.get()
        self.assertEqual(len(self.service.calls), calls_before)  # в OPEN сеть не трогаем


if __name__ == "__main__":
    unittest.main()
