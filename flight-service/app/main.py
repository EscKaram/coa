"""Точка входа Flight Service: gRPC-сервер (FlightService + Health + Reflection)."""

import asyncio
import logging
import signal

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection
from sqlalchemy.ext.asyncio import create_async_engine

from app.auth import ApiKeyAuthInterceptor
from app.cache import FlightCache, create_redis
from app.config import load_settings
from app.logging_setup import configure_logging
from app.servicer import FlightServicer
from app.store import FlightStore
from flight.v1 import flight_service_pb2 as pb
from flight.v1 import flight_service_pb2_grpc as pb_grpc

logger = logging.getLogger("flight")

FLIGHT_SERVICE_NAME = pb.DESCRIPTOR.services_by_name["FlightService"].full_name
SHUTDOWN_GRACE_SECONDS = 5


async def serve() -> None:
    configure_logging()
    settings = load_settings()

    engine = create_async_engine(settings.database_url, pool_size=settings.db_pool_size, pool_pre_ping=True)
    redis = create_redis(settings)
    cache = FlightCache(redis, settings.cache_ttl_seconds, settings.cache_generation_ttl_seconds)

    server = grpc.aio.server(interceptors=[ApiKeyAuthInterceptor(settings.api_keys)])
    pb_grpc.add_FlightServiceServicer_to_server(FlightServicer(FlightStore(engine, settings.search_limit), cache), server)
    health_servicer = health.aio.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    reflection.enable_server_reflection((FLIGHT_SERVICE_NAME, health.SERVICE_NAME, reflection.SERVICE_NAME), server)
    server.add_insecure_port(f"[::]:{settings.port}")

    await server.start()
    for service in ("", FLIGHT_SERVICE_NAME):
        await health_servicer.set(service, health_pb2.HealthCheckResponse.SERVING)
    logger.info(
        "flight service listening on :%d, redis sentinels %s, master %s, cache ttl %d s",
        settings.port, settings.redis_sentinels, settings.redis_master_name, settings.cache_ttl_seconds,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()

    logger.info("shutting down")
    await health_servicer.enter_graceful_shutdown()
    await server.stop(SHUTDOWN_GRACE_SECONDS)
    await redis.aclose()
    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(serve())
