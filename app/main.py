"""Точка входа: сгенерированные роутеры + реализации из app.impl, обработка ошибок, логирование."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.config import settings
from app.db import engine, transaction
from app.errors import register_exception_handlers
from app.request_logging import RequestLoggingMiddleware, configure_logging
from app.security import authenticate
from app.services.auth import ensure_admin
from marketplace_api.apis.auth_api import router as auth_router
from marketplace_api.apis.orders_api import router as orders_router
from marketplace_api.apis.products_api import router as products_router
from marketplace_api.apis.promo_codes_api import router as promo_codes_router
from marketplace_api.apis.system_api import router as system_router
from marketplace_api.security_api import get_token_bearerAuth

configure_logging()
logger = logging.getLogger("app")


@asynccontextmanager
async def lifespan(_: FastAPI):
    if settings.admin_email and settings.admin_password:
        async with transaction() as session:
            if await ensure_admin(session, settings.admin_email, settings.admin_password):
                logger.info("admin user created: %s", settings.admin_email)
    yield
    await engine.dispose()


app = FastAPI(title="Marketplace API", version="1.0.0", lifespan=lifespan)

for router in (system_router, auth_router, products_router, orders_router, promo_codes_router):
    app.include_router(router)

# Сгенерированная get_token_bearerAuth — заглушка; проверку JWT выполняет authenticate.
app.dependency_overrides[get_token_bearerAuth] = authenticate

register_exception_handlers(app)
app.add_middleware(RequestLoggingMiddleware)
