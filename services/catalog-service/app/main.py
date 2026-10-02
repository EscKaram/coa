"""Catalog Service — каркас сервиса без бизнес-логики.

Сервис владеет данными каталога (товары, категории, цены, остатки).
На текущем этапе реализован только health-check.
"""

import os

from fastapi import FastAPI

SERVICE_NAME = "catalog-service"
SERVICE_VERSION = os.getenv("SERVICE_VERSION", "0.1.0")

app = FastAPI(title=SERVICE_NAME, version=SERVICE_VERSION)


@app.get("/health")
def health() -> dict:
    """Liveness probe: процесс запущен и обрабатывает HTTP-запросы."""
    return {"status": "ok", "service": SERVICE_NAME, "version": SERVICE_VERSION}
