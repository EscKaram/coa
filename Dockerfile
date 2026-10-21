# Этап 1: генерация роутеров и моделей из OpenAPI-спецификации.
FROM openapitools/openapi-generator-cli:v7.10.0 AS codegen

WORKDIR /src
COPY openapi ./openapi
COPY scripts/generate.sh ./scripts/generate.sh
RUN sh scripts/generate.sh

# Этап 2: приложение.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/srv/generated:/srv \
    PORT=8080

WORKDIR /srv

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --from=codegen /src/generated ./generated
COPY app ./app

RUN useradd --system --no-create-home appuser
USER appuser

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import os, urllib.request; urllib.request.urlopen(f'http://127.0.0.1:{os.environ[\"PORT\"]}/health', timeout=2)" || exit 1

# Access-лог uvicorn отключён: каждый запрос логирует RequestLoggingMiddleware в JSON.
CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT} --no-access-log"]
