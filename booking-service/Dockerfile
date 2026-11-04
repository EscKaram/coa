# Сборка из корня репозитория: docker build -f booking-service/Dockerfile .

# Этап 1: кодогенерация клиента Flight Service из proto/ (protoc из grpcio-tools).
FROM python:3.12-slim AS codegen
RUN pip install --no-cache-dir grpcio-tools==1.68.1 googleapis-common-protos==1.66.0
WORKDIR /src
COPY proto ./proto
COPY scripts/gen_proto.sh ./scripts/gen_proto.sh
RUN sh scripts/gen_proto.sh /gen

# Этап 2: сервис.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/srv/generated:/srv \
    PORT=8080

WORKDIR /srv

COPY booking-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --from=codegen /gen ./generated
COPY booking-service/app ./app

RUN useradd --system --no-create-home appuser
USER appuser

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=5 \
    CMD python -c "import os, urllib.request; urllib.request.urlopen(f'http://127.0.0.1:{os.environ[\"PORT\"]}/health', timeout=2)" || exit 1

# Access-лог uvicorn отключён: каждый запрос логирует RequestLoggingMiddleware в JSON.
CMD ["sh", "-c", "exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT} --no-access-log"]
