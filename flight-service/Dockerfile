# Сборка из корня репозитория: docker build -f flight-service/Dockerfile .

# Этап 1: кодогенерация из proto/ (protoc из grpcio-tools).
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
    GRPC_PORT=50051

WORKDIR /srv

COPY flight-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY --from=codegen /gen ./generated
COPY flight-service/app ./app

RUN useradd --system --no-create-home appuser
USER appuser

EXPOSE 50051

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=5 CMD python -m app.healthcheck

CMD ["python", "-m", "app.main"]
