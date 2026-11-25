# Сборка из корня репозитория: docker build -f movie-service/Dockerfile .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/srv \
    SCHEMA_DIR=/srv/schemas \
    PORT=8000

WORKDIR /srv

COPY movie-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY schemas ./schemas
COPY movie-service/app ./app

RUN useradd --system --no-create-home appuser
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=5s --timeout=4s --start-period=20s --retries=10 \
    CMD python -c "import os, urllib.request; urllib.request.urlopen(f'http://127.0.0.1:{os.environ[\"PORT\"]}/health', timeout=3)" || exit 1

CMD ["python", "-m", "app.main"]
