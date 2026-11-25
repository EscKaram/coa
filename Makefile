.PHONY: up down reset ps logs test test-failover

# Сборка и запуск всей системы (то же, что docker compose up --build, но в фоне с ожиданием healthcheck).
up:
	docker compose up --build -d --wait

down:
	docker compose down

# Остановить и удалить все данные (Kafka, ClickHouse, PostgreSQL, MinIO, Grafana).
reset:
	docker compose down -v

ps:
	docker compose ps -a

logs:
	docker compose logs -f movie-service aggregation-service

# Интеграционные тесты п. 1–8 (кроме отказа брокера). Поднимает систему, если она не запущена.
test:
	docker compose --profile test run --rm --build tests

# П. 8: отказ брокера kafka-1. Брокер возвращается в строй, даже если тесты упали.
test-failover:
	docker compose --profile test build tests
	docker compose stop kafka-1
	docker compose --profile test run --rm --no-deps tests -v -m failover; \
		status=$$?; docker compose start kafka-1; exit $$status
