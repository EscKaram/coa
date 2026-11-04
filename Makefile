API_KEY ?= dev-booking-to-flight-key-change-me
GRPCURL := docker run --rm -i --network flight-booking_default fullstorydev/grpcurl:v1.9.1 -plaintext -H 'x-api-key: $(API_KEY)'

.PHONY: up down reset logs proto test test-resilience unit grpc-list

# Сборка и запуск всей системы (одна команда из задания: docker compose up --build).
up:
	docker compose up --build

down:
	docker compose down

# Остановить и удалить данные обеих БД.
reset:
	docker compose down -v

logs:
	docker compose logs -f booking-service flight-service

# Локальная генерация кода из proto/ в generated/ (для IDE; при сборке образов код генерируется сам).
proto:
	docker run --rm -v "$(CURDIR):/src" -w /src python:3.12-slim sh -c \
		"pip install -q grpcio-tools==1.68.1 googleapis-common-protos==1.66.0 && sh scripts/gen_proto.sh generated"

# E2E против запущенной системы.
test:
	python3 tests/e2e.py

# E2E + отказы: остановка Flight Service, БД бронирований и мастера Redis.
test-resilience:
	python3 tests/e2e.py --resilience

# Unit-тесты Circuit Breaker и интерцепторов retry/metadata (в образе booking-service).
unit:
	docker compose run --rm --no-deps -e FLIGHT_SERVICE_API_KEY=unused \
		-v ./booking-service/tests:/srv/tests booking-service python -m unittest discover -s tests -v

# Список gRPC-сервисов через reflection (с API-ключом).
grpc-list:
	$(GRPCURL) flight-service:50051 list flight.v1.FlightService
