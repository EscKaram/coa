GENERATOR_IMAGE := openapitools/openapi-generator-cli:v7.10.0
PSQL := docker compose exec postgres psql -U marketplace -d marketplace

.PHONY: generate up down reset logs psql

# Генерация кода из openapi/openapi.yaml в generated/ (нужен только Docker).
generate:
	docker run --rm -v "$(CURDIR):/local" -w /local --entrypoint sh $(GENERATOR_IMAGE) scripts/generate.sh

up:
	docker compose up --build

down:
	docker compose down

# Остановить и удалить данные БД.
reset:
	docker compose down -v

logs:
	docker compose logs -f app

psql:
	$(PSQL)
