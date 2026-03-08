.PHONY: up up-full down down-full logs psql test status restart clean build build-images

# ============================================================
# Weather Data Engineering Pipeline - Makefile
# ============================================================

COMPOSE = docker compose --env-file .env -f infrastructure/docker/docker-compose.yml
COMPOSE_FULL = $(COMPOSE) -f infrastructure/docker/docker-compose.openmetadata.yml

## Build Airflow image only
build:
	$(COMPOSE) build

## Build all processing images (dlt, dbt, soda) used by DockerOperator
build-images:
	$(COMPOSE) --profile build-only build

## Start core services (Airflow, PostgreSQL, MinIO, Prometheus, Grafana)
up: build-images
	$(COMPOSE) up -d --build

## Start all services including OpenMetadata catalog
up-full: build-images
	$(COMPOSE) up -d --build
	$(COMPOSE_FULL) up -d

## Stop and remove core services and volumes
down:
	$(COMPOSE) down -v

## Stop and remove all services including OpenMetadata
down-full:
	$(COMPOSE_FULL) down -v
	$(COMPOSE) down -v

## Follow service logs
logs:
	$(COMPOSE) logs -f

## Open a psql shell
psql:
	$(COMPOSE) exec postgres psql -U airflow -d weather_db

## Run unit tests locally via uv
test:
	uv run --extra dev pytest tests/unit/ -v --tb=short

## Show service status
status:
	$(COMPOSE) ps

## Restart all services
restart:
	$(COMPOSE) restart

## Remove all containers, volumes, and built images
clean:
	$(COMPOSE_FULL) down -v --rmi local 2>/dev/null || true
	$(COMPOSE) down -v --rmi local
