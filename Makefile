.PHONY: up up-full down down-full logs psql test status restart clean build

# ============================================================
# Weather Data Engineering Pipeline - Makefile
# ============================================================

COMPOSE = docker compose --env-file .env -f infrastructure/docker/docker-compose.yml
COMPOSE_FULL = $(COMPOSE) -f infrastructure/docker/docker-compose.openmetadata.yml

## Build images without starting
build:
	$(COMPOSE) build

## Start core services (Airflow, PostgreSQL, MinIO, Prometheus, Grafana)
up:
	$(COMPOSE) up -d --build

## Start all services including OpenMetadata catalog
up-full:
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

## Run unit tests inside the scheduler container
test:
	$(COMPOSE) exec airflow-scheduler bash -c "PYTHONPATH=/opt/airflow /usr/python/bin/python -m pytest /opt/airflow/tests/ -v --tb=short"

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
