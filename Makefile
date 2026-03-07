.PHONY: up down logs psql test status restart clean build

# ============================================================
# Weather Data Engineering Pipeline - Makefile
# ============================================================

COMPOSE = docker compose --env-file .env -f docker/docker-compose.yml

## Build images without starting
build:
	$(COMPOSE) build

## Start all services
up:
	$(COMPOSE) up -d --build

## Stop and remove all services and volumes
down:
	$(COMPOSE) down -v

## Follow service logs
logs:
	$(COMPOSE) logs -f

## Open a psql shell
psql:
	$(COMPOSE) exec postgres psql -U airflow -d weather_db

## Run unit tests inside the scheduler container
test:
	$(COMPOSE) exec airflow-scheduler python -m pytest /opt/airflow/ingestion/tests/ -v --tb=short

## Show service status
status:
	$(COMPOSE) ps

## Restart all services
restart:
	$(COMPOSE) restart

## Remove all containers, volumes, and built images
clean:
	$(COMPOSE) down -v --rmi local
