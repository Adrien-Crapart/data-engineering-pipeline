.PHONY: help start stop restart status logs clean prune \
       start-warehouse start-lake start-airflow start-monitoring start-metadata \
       build build-airflow build-dlt build-dbt build-soda build-all \
       psql airflow-shell dbt-run dbt-test dbt-docs dlt-run soda-check \
       lint lint-sql format check pre-commit pre-commit-install \
       test test-unit test-integration test-cov \
       doctor validate-env urls

# ============================================================
# Data Engineering Pipeline — Makefile
# Cross-platform (Linux, macOS, Windows via MSYS2/Git Bash)
# ============================================================

SHELL := /bin/bash

# --- OS detection ---
ifeq ($(OS),Windows_NT)
    DETECTED_OS := Windows
    DOCKER_CONTEXT := --context desktop-linux
else
    DETECTED_OS := $(shell uname -s)
    DOCKER_CONTEXT :=
endif

# --- Config ---
ENV_FILE     := .env
COMPOSE_DIR  := infrastructure/docker

F_WAREHOUSE  := $(COMPOSE_DIR)/compose.data-warehouse.yaml
F_LAKE       := $(COMPOSE_DIR)/compose.data-lake.yaml
F_AIRFLOW    := $(COMPOSE_DIR)/compose.orchestration.yaml
F_BUILD      := $(COMPOSE_DIR)/compose.build-images.yaml
F_MONITORING := $(COMPOSE_DIR)/compose.monitoring.yaml
F_METADATA   := $(COMPOSE_DIR)/compose.metadata.yaml

COMPOSE_ALL  := -f $(F_WAREHOUSE) -f $(F_LAKE) -f $(F_AIRFLOW) \
                -f $(F_BUILD) -f $(F_MONITORING)

COMPOSE_FULL := $(COMPOSE_ALL) -f $(F_METADATA) --profile openmetadata

DC_BASE = docker $(DOCKER_CONTEXT) compose -p data-engineering-pipeline \
          --project-directory . --env-file $(ENV_FILE)

DC      = $(DC_BASE) $(COMPOSE_ALL)
DC_FULL = $(DC_BASE) $(COMPOSE_FULL)

# Wait helper (seconds)
WAIT = sleep

# ============================================================
#  HELP (default target)
# ============================================================

help: ## Show available commands
	@echo ""
	@echo "=== Data Engineering Pipeline ==="
	@echo ""
	@echo "  Lifecycle:"
	@echo "    make start              Start all core services (ordered)"
	@echo "    make start-full         Start all services including OpenMetadata"
	@echo "    make stop               Stop all core services and remove volumes"
	@echo "    make stop-full          Stop all services including OpenMetadata"
	@echo "    make restart            Restart core services"
	@echo "    make status             Show service status"
	@echo "    make logs               Follow service logs"
	@echo ""
	@echo "  Per-Stack:"
	@echo "    make start-warehouse    Start PostgreSQL + Redis"
	@echo "    make start-lake         Start MinIO"
	@echo "    make start-airflow      Start Airflow stack"
	@echo "    make start-monitoring   Start Prometheus + Grafana"
	@echo "    make start-metadata     Start OpenMetadata"
	@echo ""
	@echo "  Build:"
	@echo "    make build-airflow      Build Airflow image"
	@echo "    make build-dlt          Build DLT runner image"
	@echo "    make build-dbt          Build dbt runner image"
	@echo "    make build-soda         Build Soda runner image"
	@echo "    make build-all          Build all images"
	@echo ""
	@echo "  Dev Tools:"
	@echo "    make psql               Open psql shell on weather_db"
	@echo "    make airflow-shell      Open bash in Airflow worker"
	@echo "    make dbt-run            Run dbt models via Docker"
	@echo "    make dbt-test           Run dbt tests via Docker"
	@echo "    make dlt-run            Run DLT ingestion via Docker"
	@echo "    make soda-check         Run Soda checks via Docker"
	@echo ""
	@echo "  Quality:"
	@echo "    make lint               Run ruff linter"
	@echo "    make lint-sql           Run SQLFluff on dbt models"
	@echo "    make format             Auto-format Python code"
	@echo "    make check              Run all checks (lint + format-check)"
	@echo "    make pre-commit         Run pre-commit on all files"
	@echo ""
	@echo "  Tests:"
	@echo "    make test               Run all tests"
	@echo "    make test-unit          Run unit tests only"
	@echo "    make test-integration   Run integration tests (requires Docker)"
	@echo "    make test-cov           Run tests with coverage report"
	@echo ""
	@echo "  Ops:"
	@echo "    make doctor             Check environment health"
	@echo "    make validate-env       Validate .env file"
	@echo "    make urls               Show service URLs"
	@echo "    make clean              Remove all containers, volumes, images"
	@echo "    make prune              Docker system prune"
	@echo ""

# ============================================================
#  LIFECYCLE
# ============================================================

start: build-all start-warehouse start-lake start-airflow start-monitoring urls ## Start all core services (ordered)

start-full: build-all start-warehouse start-lake start-airflow start-monitoring start-metadata urls ## Start all services including OpenMetadata

stop: ## Stop core services and remove volumes
	$(DC) down -v

stop-full: ## Stop all services including OpenMetadata
	$(DC_FULL) down -v

restart: ## Restart core services
	$(DC) restart

status: ## Show service status
	@$(DC_FULL) ps 2>/dev/null || $(DC) ps

logs: ## Follow service logs
	$(DC) logs -f

# ============================================================
#  PER-STACK TARGETS
# ============================================================

start-warehouse: ## Start PostgreSQL + Redis
	@echo "--- Starting Data Warehouse (PostgreSQL + Redis) ---"
	$(DC_BASE) -f $(F_WAREHOUSE) up -d
	@echo "Waiting for warehouse to be healthy..."
	@$(WAIT) 10

start-lake: ## Start MinIO
	@echo "--- Starting Data Lake (MinIO) ---"
	$(DC_BASE) -f $(F_WAREHOUSE) -f $(F_LAKE) up -d minio minio-init
	@echo "Waiting for MinIO to be healthy..."
	@$(WAIT) 5

start-airflow: build-airflow ## Start Airflow stack
	@echo "--- Starting Airflow ---"
	$(DC_BASE) -f $(F_WAREHOUSE) -f $(F_LAKE) -f $(F_AIRFLOW) up -d
	@echo "Waiting for Airflow to initialize..."
	@$(WAIT) 15

start-monitoring: ## Start Prometheus + Grafana
	@echo "--- Starting Monitoring ---"
	$(DC_BASE) -f $(F_WAREHOUSE) -f $(F_LAKE) -f $(F_MONITORING) up -d prometheus prometheus-pushgateway grafana

start-metadata: ## Start OpenMetadata
	@echo "--- Starting OpenMetadata ---"
	$(DC_BASE) -f $(F_WAREHOUSE) -f $(F_METADATA) --profile openmetadata up -d

# ============================================================
#  BUILD
# ============================================================

build: build-airflow ## Build Airflow image (alias)

build-airflow: ## Build Airflow image
	@echo "--- Building Airflow image ---"
	$(DC_BASE) -f $(F_AIRFLOW) build

build-dlt: ## Build DLT runner image
	@echo "--- Building DLT runner image ---"
	$(DC_BASE) -f $(F_BUILD) --profile build-only build dlt-runner

build-dbt: ## Build dbt runner image
	@echo "--- Building dbt runner image ---"
	$(DC_BASE) -f $(F_BUILD) --profile build-only build dbt-runner

build-soda: ## Build Soda runner image
	@echo "--- Building Soda runner image ---"
	$(DC_BASE) -f $(F_BUILD) --profile build-only build soda-runner

build-all: build-airflow build-dlt build-dbt build-soda ## Build all images

# ============================================================
#  DEV TOOLS
# ============================================================

psql: ## Open psql shell on weather_db
	$(DC_BASE) -f $(F_WAREHOUSE) exec postgres psql -U airflow -d weather_db

airflow-shell: ## Open bash in Airflow worker
	$(DC_BASE) -f $(F_WAREHOUSE) -f $(F_LAKE) -f $(F_AIRFLOW) exec airflow-worker bash

dbt-run: ## Run dbt models via Docker
	docker $(DOCKER_CONTEXT) run --rm \
		--network data-engineering-pipeline_pipeline-network \
		-e POSTGRES_HOST=postgres -e POSTGRES_PORT=5432 \
		-e POSTGRES_DB=weather_db -e POSTGRES_USER=airflow -e POSTGRES_PASSWORD=airflow \
		-e MINIO_ENDPOINT_HOST=minio:9000 \
		-e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
		weather-pipeline-dbt:1.0.0 run

dbt-test: ## Run dbt tests via Docker
	docker $(DOCKER_CONTEXT) run --rm \
		--network data-engineering-pipeline_pipeline-network \
		-e POSTGRES_HOST=postgres -e POSTGRES_PORT=5432 \
		-e POSTGRES_DB=weather_db -e POSTGRES_USER=airflow -e POSTGRES_PASSWORD=airflow \
		-e MINIO_ENDPOINT_HOST=minio:9000 \
		-e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
		weather-pipeline-dbt:1.0.0 test

dlt-run: ## Run DLT ingestion via Docker
	docker $(DOCKER_CONTEXT) run --rm \
		--network data-engineering-pipeline_pipeline-network \
		--env-file $(ENV_FILE) \
		weather-pipeline-dlt:1.0.0

soda-check: ## Run Soda checks via Docker
	docker $(DOCKER_CONTEXT) run --rm \
		--network data-engineering-pipeline_pipeline-network \
		-e POSTGRES_HOST=postgres -e POSTGRES_PORT=5432 \
		-e POSTGRES_DB=weather_db -e POSTGRES_USER=airflow -e POSTGRES_PASSWORD=airflow \
		weather-pipeline-soda:1.0.0

# ============================================================
#  QUALITY
# ============================================================

lint: ## Run ruff linter
	uv run --extra dev ruff check .

lint-sql: ## Run SQLFluff on dbt models
	uv run --extra dev sqlfluff lint transformations/dbt/models/ || true

format: ## Auto-format Python code
	uv run --extra dev ruff format .

check: lint ## Run all checks (lint + format-check)
	uv run --extra dev ruff format --check .

pre-commit: ## Run pre-commit on all files
	uv run --extra dev pre-commit run --all-files

pre-commit-install: ## Install pre-commit hooks
	uv run --extra dev pre-commit install

# ============================================================
#  TESTS
# ============================================================

test: test-unit ## Run all tests

test-unit: ## Run unit tests only
	uv run --extra dev pytest tests/unit/ -v --tb=short -m "not integration"

test-integration: ## Run integration tests (requires Docker)
	uv run --extra dev pytest tests/integration/ -v --tb=short -m integration

test-cov: ## Run tests with coverage report
	uv run --extra dev pytest tests/ -v --tb=short --cov=ingestion --cov=analytics --cov=contracts --cov=monitoring --cov-report=term-missing

# ============================================================
#  OPS
# ============================================================

doctor: validate-env ## Check environment health
	@echo ""
	@echo "=== Environment Health Check ==="
	@echo ""
	@echo "OS: $(DETECTED_OS)"
	@echo -n "Docker: " && docker --version 2>/dev/null || echo "NOT INSTALLED"
	@echo -n "Docker Compose: " && docker compose version 2>/dev/null || echo "NOT INSTALLED"
	@echo -n "Python: " && python --version 2>/dev/null || echo "NOT INSTALLED"
	@echo -n "uv: " && uv --version 2>/dev/null || echo "NOT INSTALLED"
	@echo -n "Make: " && make --version 2>/dev/null | head -1 || echo "NOT INSTALLED"
	@echo ""
	@echo "Docker daemon:" && docker info --format '  Server Version: {{.ServerVersion}}' 2>/dev/null || echo "  NOT RUNNING"
	@echo ""

validate-env: ## Validate .env file
	@if [ ! -f "$(ENV_FILE)" ]; then \
		echo "ERROR: $(ENV_FILE) not found. Copy .env.example to .env"; \
		exit 1; \
	fi
	@echo ".env file: OK"
	@grep -q "OPENWEATHER_API_KEY" $(ENV_FILE) && echo "OPENWEATHER_API_KEY: set" || echo "WARNING: OPENWEATHER_API_KEY not set"
	@grep -q "POSTGRES_PASSWORD" $(ENV_FILE) && echo "POSTGRES_PASSWORD: set" || echo "WARNING: POSTGRES_PASSWORD not set"

urls: ## Show service URLs
	@echo ""
	@echo "=== Service URLs ==="
	@echo ""
	@echo "  Airflow UI:       http://localhost:8080  (airflow/airflow)"
	@echo "  Flower:           http://localhost:5555"
	@echo "  MinIO Console:    http://localhost:9001  (minioadmin/minioadmin)"
	@echo "  Grafana:          http://localhost:3000  (admin/admin)"
	@echo "  Prometheus:       http://localhost:9090"
	@echo "  Pushgateway:      http://localhost:9091"
	@echo "  PostgreSQL:       localhost:5432         (airflow/airflow)"
	@echo "  OpenMetadata:     http://localhost:8585  (if started with make start-metadata)"
	@echo ""

clean: ## Remove all containers, volumes, and built images
	$(DC_FULL) down -v --rmi local 2>/dev/null || true
	$(DC) down -v --rmi local 2>/dev/null || true
	@echo "Cleaned up containers, volumes, and local images."

prune: ## Docker system prune
	docker $(DOCKER_CONTEXT) system prune -a --volumes -f
