# ============================================================
# Data Engineering Pipeline — justfile
# Cross-platform: Linux, macOS, Windows (Git Bash / WSL)
# ============================================================

set shell := ["bash", "-cu"]
set dotenv-load := true

# --- Config ---
env_file         := ".env"
compose_dir      := "infrastructure/docker"

f_warehouse      := compose_dir / "compose.data-warehouse.yaml"
f_lake           := compose_dir / "compose.data-lake.yaml"
f_airflow        := compose_dir / "compose.orchestration.yaml"
f_build          := compose_dir / "compose.build-images.yaml"
f_monitoring     := compose_dir / "compose.monitoring.yaml"
f_metadata       := compose_dir / "compose.metadata.yaml"

compose_all      := "-f " + f_warehouse + " -f " + f_lake + " -f " + f_airflow + " -f " + f_build + " -f " + f_monitoring
compose_full     := compose_all + " -f " + f_metadata + " --profile openmetadata"

project_name     := "data-engineering-pipeline"
network_name     := project_name + "_pipeline-network"

dc_base          := "docker compose -p " + project_name + " --project-directory . --env-file " + env_file
dc_build         := "docker compose -p " + project_name + " --env-file " + env_file

dc               := dc_base + " " + compose_all
dc_full          := dc_base + " " + compose_full

# ============================================================
#  HELP (default recipe)
# ============================================================

@_default:
    just --list --unsorted

# ============================================================
#  LIFECYCLE
# ============================================================

# Start all core services (ordered)
start: build-all start-warehouse start-lake start-airflow start-monitoring urls

# Start all services including OpenMetadata
start-full: build-all start-warehouse start-lake start-airflow start-monitoring start-metadata urls

# Stop core services and remove volumes
stop:
    {{ dc }} down -v

# Stop all services including OpenMetadata
stop-full:
    {{ dc_full }} down -v

# Restart core services
restart:
    {{ dc }} restart

# Show service status
status:
    {{ dc_full }} ps 2>/dev/null || {{ dc }} ps

# Follow service logs (optionally filter by service name)
logs *ARGS:
    {{ dc }} logs -f {{ ARGS }}

# ============================================================
#  PER-STACK TARGETS
# ============================================================

# Start PostgreSQL + Redis
start-warehouse:
    @echo "--- Starting Data Warehouse (PostgreSQL + Redis) ---"
    {{ dc_base }} -f {{ f_warehouse }} up -d
    @echo "Waiting for warehouse to be healthy..."
    @sleep 10

# Start MinIO
start-lake:
    @echo "--- Starting Data Lake (MinIO) ---"
    {{ dc_base }} -f {{ f_warehouse }} -f {{ f_lake }} up -d minio minio-init
    @echo "Waiting for MinIO to be healthy..."
    @sleep 5

# Start Airflow stack
start-airflow: build-airflow
    @echo "--- Starting Airflow ---"
    {{ dc_base }} -f {{ f_warehouse }} -f {{ f_lake }} -f {{ f_airflow }} up -d
    @echo "Waiting for Airflow to initialize..."
    @sleep 15

# Start full monitoring stack
start-monitoring:
    @echo "--- Starting Monitoring Stack ---"
    {{ dc_base }} -f {{ f_warehouse }} -f {{ f_lake }} -f {{ f_monitoring }} up -d \
        statsd-exporter prometheus prometheus-pushgateway alertmanager \
        node-exporter docker-exporter \
        loki promtail \
        mailhog grafana

# Start OpenMetadata
start-metadata:
    @echo "--- Starting OpenMetadata ---"
    {{ dc_base }} -f {{ f_warehouse }} -f {{ f_metadata }} --profile openmetadata up -d

# ============================================================
#  BUILD & NETWORK
# ============================================================

# Create the pipeline Docker network if it doesn't exist
ensure-network:
    @docker network inspect {{ network_name }} >/dev/null 2>&1 \
        || docker network create {{ network_name }} >/dev/null 2>&1 \
        || true
    @echo "Network {{ network_name }}: OK"

# Build Airflow image
build-airflow:
    @echo "--- Building Airflow image ---"
    {{ dc_base }} -f {{ f_airflow }} build

# Build Airflow image (alias)
build: build-airflow

# Build DLT runner image
build-dlt:
    @echo "--- Building DLT runner image ---"
    {{ dc_build }} -f {{ f_build }} --profile build-only build dlt-runner

# Build dbt runner image
build-dbt:
    @echo "--- Building dbt runner image ---"
    {{ dc_build }} -f {{ f_build }} --profile build-only build dbt-runner

# Build Soda runner image
build-soda:
    @echo "--- Building Soda runner image ---"
    {{ dc_build }} -f {{ f_build }} --profile build-only build soda-runner

# Build all images
build-all: build-airflow build-dlt build-dbt build-soda

# Force-rebuild all images (no cache)
rebuild: ensure-network
    @echo "--- Force rebuilding all images (no cache) ---"
    {{ dc_base }} -f {{ f_airflow }} build --no-cache
    {{ dc_build }} -f {{ f_build }} --profile build-only build --no-cache dlt-runner
    {{ dc_build }} -f {{ f_build }} --profile build-only build --no-cache dbt-runner
    {{ dc_build }} -f {{ f_build }} --profile build-only build --no-cache soda-runner
    @echo "All images rebuilt. Run 'just restart' to apply."

# ============================================================
#  DEV TOOLS
# ============================================================

# Open psql shell on datawarehouse
psql:
    {{ dc_base }} -f {{ f_warehouse }} exec postgres psql -U datawarehouse_user -d datawarehouse

# Open bash in Airflow worker
airflow-shell:
    {{ dc_base }} -f {{ f_warehouse }} -f {{ f_lake }} -f {{ f_airflow }} exec airflow-worker bash

# Run dbt models via Docker
dbt-run *ARGS: ensure-network
    docker run --rm \
        --network {{ network_name }} \
        -v "$(pwd)/transformations:/app" \
        --env-file {{ env_file }} \
        weather-pipeline-dbt:1.0.0 run {{ ARGS }}

# Run dbt tests via Docker
dbt-test *ARGS: ensure-network
    docker run --rm \
        --network {{ network_name }} \
        -v "$(pwd)/transformations:/app" \
        --env-file {{ env_file }} \
        weather-pipeline-dbt:1.0.0 test {{ ARGS }}

# Run DLT ingestion via Docker (default: openweather pipeline)
dlt-run *ARGS: ensure-network
    docker run --rm -it \
        --network {{ network_name }} \
        --env-file {{ env_file }} \
        -e PYTHONUNBUFFERED=1 \
        weather-pipeline-dlt:1.0.0 {{ ARGS }}

# Run a specific ingestion pipeline with custom cities
dlt-run-openweather *ARGS: ensure-network
    docker run --rm -it \
        --network {{ network_name }} \
        --env-file {{ env_file }} \
        -e PYTHONUNBUFFERED=1 \
        weather-pipeline-dlt:1.0.0 --pipeline openweather {{ ARGS }}

# Generate dbt docs and upload to S3
dbt-docs: ensure-network
    docker run --rm \
        --network {{ network_name }} \
        -v "$(pwd)/transformations:/app" \
        --env-file {{ env_file }} \
        weather-pipeline-dbt:1.0.0 docs

# Run Soda checks via Docker (with S3 report upload)
soda-check layer="staging": ensure-network
    docker run --rm \
        --network {{ network_name }} \
        --env-file {{ env_file }} \
        -e MINIO_ENDPOINT=http://minio:9000 \
        -v "$(pwd)/data_quality:/app/data_quality" \
        weather-pipeline-soda:1.0.0 \
        data_quality.soda.run_scan --layer {{ layer }}

# Run Great Expectations validations via Docker (with S3 report upload)
gx-check layer="all": ensure-network
    docker run --rm \
        --network {{ network_name }} \
        --env-file {{ env_file }} \
        -e MINIO_ENDPOINT=http://minio:9000 \
        -v "$(pwd)/data_quality:/app/data_quality" \
        weather-pipeline-soda:1.0.0 \
        data_quality.expectations.run_validations --layer {{ layer }}

# ============================================================
#  QUALITY
# ============================================================

# Run ruff linter
lint:
    uv run --extra dev ruff check .

# Run SQLFluff on dbt models
lint-sql:
    uv run --extra dev sqlfluff lint transformations/models/ || true

# Auto-format Python code
format:
    uv run --extra dev ruff format .

# Run all checks (lint + format-check)
check: lint
    uv run --extra dev ruff format --check .

# Run pre-commit on all files
pre-commit:
    uv run --extra dev pre-commit run --all-files

# Install pre-commit hooks
pre-commit-install:
    uv run --extra dev pre-commit install

# ============================================================
#  TESTS
# ============================================================

# Run all tests
test: test-unit

# Run unit tests only
test-unit:
    uv run --extra dev pytest tests/unit/ -v --tb=short -m "not integration"

# Run integration tests (requires Docker)
test-integration:
    uv run --extra dev pytest tests/integration/ -v --tb=short -m integration

# Run tests with coverage report
test-cov:
    uv run --extra dev pytest tests/ -v --tb=short --cov=ingestion --cov=analytics --cov=contracts --cov=monitoring --cov-report=term-missing

# ============================================================
#  OPS
# ============================================================

# Check environment health
doctor: validate-env
    @echo ""
    @echo "=== Environment Health Check ==="
    @echo ""
    @echo "OS: {{ os() }} / {{ arch() }}"
    @echo -n "Docker: " && docker --version 2>/dev/null || echo "NOT INSTALLED"
    @echo -n "Docker Compose: " && docker compose version 2>/dev/null || echo "NOT INSTALLED"
    @echo -n "Python: " && python3 --version 2>/dev/null || python --version 2>/dev/null || echo "NOT INSTALLED"
    @echo -n "uv: " && (uv --version 2>/dev/null || uv.exe --version 2>/dev/null || echo "NOT INSTALLED")
    @echo -n "just: " && (just --version 2>/dev/null || just.exe --version 2>/dev/null || echo "NOT INSTALLED")
    @echo ""
    @docker info >/dev/null 2>&1 && echo "Docker daemon: running" || echo "Docker daemon: NOT RUNNING"
    @echo ""

# Validate .env file
validate-env:
    @test -f "{{ env_file }}" || (echo "ERROR: {{ env_file }} not found. Copy .env.example to .env" && exit 1)
    @echo ".env file: OK"
    @grep -q "OPENWEATHER_API_KEY" "{{ env_file }}" && echo "OPENWEATHER_API_KEY: set" || echo "WARNING: OPENWEATHER_API_KEY not set"
    @grep -q "POSTGRES_PASSWORD" "{{ env_file }}" && echo "POSTGRES_PASSWORD: set" || echo "WARNING: POSTGRES_PASSWORD not set"

# Show service URLs
@urls:
    echo ""
    echo "=== Service URLs ==="
    echo ""
    echo "  Airflow UI:       http://localhost:8080  (airflow/airflow)"
    echo "  Flower:           http://localhost:5555"
    echo "  MinIO Console:    http://localhost:9001  (minioadmin/minioadmin)"
    echo "  Grafana:          http://localhost:3000  (admin/admin)"
    echo "  Prometheus:       http://localhost:9090"
    echo "  Pushgateway:      http://localhost:9091"
    echo "  Alertmanager:     http://localhost:9093"
    echo "  Loki:             http://localhost:3100"
    echo "  MailHog:          http://localhost:8025"
    echo "  StatsD Exporter:  http://localhost:9102/metrics"
    echo "  Docker Exporter:  http://localhost:9417/metrics"
    echo "  PostgreSQL:       localhost:5432         (airflow/airflow)"
    echo "  OpenMetadata:     http://localhost:8585  (if started with just start-metadata)"
    echo ""

# Remove all containers, volumes, and built images
clean:
    {{ dc_full }} down -v --rmi local 2>/dev/null || true
    {{ dc }} down -v --rmi local 2>/dev/null || true
    @echo "Cleaned up containers, volumes, and local images."

# Docker system prune
prune:
    docker system prune -a --volumes -f
