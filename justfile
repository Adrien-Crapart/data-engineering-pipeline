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
f_lineage        := compose_dir / "compose.lineage.yaml"

compose_all      := "-f " + f_warehouse + " -f " + f_lake + " -f " + f_airflow + " -f " + f_build + " -f " + f_monitoring + " -f " + f_lineage
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

# Start all core services (build + start, for first launch or after code changes)
start: build-all start-warehouse start-lake start-lineage start-airflow start-monitoring urls

# Start all services including OpenMetadata (build + start)
start-full: build-all start-warehouse start-lake start-lineage start-airflow start-monitoring start-metadata urls

# Start all core services WITHOUT rebuilding (fast resume)
[no-exit-message]
up: _up-core urls

# Start all services including OpenMetadata WITHOUT rebuilding (fast resume)
[no-exit-message]
up-full: _up-full-core urls

[private]
_up-core:
    @echo "--- Starting all core services (no rebuild) ---"
    {{ dc }} up -d

[private]
_up-full-core:
    @echo "--- Starting all services including OpenMetadata (no rebuild) ---"
    {{ dc_full }} up -d

# Stop core services (containers stopped, volumes kept)
down:
    {{ dc }} down

# Stop all services including OpenMetadata (containers stopped, volumes kept)
down-full:
    {{ dc_full }} down

# Stop core services and remove volumes (DESTRUCTIVE — data is lost)
stop:
    {{ dc }} down -v

# Stop all services including OpenMetadata and remove volumes (DESTRUCTIVE — data is lost)
stop-full:
    {{ dc_full }} down -v

# Restart core services
restart:
    {{ dc }} restart

# Restart all services including OpenMetadata
restart-full:
    {{ dc_full }} restart

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
        mailhog grafana metabase

# Start Kafka (OpenLineage event transport)
start-lineage:
    @echo "--- Starting Kafka (OpenLineage) ---"
    {{ dc_base }} -f {{ f_lineage }} up -d kafka
    @echo "Waiting for Kafka to be healthy..."
    @sleep 10

# Start OpenMetadata
start-metadata:
    @echo "--- Starting OpenMetadata ---"
    {{ dc_base }} -f {{ f_warehouse }} -f {{ f_metadata }} --profile openmetadata up -d

# Start Metabase only
start-metabase:
    @echo "--- Starting Metabase ---"
    {{ dc_base }} -f {{ f_warehouse }} -f {{ f_monitoring }} up -d metabase
    @echo "Waiting for Metabase to be healthy..."
    @sleep 20
    @echo "Metabase UI: http://localhost:3001"

# Stop Metabase
stop-metabase:
    @echo "--- Stopping Metabase ---"
    {{ dc_base }} -f {{ f_monitoring }} stop metabase

# Restart Metabase
restart-metabase:
    @echo "--- Restarting Metabase ---"
    {{ dc_base }} -f {{ f_monitoring }} restart metabase

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
#  SELF-REVIEW (pre-push gate)
# ============================================================

# Target branch for diff comparison (default: master)
review_base := "master"

# Full self-review: lint + secrets + tests + diff analysis
review base=review_base:
    @echo ""
    @echo "=============================================="
    @echo "  SELF-REVIEW — Pre-Push Gate"
    @echo "=============================================="
    @echo ""
    @echo "Branch: $(git branch --show-current)"
    @echo "Base:   {{ base }}"
    @echo "Commits: $(git rev-list --count {{ base }}..HEAD)"
    @echo ""
    @echo "--- [1/7] Diff stats ---"
    @git diff --stat {{ base }}...HEAD
    @echo ""
    @echo "Lines changed:"
    @git diff {{ base }}...HEAD --shortstat
    @echo ""
    @LINES=$$(git diff {{ base }}...HEAD --shortstat | grep -oP '\d+ insertion' | grep -oP '\d+' || echo 0); \
        DEL=$$(git diff {{ base }}...HEAD --shortstat | grep -oP '\d+ deletion' | grep -oP '\d+' || echo 0); \
        TOTAL=$$((LINES + DEL)); \
        if [ "$$TOTAL" -gt 1500 ]; then \
            echo "⚠  WARNING: $$TOTAL lines changed (> 1500 hard limit). Consider splitting."; \
        elif [ "$$TOTAL" -gt 500 ]; then \
            echo "⚡ INFO: $$TOTAL lines changed (> 500 target). Ensure PR is well-structured."; \
        else \
            echo "✓  $$TOTAL lines changed — within target."; \
        fi
    @echo ""
    @echo "--- [2/7] Secret scan ---"
    @SECRETS_FOUND=0; \
        for pattern in "password=" "api_key=" "secret=" "token=" "AWS_SECRET" "PRIVATE_KEY"; do \
            MATCHES=$$(git diff {{ base }}...HEAD | grep -i "^+" | grep -i "$$pattern" | grep -v ".example" | grep -v ".mdc" || true); \
            if [ -n "$$MATCHES" ]; then \
                echo "⚠  Potential secret found (pattern: $$pattern):"; \
                echo "$$MATCHES"; \
                SECRETS_FOUND=1; \
            fi; \
        done; \
        if [ "$$SECRETS_FOUND" = "0" ]; then \
            echo "✓  No secrets detected in diff."; \
        else \
            echo ""; \
            echo "⚠  Review above matches — ensure no real credentials are committed."; \
        fi
    @echo ""
    @echo "--- [3/7] Debug artifacts ---"
    @DEBUG_FOUND=0; \
        PRINTS=$$(git diff {{ base }}...HEAD | grep -n "^+.*print(" | grep -v "test_" | grep -v "#" || true); \
        if [ -n "$$PRINTS" ]; then \
            echo "⚠  print() statements found (use logging instead):"; \
            echo "$$PRINTS"; \
            DEBUG_FOUND=1; \
        fi; \
        TODOS=$$(git diff {{ base }}...HEAD | grep -n "^+.*TODO\|^+.*FIXME\|^+.*HACK\|^+.*XXX" || true); \
        if [ -n "$$TODOS" ]; then \
            echo "⚠  TODO/FIXME markers found:"; \
            echo "$$TODOS"; \
            DEBUG_FOUND=1; \
        fi; \
        SELECT_STAR=$$(git diff {{ base }}...HEAD -- '*.sql' | grep -i "^+.*SELECT \*" || true); \
        if [ -n "$$SELECT_STAR" ]; then \
            echo "⚠  SELECT * found in SQL files:"; \
            echo "$$SELECT_STAR"; \
            DEBUG_FOUND=1; \
        fi; \
        if [ "$$DEBUG_FOUND" = "0" ]; then \
            echo "✓  No debug artifacts found."; \
        fi
    @echo ""
    @echo "--- [4/7] Commit hygiene ---"
    @COMMITS=$$(git log {{ base }}..HEAD --oneline); \
        echo "$$COMMITS"; \
        echo ""; \
        BAD=$$(echo "$$COMMITS" | grep -ivE "^[a-f0-9]+ (feat|fix|docs|refactor|test|chore|ci|style|perf|build|revert)\(" || true); \
        if [ -n "$$BAD" ]; then \
            echo "⚠  Non-conventional commits detected:"; \
            echo "$$BAD"; \
            echo "   Expected format: type(scope): description"; \
        else \
            echo "✓  All commits follow conventional format."; \
        fi
    @echo ""
    @echo "--- [5/7] Python lint + format ---"
    @uv run --extra dev ruff check . && echo "✓  Ruff lint passed." || echo "✗  Ruff lint FAILED."
    @uv run --extra dev ruff format --check . && echo "✓  Ruff format passed." || echo "✗  Ruff format FAILED."
    @echo ""
    @echo "--- [6/7] Unit tests ---"
    @uv run --extra dev pytest tests/unit/ -v --tb=short -m "not integration" -q && echo "✓  Tests passed." || echo "✗  Tests FAILED."
    @echo ""
    @echo "--- [7/7] Changed file summary ---"
    @echo ""
    @echo "Python files:"
    @git diff --name-only {{ base }}...HEAD -- '*.py' | head -30 || echo "  (none)"
    @echo ""
    @echo "SQL files:"
    @git diff --name-only {{ base }}...HEAD -- '*.sql' | head -30 || echo "  (none)"
    @echo ""
    @echo "YAML/Config files:"
    @git diff --name-only {{ base }}...HEAD -- '*.yaml' '*.yml' '*.toml' '*.cfg' | head -30 || echo "  (none)"
    @echo ""
    @echo "Docker files:"
    @git diff --name-only {{ base }}...HEAD -- '*Dockerfile*' '*compose*' | head -30 || echo "  (none)"
    @echo ""
    @echo "DAG files:"
    @git diff --name-only {{ base }}...HEAD -- '*dags*' | head -30 || echo "  (none)"
    @echo ""
    @echo "=============================================="
    @echo "  SELF-REVIEW COMPLETE"
    @echo "=============================================="
    @echo ""
    @echo "Manual checks remaining (see .cursor/rules/self-review.mdc):"
    @echo "  □ Data correctness verified on sample data"
    @echo "  □ dbt dependencies correct (no cycles)"
    @echo "  □ DAG task ordering correct"
    @echo "  □ Docker images build if infra changed"
    @echo "  □ PR description ready with review strategy"
    @echo "  □ Can explain this PR in under 2 minutes"
    @echo ""

# Quick review: diff stats + secret scan only (no tests)
review-quick base=review_base:
    @echo ""
    @echo "=== Quick Review ({{ base }}..HEAD) ==="
    @echo ""
    @echo "Branch: $(git branch --show-current)"
    @echo "Commits: $(git rev-list --count {{ base }}..HEAD)"
    @echo ""
    @git diff --stat {{ base }}...HEAD
    @echo ""
    @git diff {{ base }}...HEAD --shortstat
    @echo ""
    @echo "--- Secret scan ---"
    @git diff {{ base }}...HEAD | grep -i "^+" | grep -iE "password=|api_key=|secret=|token=" | grep -v ".example" | grep -v ".mdc" || echo "✓  No secrets detected."
    @echo ""
    @echo "--- Conventional commits ---"
    @git log {{ base }}..HEAD --oneline
    @echo ""

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
    echo "  Metabase:         http://localhost:3001  (setup on first launch)"
    echo "  Prometheus:       http://localhost:9090"
    echo "  Pushgateway:      http://localhost:9091"
    echo "  Alertmanager:     http://localhost:9093"
    echo "  Loki:             http://localhost:3100"
    echo "  MailHog:          http://localhost:8025"
    echo "  StatsD Exporter:  http://localhost:9102/metrics"
    echo "  Docker Exporter:  http://localhost:9417/metrics"
    echo "  PostgreSQL:       localhost:5432         (airflow/airflow)"
    echo "  Kafka:            localhost:9094         (OpenLineage events)"
    echo "  OpenMetadata:     http://localhost:8585  (if started with just start-metadata)"
    echo ""

# Bootstrap OpenMetadata governance (classifications, glossary, teams, tiers, domains)
om-bootstrap *args='':
    @echo "--- Bootstrapping OpenMetadata configuration ---"
    @{{ if os() == "windows" { "powershell -Command \"uv run --extra dev python metadata/om_bootstrap.py " + args + "\"" } else { "uv run --extra dev python metadata/om_bootstrap.py " + args } }}

# Export current OpenMetadata configuration to JSON
om-export:
    @echo "--- Exporting OpenMetadata configuration ---"
    @{{ if os() == "windows" { "powershell -Command \"uv run --extra dev python metadata/om_bootstrap.py --export-only\"" } else { "uv run --extra dev python metadata/om_bootstrap.py --export-only" } }}

# Full OM setup: tests, profiler, Airflow service, column descriptions, agent triggers
om-setup *args='':
    @echo "--- Full OpenMetadata setup (tests + profiler + services) ---"
    @{{ if os() == "windows" { "powershell -Command \"uv run --extra dev python metadata/om_setup_full.py " + args + "\"" } else { "uv run --extra dev python metadata/om_setup_full.py " + args } }}

# Create and deploy TestSuite agent for automated quality checks
om-create-test-agent schedule="0 */6 * * *":
    @echo "--- Creating TestSuite agent (schedule: {{ schedule }}) ---"
    @{{ if os() == "windows" { "powershell -Command \"uv run --extra dev python metadata/om_create_test_agent.py --schedule '" + schedule + "'\"" } else { "uv run --extra dev python metadata/om_create_test_agent.py --schedule '" + schedule + "'" } }}

# Run data quality tests on-demand
om-run-tests wait="":
    @echo "--- Triggering data quality tests ---"
    @{{ if os() == "windows" { if wait == "wait" { "powershell -Command \"uv run --extra dev python metadata/om_run_tests.py --wait\"" } else { "powershell -Command \"uv run --extra dev python metadata/om_run_tests.py\"" } } else { if wait == "wait" { "uv run --extra dev python metadata/om_run_tests.py --wait" } else { "uv run --extra dev python metadata/om_run_tests.py" } } }}

# Create observability alerts for data quality monitoring
om-create-alerts slack="" email="":
    @echo "--- Creating observability alerts ---"
    @{{ if os() == "windows" { "powershell -Command \"uv run --extra dev python metadata/om_create_alerts.py" + (if slack != "" { " --slack-webhook " + slack } else { "" }) + (if email != "" { " --email " + email } else { "" }) + "\"" } else { "uv run --extra dev python metadata/om_create_alerts.py" + (if slack != "" { " --slack-webhook " + slack } else { "" }) + (if email != "" { " --email " + email } else { "" }) } }}

# Complete data quality setup (agent + alerts)
om-quality-setup schedule="0 */6 * * *" slack="" email="":
    @echo "--- Complete data quality setup ---"
    @echo "Step 1/2: Creating TestSuite agent..."
    @{{ if os() == "windows" { "powershell -Command \"uv run --extra dev python metadata/om_create_test_agent.py --schedule '" + schedule + "'\"" } else { "uv run --extra dev python metadata/om_create_test_agent.py --schedule '" + schedule + "'" } }}
    @echo ""
    @echo "Step 2/2: Creating observability alerts..."
    @{{ if os() == "windows" { "powershell -Command \"uv run --extra dev python metadata/om_create_alerts.py" + (if slack != "" { " --slack-webhook " + slack } else { "" }) + (if email != "" { " --email " + email } else { "" }) + "\"" } else { "uv run --extra dev python metadata/om_create_alerts.py" + (if slack != "" { " --slack-webhook " + slack } else { "" }) + (if email != "" { " --email " + email } else { "" }) } }}
    @echo ""
    @echo "✓ Data quality setup complete!"

# Remove all containers, volumes, and built images
clean:
    {{ dc_full }} down -v --rmi local 2>/dev/null || true
    {{ dc }} down -v --rmi local 2>/dev/null || true
    @echo "Cleaned up containers, volumes, and local images."

# Docker system prune
prune:
    docker system prune -a --volumes -f
