# Weather Data Engineering Pipeline

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![Airflow](https://img.shields.io/badge/Airflow-3.1-017cee?logo=apache-airflow)
![dbt](https://img.shields.io/badge/dbt-1.10-ff694b?logo=dbt)
![dlt](https://img.shields.io/badge/dlt-1.23-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql)
![MinIO](https://img.shields.io/badge/MinIO-S3_Data_Lake-red?logo=minio)
![Prometheus](https://img.shields.io/badge/Prometheus-v3.10-orange?logo=prometheus)
![Grafana](https://img.shields.io/badge/Grafana-12.4-F46800?logo=grafana)
![Soda](https://img.shields.io/badge/Soda_Core-3.5-green)
![DuckDB](https://img.shields.io/badge/DuckDB-1.x-yellow?logo=duckdb)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![Tests](https://img.shields.io/badge/tests-86_passing-brightgreen)

> Production-grade weather data pipeline demonstrating modern Data Engineering best practices.
> **API → Data Lake → Warehouse → Transformations → Quality → Observability → Catalog**

---

## Architecture

```mermaid
graph LR
    A[OpenWeather API] -->|dlt + contracts| B[MinIO Data Lake]
    B -->|raw JSON archive| C[(PostgreSQL)]
    C -->|raw schema| D["dbt (DockerOperator)"]
    D -->|staging + mart| E["Soda + Elementary (DockerOperator)"]
    E --> F[Prometheus + Grafana]
    F --> G[OpenMetadata Catalog]
    H[Airflow] -->|orchestrates| A
    H -->|orchestrates| D
    H -->|orchestrates| E
    H -->|orchestrates| G
    C -->|replay| I[Replay Pipeline]
```

## Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Orchestration | Apache Airflow 3.1 | DAG scheduling and monitoring |
| Ingestion | dlt (Data Load Tool) | API extraction with schema management |
| Data Lake | MinIO (S3-compatible) | Immutable raw data archive |
| Warehouse | PostgreSQL 16 | Layered schemas (raw, staging, mart) |
| Transformation | dbt-core (DockerOperator) | SQL models: staging → mart |
| Data Contracts | YAML + Python validator | Schema governance before storage |
| Data Quality | Soda Core + Great Expectations + Elementary | Automated validation and anomaly detection |
| Monitoring | Prometheus + Grafana | Pipeline metrics, dashboards, alerting |
| Metadata Catalog | OpenMetadata | Data catalog, lineage, profiling |
| Infrastructure | Docker Compose | Reproducible, versioned containers |
| CI/CD | GitHub Actions | Lint, test, build, dbt compile |

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/your-username/data-engineering-pipeline.git
cd data-engineering-pipeline

# 2. Set up environment
cp .env.example .env
# Edit .env with your OpenWeather API key (free at openweathermap.org/api)

# 3. Start the core platform
make up

# 4. (Optional) Start with OpenMetadata catalog
make up-full
```

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| OpenMetadata | http://localhost:8585 | — |

## Project Structure

```
data-engineering-pipeline/
├── analytics/           — DuckDB local analytics engine
├── contracts/           — Data contract YAML definitions and validator
├── cursor/              — Cursor AI rules for project conventions
├── data_quality/        — Soda Core checks and Great Expectations suites
├── docs/                — Architecture, pipeline, lineage, ADR documentation
├── infrastructure/      — Docker, Compose, init scripts, processing Dockerfile
├── ingestion/           — dlt pipelines, config, MinIO storage client
├── metadata/            — OpenMetadata ingestion configuration
├── monitoring/          — Prometheus config, Grafana dashboards, metrics exporter
├── orchestration/       — Airflow DAGs (weather, replay, metadata)
├── replay/              — Historical data replay from MinIO data lake
├── scripts/             — Utility shell scripts (setup, manual runs)
├── tests/               — Unit and integration tests (86+ tests)
└── transformations/     — dbt models (staging, marts), tests, Elementary
```

Each folder contains a `README.md` with detailed documentation.

## Pipeline Flow

```
extract_weather → dbt_deps → dbt_run → dbt_test → soda_scan → elementary_report → push_metrics
```

- **extract_weather**: dlt ingestion → validate contracts → archive to MinIO → load to PostgreSQL
- **dbt_***: Run in isolated Docker container via `DockerOperator`
- **soda_scan**: Run in isolated Docker container via `DockerOperator`
- **elementary_report**: Run in isolated Docker container via `DockerOperator`
- **push_metrics**: Push Prometheus metrics to Pushgateway

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| DockerOperator for processing | Airflow orchestrates only; dbt/Soda run in isolated containers |
| Pinned Docker image versions | Reproducible builds, no surprise breakages |
| MinIO as raw data lake | Immutable, replayable, auditable raw data archive |
| Data contracts before storage | Schema governance catches issues at ingestion time |
| Separate OpenMetadata compose | Lightweight core stack; catalog is optional for dev |
| Elementary for dbt observability | Volume anomalies, schema changes, freshness monitoring |

## Data Layers

| Schema | Layer | Description |
|--------|-------|-------------|
| `raw` | Raw | Unmodified API responses loaded by dlt |
| `staging` | Staging | Cleaned, typed, renamed views |
| `mart` | Mart | Business-ready analytical tables |
| `elementary` | Observability | dbt model monitoring metadata |

## Testing

| Category | Tool | Count | Scope |
|----------|------|-------|-------|
| Unit Tests | pytest | 43+ | Config, pipeline, contracts, MinIO client, metrics, replay, DAGs |
| dbt Tests | dbt test | 11 | Schema tests, data integrity, singular tests |
| Data Quality | Soda Core | 19 | Row counts, nulls, ranges, duplicates |
| Elementary | elementary | 7+ | Volume anomalies, schema changes |
| CI/CD | GitHub Actions | 5 jobs | Lint, test, Docker build, dbt compile, contract validation |

## Available Commands

```bash
make up          # Start core services (Airflow, PostgreSQL, MinIO, Prometheus, Grafana)
make up-full     # Start all services including OpenMetadata
make down        # Stop core services
make down-full   # Stop all services including OpenMetadata
make build       # Build Docker images
make logs        # Follow service logs
make psql        # Open PostgreSQL shell
make test        # Run unit tests in Docker
make status      # Show service status
make clean       # Full cleanup (containers + images + volumes)
```

## Documentation

- [Architecture](docs/architecture.md) — System design and service topology
- [Pipeline](docs/pipeline.md) — DAG details, task graph, error handling
- [Data Lineage](docs/lineage.md) — End-to-end and column-level lineage
- [ADR-001](docs/decisions/adr_001_architecture.md) — Architecture decisions

## Git Workflow

Feature branching strategy with conventional commits:

```
master
├── feature/project-setup
├── feature/openweather-ingestion
├── feature/dbt-transformations
├── feature/airflow-orchestration
├── feature/data-quality
├── feature/minio-data-lake
├── feature/data-contracts
├── feature/airflow-remote-logs
├── feature/replay-system
├── feature/monitoring
├── feature/observability
├── feature/openmetadata-catalog
├── feature/ci-cd
├── fix/cursor-rules
├── fix/docker-versioning
├── fix/folder-documentation
├── fix/add-missing-tests
├── fix/airflow-docker-operator
└── fix/update-documentation
```

---

*Built as a technical portfolio project for Data Engineering positions.*
