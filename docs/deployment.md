# Deployment Guide

Complete deployment guide for the Data Engineering Pipeline.

## Prerequisites

- Docker >= 20.10.0
- Docker Compose >= v2.2.3
- 12 GB RAM minimum (core stack + OpenMetadata)
- 8 GB RAM minimum (core stack only)

## Minimum Resource Requirements

### Core Stack (~8 GB RAM, ~7 CPU)

| Service | RAM Limit | CPU Limit | Purpose |
|---------|-----------|-----------|---------|
| postgres | 512 MB | 0.50 | Data warehouse (mart + staging) |
| redis | 256 MB | 0.25 | Celery message broker |
| airflow-api-server | 1 GB | 1.00 | Airflow REST API and UI |
| airflow-scheduler | 1 GB | 1.00 | DAG scheduling |
| airflow-dag-processor | 512 MB | 0.50 | DAG file parsing |
| airflow-worker | 2 GB | 1.50 | Celery worker (runs DockerOperator) |
| flower | 256 MB | 0.25 | Celery monitoring UI |
| minio | 512 MB | 0.50 | S3-compatible data lake |
| prometheus | 256 MB | 0.25 | Metrics collection |
| prometheus-pushgateway | 128 MB | 0.25 | Pipeline metrics push endpoint |
| grafana | 256 MB | 0.50 | Dashboards and alerting |

### OpenMetadata Stack (~4 GB RAM, ~3 CPU)

| Service | RAM Limit | CPU Limit | Purpose |
|---------|-----------|-----------|---------|
| openmetadata-mysql | 512 MB | 0.50 | OM metadata database |
| openmetadata-elasticsearch | 1 GB | 1.00 | Search engine (ES 9.x) |
| openmetadata-server | 1.5 GB | 1.00 | Catalog API server |
| openmetadata-ingestion | 1 GB | 0.50 | Metadata ingestion (embedded Airflow) |

### DockerOperator Containers (configurable via environment)

| Container | Default RAM | Default CPU | Environment Variable |
|-----------|-------------|-------------|---------------------|
| dlt-runner | 512 MB | 0.50 | `DLT_MEM_LIMIT`, `DLT_CPU_LIMIT` |
| dbt-runner | 512 MB | 0.50 | `DBT_MEM_LIMIT`, `DBT_CPU_LIMIT` |
| soda-runner | 256 MB | 0.25 | `SODA_MEM_LIMIT`, `SODA_CPU_LIMIT` |

## Quick Start

```bash
# 1. Clone and configure
git clone <repo-url>
cd data-engineering-pipeline
cp .env.example .env
# Edit .env: set OPENWEATHER_API_KEY and PROJECT_ROOT

# 2. Start core stack
make up

# 3. (Optional) Start with OpenMetadata
make up-full

# 4. Verify all services are healthy
make status
```

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| Flower (Celery) | http://localhost:5555 | -- |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | -- |
| OpenMetadata | http://localhost:8585 | -- |

## Environment Variables

See [`.env.example`](../.env.example) for all available variables. Key ones:

| Variable | Description | Required |
|----------|-------------|----------|
| `OPENWEATHER_API_KEY` | OpenWeather API key | Yes |
| `PROJECT_ROOT` | Absolute path to project root | Yes (for DockerOperator mounts) |
| `COMPOSE_PROJECT_NAME` | Docker Compose project name | No (default: `data-engineering-pipeline`) |
| `DLT_MEM_LIMIT` | Memory limit for dlt container | No (default: `512m`) |

## Clean Redeploy

```bash
# Full teardown (removes all data!)
make clean

# Or manually:
docker compose --env-file .env \
  -f infrastructure/docker/docker-compose.yml \
  -f infrastructure/docker/docker-compose.openmetadata.yml \
  down -v
docker volume prune -f

# Rebuild and start
make up
```

## Troubleshooting

### Airflow worker cannot connect to Docker socket

Ensure `/var/run/docker.sock` is accessible. On Linux:

```bash
sudo chmod 666 /var/run/docker.sock
```

On Docker Desktop (Windows/Mac), Docker socket access is provided automatically.

### OpenMetadata Elasticsearch fails to start

ES 9.x requires `vm.max_map_count >= 262144`. On Linux:

```bash
sudo sysctl -w vm.max_map_count=262144
```

### DuckDB cannot read from MinIO S3

Verify S3 settings in dbt `profiles.yml` match your MinIO configuration:
- `s3_endpoint` must point to `minio:9000` (from inside Docker) or `localhost:9000` (from host)
- `s3_use_ssl` must be `false` for local MinIO
- `s3_url_style` must be `path` (not virtual-hosted)
