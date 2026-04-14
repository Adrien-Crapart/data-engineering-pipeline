# Version Verification Protocol

## Core Rule

**Before answering any question about a tool or service in this project, verify the actual running version** by checking:
1. `docker ps --format "table {{.Names}}\t{{.Image}}"`
2. Compose files in `infrastructure/docker/` for pinned image tags
3. `pyproject.toml` files for Python dependency versions
4. The official documentation **for that specific version**

Different versions of the same tool can have completely different UIs, APIs, and terminology (e.g., OpenMetadata 1.12.x renamed "Ingestion" → "Agents").

## Pinned Versions (as of 2026-04-02)

| Component | Version | Image |
|-----------|---------|-------|
| OpenMetadata Server | 1.12.3 | `docker.getcollate.io/openmetadata/server:1.12.3` |
| OpenMetadata Ingestion | 1.12.3 | `docker.getcollate.io/openmetadata/ingestion:1.12.3` |
| Elasticsearch | 9.0.2 | `docker.elastic.co/elasticsearch/elasticsearch:9.0.2` |
| PostgreSQL | 16 | `postgres:16-alpine` |
| Apache Airflow | 3.x | Custom build (`orchestration/Dockerfile`) |
| MinIO | RELEASE.2025-09-07 | `minio/minio:RELEASE.2025-09-07T16-13-09Z` |
| Apache Kafka | 4.0.0 | `apache/kafka:4.0.0` |
| Grafana | 12.4.0 | `grafana/grafana:12.4.0` |
| Prometheus | 3.2.1 | `prom/prometheus:v3.2.1` |
| Alertmanager | 0.28.1 | `prom/alertmanager:v0.28.1` |
| Loki | 3.4.2 | `grafana/loki:3.4.2` |
| Redis | 7.x | `redis:7-alpine` |
| Metabase | 0.51.7 | `metabase/metabase:v0.51.7` |
| dbt-core | 1.11.x | Custom build (`transformations/Dockerfile`) |
| dbt-duckdb | 1.10.x | Custom build (`transformations/Dockerfile`) |

## Versioned Documentation URLs

| Tool | Docs |
|------|------|
| OpenMetadata | `https://docs.open-metadata.org/v1.12.x/` |
| Apache Airflow | `https://airflow.apache.org/docs/apache-airflow/3.0.0/` |
| Grafana | `https://grafana.com/docs/grafana/v12.4/` |
| Prometheus | `https://prometheus.io/docs/prometheus/3.2/` |
| MinIO | `https://min.io/docs/minio/linux/` |

## Update Protocol

After verifying a version or discovering a version-specific behavior:
1. Update the version table above
2. Update `.claude/agents/openmetadata-agent.md` if OM-specific
3. Update `GUIDE_OPENMETADATA.md` with correct paths/terminology
