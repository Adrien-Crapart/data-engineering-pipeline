# Infrastructure Reuse Rule

Applies to: `**/compose*.yaml`, `infrastructure/**`

## Core Principle

Always reuse existing services — never create redundant instances.

## Existing Shared Services

| Service | Host | Port | Credentials | Notes |
|---------|------|------|-------------|-------|
| PostgreSQL | `postgres` | 5432 | `airflow` / `airflow` | DB: `datawarehouse`; schemas: `staging`, `core`, `mart`, `analytic`, `elementary`, `staging_quarantine` |
| Redis | `redis` | 6379 | — | Airflow Celery broker/backend |
| MinIO | `minio` | 9000 (API), 9001 (console) | `minioadmin` / `minioadmin` | Buckets: `data-lake`, `airflow-logs` |

## What NOT to Do

```yaml
# FORBIDDEN
services:
  my_service_db:
    image: postgres:16      # NO — use existing postgres
  my_service_cache:
    image: redis:7          # NO — use existing redis
  my_service_storage:
    image: minio/minio      # NO — use existing minio
```

## What to Do Instead

**Need a new database?** Add via `infrastructure/scripts/init_db.sql`:
```sql
CREATE DATABASE new_db OWNER airflow;
```

**Need a new S3 bucket?** Add to `minio-init` entrypoint in `compose.data-lake.yaml`:
```bash
mc mb --ignore-existing myminio/new-bucket
```

**Need Redis for a new service?** Use a different DB number:
```yaml
environment:
  - REDIS_HOST=redis
  - REDIS_DB=2
```

## Network

All services connect to `pipeline-network`:
```yaml
networks:
  pipeline-network:
    external: true
    name: ${COMPOSE_PROJECT_NAME}_pipeline-network
```

## Exceptions (document with a comment)

A separate instance is only justified for:
1. Security isolation (compliance)
2. Version incompatibility
3. Critical performance isolation
