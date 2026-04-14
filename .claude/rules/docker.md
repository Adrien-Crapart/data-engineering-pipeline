# Docker Standards

Applies to: `**/Dockerfile`, `**/compose*.yaml`, `**/docker-compose*.yaml`

## Mandatory: Test Every Container Change

When any Docker container is created or modified:

1. Build: `just build-<service>`
2. Start: `just up` or `just start`
3. Check logs: `just logs <service>`
4. Verify health check passes (status "healthy")
5. Confirm the service is reachable

**Never** consider work done without completing these steps.

## Image Versioning

- **NEVER** use `:latest` tag in Compose files.
- **ALWAYS** pin to a specific semantic version: `postgres:16-alpine`, `grafana/grafana:12.4.0`.
- Current pinned versions are tracked in `.claude/rules/version-verification.md`.

## Dockerfile Best Practices

```dockerfile
# Pin base image
FROM python:3.12-slim

# Use uv via COPY from its image — never curl
COPY --from=ghcr.io/astral-sh/uv:0.5 /uv /uvx /bin/

# Use cache mounts for dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Run as non-root
RUN useradd -m appuser
USER appuser
```

- Multi-stage builds to reduce image size.
- Group `RUN` commands to minimize layers.
- Clean apt/apk caches in the same `RUN` layer.
- `pyproject.toml` is the single source of truth — never hardcode deps in Dockerfiles.
- **No `requirements.txt`** — delete if found, migrate to `pyproject.toml`.

## Health Checks (Mandatory for Every Service)

| Service | Test | Interval | Timeout | Retries | Start Period |
|---------|------|----------|---------|---------|--------------|
| PostgreSQL | `pg_isready -U $USER -d $DB` | 30s | 10s | 3 | 40s |
| Redis | `redis-cli ping` | 30s | 5s | 3 | 20s |
| MinIO | `curl -f http://localhost:9000/minio/health/live` | 30s | 5s | 3 | 10s |
| Airflow | `curl -f http://localhost:8080/health` | 30s | 10s | 3 | 60s |
| Prometheus | `wget --spider http://localhost:9090/-/healthy` | 30s | 5s | 3 | 10s |
| Grafana | `curl -f http://localhost:3000/api/health` | 30s | 10s | 3 | 30s |

## Compose Service Standards

```yaml
services:
  myservice:
    image: vendor/image:1.2.3          # pinned, never :latest
    restart: unless-stopped
    healthcheck:
      test: [...]
      interval: 30s
    networks:
      - pipeline-network
    depends_on:
      postgres:
        condition: service_healthy     # wait for health, not just start
    deploy:
      resources:
        limits:
          memory: 2G
        reservations:
          memory: 1G
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
```

- All services must be on `pipeline-network` (external, named).
- Never hardcode secrets — use environment variable substitution with defaults.
- Volumes must be named (never anonymous).
- DockerOperator memory limits: `DLT_MEM_LIMIT` (512m), `DBT_MEM_LIMIT` (512m), `SODA_MEM_LIMIT` (256m).

## Security

- Non-root users when possible.
- Mount volumes as `:ro` when write access is not needed.
- Never expose the Docker socket unless required (Airflow DockerOperator is the exception).
- `.dockerignore` must exclude: `.env`, `.git`, `__pycache__`, `.venv`.
