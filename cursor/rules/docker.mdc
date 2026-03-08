# Docker Standards

## Image Versioning

- **NEVER** use `:latest` tag for any Docker image in docker-compose files or Dockerfiles.
- **ALWAYS** pin images to a specific semantic version (e.g., `postgres:16-alpine`, `grafana/grafana:11.4.0`).
- When adding a new service, look up the current stable version on Docker Hub and pin it.
- Document the pinned version choice in a comment if it's not obvious.

## Dockerfile Best Practices

- Always pin the base image version (e.g., `FROM apache/airflow:3.1.7-python3.12`).
- Use multi-stage builds when possible to reduce image size.
- Run as non-root user when possible.
- Group `RUN` commands to minimize layers.
- Clean up apt/apk caches in the same `RUN` layer.

## Docker Compose

- All services must have a `healthcheck` defined.
- All services must be on an explicit named network.
- Use environment variable substitution with defaults for all credentials.
- Never hardcode secrets in docker-compose files.
- Volumes must be named (not anonymous).
