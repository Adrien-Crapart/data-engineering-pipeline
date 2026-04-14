FROM python:3.12-slim-bookworm AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_DEV=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv

WORKDIR /build

COPY transformations/pyproject.toml pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-install-project

FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash git postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Bake project files into /opt/dbt-project as a bind-mount fallback.
# On Windows Docker Desktop, the bind mount at /app can silently produce an
# empty directory when the host path is unreachable. The entrypoint detects
# this and copies these defaults into /app before running dbt.
RUN mkdir -p /opt/dbt-project/analyses /opt/dbt-project/seeds /opt/dbt-project/snapshots \
             /opt/dbt-project/macros /opt/dbt-project/models /opt/dbt-project/tests

COPY transformations/dbt_project.yml  /opt/dbt-project/dbt_project.yml
COPY transformations/profiles.yml     /opt/dbt-project/profiles.yml
COPY transformations/packages.yml     /opt/dbt-project/packages.yml
COPY transformations/models/          /opt/dbt-project/models/
COPY transformations/tests/           /opt/dbt-project/tests/
COPY transformations/macros/          /opt/dbt-project/macros/

COPY transformations/scripts/ /app/scripts/
COPY transformations/scripts/entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh /app/scripts/entrypoint.sh \
    && chmod +x /entrypoint.sh /app/scripts/entrypoint.sh

# Pre-install dbt packages so the image is self-contained.
# The entrypoint fallback copies /opt/dbt-project → /app, so packages will
# already be present when dbt deps runs on first use (avoids the parse-time
# chicken-and-egg failure on dbt 1.11.x and speeds up cold starts).
RUN cd /opt/dbt-project \
    && /opt/venv/bin/dbt deps --profiles-dir . --project-dir .

ENTRYPOINT ["/entrypoint.sh"]
CMD ["run"]
