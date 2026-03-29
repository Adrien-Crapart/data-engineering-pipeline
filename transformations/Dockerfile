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

COPY transformations/scripts/ /app/scripts/
COPY transformations/scripts/entrypoint.sh /entrypoint.sh
RUN sed -i 's/\r$//' /entrypoint.sh /app/scripts/entrypoint.sh \
    && chmod +x /entrypoint.sh /app/scripts/entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["run"]
