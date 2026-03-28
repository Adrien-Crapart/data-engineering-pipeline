FROM python:3.12-slim-bookworm AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_DEV=1

WORKDIR /app

COPY ingestion/pyproject.toml pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --no-install-project

FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app

ENTRYPOINT ["bash", "-c"]
