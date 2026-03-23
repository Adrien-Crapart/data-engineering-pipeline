FROM python:3.12-slim-bookworm AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_DEV=1

WORKDIR /app

COPY ingestion/pyproject.toml pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project 2>/dev/null || \
    uv pip install --system --no-cache \
        "dlt[filesystem,parquet]>=1.0.0" \
        "s3fs>=2024.0.0" \
        "polars>=1.0.0" \
        "pyarrow>=17.0.0" \
        "minio>=7.2.0" \
        "pyyaml>=6.0" \
        "requests>=2.32.0" \
        "python-dotenv>=1.0.0" \
        "prometheus_client>=0.20.0"

FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

WORKDIR /app

ENTRYPOINT ["bash", "-c"]
