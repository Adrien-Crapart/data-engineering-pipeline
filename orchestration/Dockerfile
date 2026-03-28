FROM apache/airflow:3.1.8-python3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
ENV UV_LINK_MODE=copy

COPY pyproject.toml /tmp/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    cd /tmp && uv pip install --system -r pyproject.toml

USER airflow
