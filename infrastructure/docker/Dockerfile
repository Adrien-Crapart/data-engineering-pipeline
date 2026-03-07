FROM apache/airflow:3.1.7-python3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install Python dependencies as root (site-packages owned by root)
COPY requirements.txt /tmp/requirements.txt
RUN uv pip install --system --no-cache -r /tmp/requirements.txt

USER airflow
