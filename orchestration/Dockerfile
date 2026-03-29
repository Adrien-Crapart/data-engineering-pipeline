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

COPY pyproject.toml /tmp/build/pyproject.toml
RUN cd /tmp/build \
    && python -c "import tomllib, pathlib; \
       deps = tomllib.loads(pathlib.Path('pyproject.toml').read_text())['project']['dependencies']; \
       pathlib.Path('requirements.txt').write_text('\n'.join(deps))" \
    && uv pip install --python /home/airflow/.local/bin/python -r requirements.txt

USER airflow
WORKDIR /opt/airflow
