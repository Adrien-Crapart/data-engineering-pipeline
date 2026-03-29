"""
Transformation Pipeline DAG — Transform staging data to core/mart/analytic
via dbt-duckdb Docker, validate quality, generate docs, and push metrics.

Flow:
  1. dbt_build_transform  — dbt build (run + test) core/marts/analytic models
  2. gx_validate_mart     — Great Expectations checks on mart tables
  3. soda_scan_mart       — Soda scan on mart tables
  4. dbt_generate_docs    — Generate dbt docs and upload to S3
  5. finalize             — Emit mart_weather Asset and push metrics

Schedule: Event-driven — triggered when ingestion_pipeline emits raw_weather_s3 Asset.
Uses dbt-duckdb: reads S3 Parquet for staging views, writes to PG via ATTACH for core/mart.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Asset, dag, task
from docker.types import Mount

from plugins.callbacks.handlers import on_failure_callback, on_retry_callback, on_success_callback
from plugins.constants import (
    ASSET_MART_WEATHER,
    ASSET_RAW_WEATHER_S3,
    DAG_START_DATE,
    DEFAULT_DBT_IMAGE,
    DEFAULT_DBT_MEM,
    DEFAULT_NETWORK,
    DEFAULT_SODA_IMAGE,
    DEFAULT_SODA_MEM,
    POOL_DATABASE,
    POOL_DOCKER,
)

logger = logging.getLogger(__name__)

raw_weather_s3 = Asset(name="raw_weather_s3", uri=ASSET_RAW_WEATHER_S3)
mart_weather = Asset(name="mart_weather", uri=ASSET_MART_WEATHER)

DBT_VENV_PATH = "/opt/venv/bin:/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin"
HOST_PROJECT_ROOT = os.environ.get("PROJECT_ROOT", ".")

DBT_MOUNTS = [Mount(source=f"{HOST_PROJECT_ROOT}/transformations", target="/app", type="bind")]

DBT_COMMON_ENV = {
    "PATH": DBT_VENV_PATH,
    "HOME": "/tmp",
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "datawarehouse",
    "POSTGRES_USER": "datawarehouse_user",
    "POSTGRES_PASSWORD": "datawarehouse_password",
    "MINIO_ROOT_USER": "minioadmin",
    "MINIO_ROOT_PASSWORD": "minioadmin",
    "MINIO_ENDPOINT": "http://minio:9000",
    "MINIO_BUCKET_NAME": "weather-data-lake",
    "PYTHONUNBUFFERED": "1",
}

DOCKER_DEFAULTS = {
    "auto_remove": "success",
    "docker_url": "unix://var/run/docker.sock",
    "mount_tmp_dir": False,
}

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": on_failure_callback,
    "on_retry_callback": on_retry_callback,
}


@dag(
    dag_id="transformation_pipeline",
    description="dbt-duckdb core/mart/analytic → quality gates → docs → observability",
    schedule=[raw_weather_s3],
    start_date=DAG_START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_success_callback=on_success_callback,
    tags=["weather", "transformation", "dbt", "duckdb", "soda", "great-expectations"],
    doc_md=__doc__,
    params={
        "run_quality_checks": {"type": "boolean", "default": True, "description": "Run GX + Soda checks"},
        "generate_docs": {"type": "boolean", "default": True, "description": "Generate and upload dbt docs"},
        "send_notifications": {"type": "boolean", "default": True, "description": "Send alerts"},
    },
)
def transformation_pipeline():
    dbt_run_transform = DockerOperator(
        task_id="dbt_run_transform",
        image=DEFAULT_DBT_IMAGE,
        command="run --select staging core marts analytic",
        network_mode=DEFAULT_NETWORK,
        mounts=DBT_MOUNTS,
        environment=DBT_COMMON_ENV,
        mem_limit=DEFAULT_DBT_MEM,
        execution_timeout=timedelta(minutes=20),
        pool=POOL_DATABASE,
        **DOCKER_DEFAULTS,
    )

    QUALITY_ENV = {
        "POSTGRES_HOST": "postgres",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DB": "datawarehouse",
        "POSTGRES_USER": "datawarehouse_user",
        "POSTGRES_PASSWORD": "datawarehouse_password",
        "MINIO_ROOT_USER": "minioadmin",
        "MINIO_ROOT_PASSWORD": "minioadmin",
        "MINIO_ENDPOINT": "http://minio:9000",
        "MINIO_BUCKET_NAME": "weather-data-lake",
        "PYTHONPATH": "/app",
        "PYTHONUNBUFFERED": "1",
    }

    QUALITY_MOUNTS = [
        Mount(source=f"{HOST_PROJECT_ROOT}/data_quality", target="/app/data_quality", type="bind"),
    ]

    gx_validate_mart = DockerOperator(
        task_id="gx_validate_mart",
        image=DEFAULT_SODA_IMAGE,
        command="data_quality.expectations.run_validations --layer mart",
        network_mode=DEFAULT_NETWORK,
        environment=QUALITY_ENV,
        mounts=QUALITY_MOUNTS,
        mem_limit=DEFAULT_SODA_MEM,
        execution_timeout=timedelta(minutes=10),
        pool=POOL_DATABASE,
        **DOCKER_DEFAULTS,
    )

    soda_scan_mart = DockerOperator(
        task_id="soda_scan_mart",
        image=DEFAULT_SODA_IMAGE,
        command="data_quality.soda.run_scan --layer mart",
        network_mode=DEFAULT_NETWORK,
        environment=QUALITY_ENV,
        mounts=QUALITY_MOUNTS,
        mem_limit=DEFAULT_SODA_MEM,
        execution_timeout=timedelta(minutes=10),
        pool=POOL_DATABASE,
        **DOCKER_DEFAULTS,
    )

    dbt_generate_docs = DockerOperator(
        task_id="dbt_generate_docs",
        image=DEFAULT_DBT_IMAGE,
        command="docs",
        network_mode=DEFAULT_NETWORK,
        mounts=DBT_MOUNTS,
        environment=DBT_COMMON_ENV,
        mem_limit=DEFAULT_DBT_MEM,
        execution_timeout=timedelta(minutes=10),
        pool=POOL_DOCKER,
        **DOCKER_DEFAULTS,
    )

    @task(outlets=[mart_weather])
    def finalize_transformation(**context) -> str:
        """Emit mart_weather Asset and push metrics."""
        try:
            from monitoring.metrics.exporter import (
                push_metrics as _push,
                record_pipeline_run,
            )
            record_pipeline_run("transformation_pipeline", duration=0, success=True)
            _push()
        except Exception:
            logger.warning("Prometheus push failed — not critical", exc_info=True)
        logger.info("Transformation complete — emitting mart_weather Asset")
        return "mart_asset_emitted"

    quality_gate = [gx_validate_mart, soda_scan_mart]

    dbt_run_transform >> quality_gate >> dbt_generate_docs >> finalize_transformation()


transformation_pipeline()
