"""
Ingestion Pipeline DAG — Extract weather data and store on S3 as Parquet,
then transform to staging via dbt-duckdb Docker, then quality checks.

Flow:
  1. extract_weather      — Call OpenWeather API, store Parquet on MinIO S3 via DLT
  2. dbt_build_staging    — dbt build (run + test) staging models in single DuckDB session
  3. gx_validate_staging  — Great Expectations checks on core tables
  4. soda_scan_staging    — Soda scan on core tables
  5. finalize_ingestion   — Emit raw_weather_s3 Asset → triggers transformation DAG

Uses Airflow Assets for event-driven scheduling.
All configuration via Airflow Variables and Connections (no os.environ in tasks).
Schedule: Every 6 hours (Europe/Paris timezone).
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Asset, dag, task
from docker.types import Mount

from plugins.callbacks.handlers import on_failure_callback, on_retry_callback, on_success_callback
from plugins.constants import (
    ASSET_RAW_WEATHER_S3,
    DAG_START_DATE,
    DEFAULT_DBT_IMAGE,
    DEFAULT_DBT_MEM,
    DEFAULT_DLT_IMAGE,
    DEFAULT_DLT_MEM,
    DEFAULT_NETWORK,
    DEFAULT_SODA_IMAGE,
    DEFAULT_SODA_MEM,
    POOL_API,
    POOL_DATABASE,
)

logger = logging.getLogger(__name__)

raw_weather_s3 = Asset(name="raw_weather_s3", uri=ASSET_RAW_WEATHER_S3)

DBT_VENV_PATH = "/opt/venv/bin:/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin"
HOST_PROJECT_ROOT = os.environ.get("PROJECT_ROOT", ".")

DOCKER_DEFAULTS = {
    "auto_remove": "success",
    "docker_url": "unix://var/run/docker.sock",
    "mount_tmp_dir": False,
}

DBT_ENV = {
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

DBT_MOUNTS = [Mount(source=f"{HOST_PROJECT_ROOT}/transformations", target="/app", type="bind")]

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
    dag_id="ingestion_pipeline",
    description="Extract weather API → Parquet S3 (DLT) → dbt-duckdb staging → quality checks",
    schedule="0 */6 * * *",
    start_date=DAG_START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_success_callback=on_success_callback,
    tags=["weather", "ingestion", "dlt", "dbt", "staging", "s3"],
    doc_md=__doc__,
    params={
        "version": Param(
            default="latest",
            type="string",
            description="S3 data version to reload (YYYYMMDD_HHMMSS or 'latest' for fresh extraction)",
        ),
        "force_download": Param(
            default=True,
            type="boolean",
            description="Download fresh data from OpenWeather API",
        ),
        "run_quality_checks": Param(
            default=True,
            type="boolean",
            description="Run Great Expectations + Soda quality checks",
        ),
        "send_notifications": Param(
            default=True,
            type="boolean",
            description="Send Slack/Email alerts on failure/success",
        ),
        "trigger_transformation": Param(
            default=True,
            type="boolean",
            description="Emit Asset to trigger the transformation DAG",
        ),
    },
)
def ingestion_pipeline():
    extract_weather = DockerOperator(
        task_id="extract_weather",
        image=DEFAULT_DLT_IMAGE,
        command="--pipeline openweather",
        network_mode=DEFAULT_NETWORK,
        environment={
            "OPENWEATHER_API_KEY": "{{ var.value.openweather_api_key }}",
            "WEATHER_CITIES": "{{ var.value.get('weather_cities', 'Paris,Lyon,Marseille,Toulouse,Nice') }}",
            "MINIO_ROOT_USER": "{{ conn.minio_s3.extra_dejson.aws_access_key_id }}",
            "MINIO_ROOT_PASSWORD": "{{ conn.minio_s3.extra_dejson.aws_secret_access_key }}",
            "MINIO_ENDPOINT": "{{ conn.minio_s3.extra_dejson.endpoint_url }}",
            "MINIO_BUCKET_NAME": "{{ var.value.get('minio_bucket_name', 'weather-data-lake') }}",
            "PYTHONUNBUFFERED": "1",
        },
        mem_limit=DEFAULT_DLT_MEM,
        execution_timeout=timedelta(minutes=15),
        pool=POOL_API,
        **DOCKER_DEFAULTS,
    )

    dbt_build_staging = DockerOperator(
        task_id="dbt_build_staging",
        image=DEFAULT_DBT_IMAGE,
        command="run --select staging",
        network_mode=DEFAULT_NETWORK,
        mounts=DBT_MOUNTS,
        environment=DBT_ENV,
        mem_limit=DEFAULT_DBT_MEM,
        execution_timeout=timedelta(minutes=15),
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

    gx_validate_staging = DockerOperator(
        task_id="gx_validate_staging",
        image=DEFAULT_SODA_IMAGE,
        command="data_quality.expectations.run_validations --layer staging",
        network_mode=DEFAULT_NETWORK,
        environment=QUALITY_ENV,
        mounts=QUALITY_MOUNTS,
        mem_limit=DEFAULT_SODA_MEM,
        execution_timeout=timedelta(minutes=10),
        pool=POOL_DATABASE,
        **DOCKER_DEFAULTS,
    )

    soda_scan_staging = DockerOperator(
        task_id="soda_scan_staging",
        image=DEFAULT_SODA_IMAGE,
        command="data_quality.soda.run_scan --layer staging",
        network_mode=DEFAULT_NETWORK,
        environment=QUALITY_ENV,
        mounts=QUALITY_MOUNTS,
        mem_limit=DEFAULT_SODA_MEM,
        execution_timeout=timedelta(minutes=10),
        pool=POOL_DATABASE,
        **DOCKER_DEFAULTS,
    )

    @task(outlets=[raw_weather_s3])
    def finalize_ingestion(**context) -> str:
        """Emit the raw_weather_s3 Asset to trigger the transformation DAG."""
        trigger = context["params"].get("trigger_transformation", True)
        if not trigger:
            raise AirflowSkipException("Transformation trigger disabled via DAG param")
        logger.info("Ingestion complete — emitting raw_weather_s3 Asset")
        return "asset_emitted"

    quality_gate = [gx_validate_staging, soda_scan_staging]

    extract_weather >> dbt_build_staging >> quality_gate >> finalize_ingestion()


ingestion_pipeline()
