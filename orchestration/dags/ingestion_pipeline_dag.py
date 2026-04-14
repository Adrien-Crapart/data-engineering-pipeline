"""
Ingestion Pipeline DAG — Extract weather data, store raw Parquet on S3 (bronze),
then transform to staging in PostgreSQL via dbt-duckdb (silver).

Flow:
  1. extract_weather      — Call OpenWeather API, store Parquet on MinIO S3 via DLT
  2. dbt_staging (Cosmos) — Per-model dbt run for staging + quarantine (DuckDB → PG ATTACH)
  3. finalize_ingestion   — Emit staging_weather Asset → triggers quality_gate DAG

Medallion layers:
  - Bronze: S3 raw Parquet (immutable, versioned)
  - Silver: PG staging schema (validated, persisted)
  - Quarantine: PG staging_quarantine schema (rejected rows for triage)

Uses Airflow Assets for event-driven scheduling.
Uses Cosmos DbtTaskGroup with Docker execution for per-model orchestration.
Schedule: Every 6 hours (Europe/Paris timezone).
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta
from pathlib import Path

from airflow.exceptions import AirflowSkipException
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Asset, Param, dag, task
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior
from docker.types import Mount
from plugins.callbacks.handlers import (
    on_failure_callback,
    on_retry_callback,
    on_success_callback,
)
from plugins.constants import (
    ASSET_STAGING_WEATHER,
    DAG_START_DATE,
    DEFAULT_DBT_IMAGE,
    DEFAULT_DBT_MEM,
    DEFAULT_DLT_IMAGE,
    DEFAULT_DLT_MEM,
    DEFAULT_NETWORK,
    POOL_API,
    POOL_DATABASE,
    normalize_host_path,
)

logger = logging.getLogger(__name__)

staging_weather = Asset(name="staging_weather", uri=ASSET_STAGING_WEATHER)

HOST_PROJECT_ROOT = os.environ.get("PROJECT_ROOT", ".")

DOCKER_DEFAULTS = {
    "auto_remove": "success",
    "docker_url": "unix://var/run/docker.sock",
    "mount_tmp_dir": False,
}

DBT_ENV = {
    "PATH": "/opt/venv/bin:/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin",
    "HOME": "/tmp",
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "datawarehouse",
    "POSTGRES_USER": "datawarehouse_user",
    "POSTGRES_PASSWORD": "datawarehouse_password",
    "MINIO_ROOT_USER": "minioadmin",
    "MINIO_ROOT_PASSWORD": "minioadmin",
    "MINIO_ENDPOINT": "http://minio:9000",
    "MINIO_BUCKET_NAME": "data-lake",
    "PYTHONUNBUFFERED": "1",
}

DBT_MOUNTS = [
    Mount(
        source=f"{normalize_host_path(HOST_PROJECT_ROOT)}/transformations",
        target="/app",
        type="bind",
    )
]

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
    description="Extract weather API -> Parquet S3 (bronze) -> Cosmos dbt staging PG (silver) -> emit Asset",
    schedule="0 */6 * * *",
    start_date=DAG_START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_success_callback=on_success_callback,
    tags=["weather", "ingestion", "dlt", "dbt", "cosmos", "staging", "s3", "medallion"],
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
        "send_notifications": Param(
            default=True,
            type="boolean",
            description="Send Slack/Email alerts on failure/success",
        ),
        "trigger_quality_gate": Param(
            default=True,
            type="boolean",
            description="Emit Asset to trigger the quality gate DAG",
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
            "MINIO_BUCKET_NAME": "{{ var.value.get('minio_bucket_name', 'data-lake') }}",
            "PYTHONUNBUFFERED": "1",
        },
        mem_limit=DEFAULT_DLT_MEM,
        execution_timeout=timedelta(minutes=15),
        pool=POOL_API,
        **DOCKER_DEFAULTS,
    )

    dbt_staging = DbtTaskGroup(
        group_id="dbt_staging",
        project_config=ProjectConfig(
            dbt_project_path=Path("/opt/airflow/dbt"),
        ),
        profile_config=ProfileConfig(
            profile_name="weather_pipeline",
            target_name="dev",
            profiles_yml_filepath=Path("/opt/airflow/dbt/profiles.yml"),
        ),
        render_config=RenderConfig(
            load_method=LoadMode.CUSTOM,
            select=["path:models/staging", "path:models/staging_quarantine"],
            test_behavior=TestBehavior.NONE,
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.DOCKER,
        ),
        operator_args={
            "image": DEFAULT_DBT_IMAGE,
            "network_mode": DEFAULT_NETWORK,
            "docker_url": "unix://var/run/docker.sock",
            "mount_tmp_dir": False,
            "auto_remove": "success",
            "get_logs": True,
            "mounts": DBT_MOUNTS,
            "environment": DBT_ENV,
            "mem_limit": DEFAULT_DBT_MEM,
        },
        default_args={"pool": POOL_DATABASE},
    )

    @task(outlets=[staging_weather])
    def finalize_ingestion(**context) -> str:
        """Emit the staging_weather Asset to trigger the quality gate DAG."""
        trigger = context["params"].get("trigger_quality_gate", True)
        if not trigger:
            raise AirflowSkipException("Quality gate trigger disabled via DAG param")
        logger.info("Ingestion complete — emitting staging_weather Asset")
        return "asset_emitted"

    extract_weather >> dbt_staging >> finalize_ingestion()


ingestion_pipeline()
