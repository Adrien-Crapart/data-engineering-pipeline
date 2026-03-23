"""
Ingestion Pipeline DAG — Extract weather data, store on S3 as Parquet,
load to PostgreSQL raw schema, then transform to staging via dbt.

Flow:
  1. extract_weather      — Call OpenWeather API, store Parquet on MinIO S3, load to PG raw
  2. validate_contracts    — Check raw data against YAML data contracts
  3. dbt_deps             — Install dbt packages
  4. dbt_run_staging      — Run dbt staging models (raw → staging views)
  5. dbt_test_staging     — Run dbt tests on staging models
  6. gx_validate_staging  — Great Expectations checks on staging tables
  7. soda_scan_staging    — Soda scan on staging tables

Schedule: Every 6 hours.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag
from docker.types import Mount

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (parse-time safe: os.environ only)
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.environ.get("PROJECT_ROOT", ".")
NETWORK_NAME = (
    os.environ.get("COMPOSE_PROJECT_NAME", "data-engineering-pipeline")
    + "_pipeline-network"
)

DLT_IMAGE = os.environ.get("DLT_IMAGE", "weather-pipeline-dlt:1.0.0")
DBT_IMAGE = os.environ.get("DBT_IMAGE", "weather-pipeline-dbt:1.0.0")
SODA_IMAGE = os.environ.get("SODA_IMAGE", "weather-pipeline-soda:1.0.0")

DLT_MEM_LIMIT = os.environ.get("DLT_MEM_LIMIT", "512m")
DBT_MEM_LIMIT = os.environ.get("DBT_MEM_LIMIT", "512m")
SODA_MEM_LIMIT = os.environ.get("SODA_MEM_LIMIT", "256m")

MINIO_ENV = {
    "MINIO_ROOT_USER": os.environ.get("MINIO_ROOT_USER", "minioadmin"),
    "MINIO_ROOT_PASSWORD": os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
    "MINIO_ENDPOINT": os.environ.get("MINIO_ENDPOINT", "minio:9000"),
    "MINIO_BUCKET_NAME": os.environ.get("MINIO_BUCKET_NAME", "weather-data-lake"),
}

PG_ENV = {
    "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "postgres"),
    "POSTGRES_PORT": os.environ.get("POSTGRES_PORT", "5432"),
    "POSTGRES_DB": os.environ.get("POSTGRES_DB", "weather_db"),
    "POSTGRES_USER": os.environ.get("POSTGRES_USER", "airflow"),
    "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "airflow"),
}

DOCKER_DEFAULTS = {
    "network_mode": NETWORK_NAME,
    "auto_remove": "success",
    "docker_url": "unix://var/run/docker.sock",
    "mount_tmp_dir": False,
}


def _sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    logger.error(
        "SLA MISS on %s | tasks: %s | blocking: %s",
        dag.dag_id,
        [t.task_id for t in task_list],
        [t.task_id for t in blocking_tis],
    )


def _on_failure_callback(context):
    ti = context.get("task_instance")
    logger.error(
        "TASK FAILED: %s | execution_date=%s",
        getattr(ti, "task_id", "unknown"),
        context.get("execution_date"),
    )


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": _on_failure_callback,
}


@dag(
    dag_id="ingestion_pipeline",
    description="Extract weather API → Parquet S3 → PG raw → dbt staging → quality checks",
    schedule="0 */6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["weather", "ingestion", "dlt", "dbt", "staging"],
    doc_md=__doc__,
    sla_miss_callback=_sla_miss_callback,
)
def ingestion_pipeline():

    # --- Extract: API → Parquet on S3 + raw schema in PostgreSQL ---
    extract_weather = DockerOperator(
        task_id="extract_weather",
        image=DLT_IMAGE,
        command="cd /app && python -m ingestion.pipelines.openweather_pipeline",
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/ingestion", target="/app/ingestion", type="bind"),
            Mount(source=f"{PROJECT_ROOT}/contracts", target="/app/contracts", type="bind"),
        ],
        environment={
            **PG_ENV,
            **MINIO_ENV,
            "OPENWEATHER_API_KEY": os.environ.get("OPENWEATHER_API_KEY", ""),
            "WEATHER_CITIES": os.environ.get("WEATHER_CITIES", "Paris,Lyon,Marseille"),
            "PYTHONPATH": "/app",
        },
        mem_limit=DLT_MEM_LIMIT,
        execution_timeout=timedelta(minutes=15),
        sla=timedelta(minutes=20),
        **DOCKER_DEFAULTS,
    )

    # --- dbt deps ---
    dbt_install_deps = DockerOperator(
        task_id="dbt_install_deps",
        image=DBT_IMAGE,
        command="deps",
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/transformations", target="/app", type="bind"),
        ],
        environment={**PG_ENV, **MINIO_ENV},
        mem_limit=DBT_MEM_LIMIT,
        execution_timeout=timedelta(minutes=10),
        **DOCKER_DEFAULTS,
    )

    # --- dbt run staging models (raw → staging) ---
    dbt_run_staging = DockerOperator(
        task_id="dbt_run_staging",
        image=DBT_IMAGE,
        command="run --select staging",
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/transformations", target="/app", type="bind"),
        ],
        environment={**PG_ENV, **MINIO_ENV},
        mem_limit=DBT_MEM_LIMIT,
        execution_timeout=timedelta(minutes=15),
        sla=timedelta(minutes=25),
        **DOCKER_DEFAULTS,
    )

    # --- dbt test staging ---
    dbt_test_staging = DockerOperator(
        task_id="dbt_test_staging",
        image=DBT_IMAGE,
        command="test --select staging",
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/transformations", target="/app", type="bind"),
        ],
        environment={**PG_ENV, **MINIO_ENV},
        mem_limit=DBT_MEM_LIMIT,
        execution_timeout=timedelta(minutes=10),
        **DOCKER_DEFAULTS,
    )

    # --- Great Expectations on staging ---
    gx_validate_staging = DockerOperator(
        task_id="gx_validate_staging",
        image=SODA_IMAGE,
        command="cd /app && python -m data_quality.expectations.run_validations --layer staging",
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/data_quality", target="/app/data_quality", type="bind"),
        ],
        environment={**PG_ENV, **MINIO_ENV, "PYTHONPATH": "/app"},
        mem_limit=SODA_MEM_LIMIT,
        execution_timeout=timedelta(minutes=10),
        **DOCKER_DEFAULTS,
    )

    # --- Soda scan staging ---
    soda_scan_staging = DockerOperator(
        task_id="soda_scan_staging",
        image=SODA_IMAGE,
        command=(
            "soda scan "
            "-d weather_db "
            "-c /app/data_quality/soda/configuration.yml "
            "/app/data_quality/soda/checks/staging_checks.yml"
        ),
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/data_quality", target="/app/data_quality", type="bind"),
        ],
        environment=PG_ENV,
        mem_limit=SODA_MEM_LIMIT,
        execution_timeout=timedelta(minutes=10),
        **DOCKER_DEFAULTS,
    )

    # --- Dependencies ---
    extract_weather >> dbt_install_deps >> dbt_run_staging >> dbt_test_staging
    dbt_test_staging >> gx_validate_staging
    dbt_test_staging >> soda_scan_staging


ingestion_pipeline()
