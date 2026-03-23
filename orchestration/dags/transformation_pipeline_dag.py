"""
Transformation Pipeline DAG — Transform staging data to core/mart/analytic,
validate quality, generate docs, and push observability reports.

Flow:
  1. dbt_run_core_mart    — Run dbt core + mart + analytic models (staging → gold)
  2. dbt_test_core_mart   — Run dbt tests on core/mart/analytic
  3. gx_validate_mart     — Great Expectations checks on mart tables
  4. soda_scan_mart       — Soda scan on mart tables
  5. dbt_generate_docs    — Generate dbt docs and upload to S3
  6. elementary_report    — Generate Elementary observability report
  7. push_metrics         — Push pipeline metrics to Prometheus

Uses astronomer-cosmos Docker operators for dbt execution in isolated containers.

Schedule: Every 6 hours, offset by 2 hours from ingestion (data-aware via Asset).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag, task
from cosmos.operators.docker import (
    DbtDepsDockerOperator,
    DbtRunDockerOperator,
    DbtTestDockerOperator,
)
from docker.types import Mount

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.environ.get("PROJECT_ROOT", ".")
NETWORK_NAME = (
    os.environ.get("COMPOSE_PROJECT_NAME", "data-engineering-pipeline")
    + "_pipeline-network"
)

DBT_IMAGE = os.environ.get("DBT_IMAGE", "weather-pipeline-dbt:1.0.0")
SODA_IMAGE = os.environ.get("SODA_IMAGE", "weather-pipeline-soda:1.0.0")

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

DBT_PROJECT_DIR = "/app"


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
    dag_id="transformation_pipeline",
    description="dbt core/mart/analytic → quality gates → docs → observability",
    schedule="0 2-23/6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["weather", "transformation", "dbt", "cosmos", "soda", "great-expectations"],
    doc_md=__doc__,
    sla_miss_callback=_sla_miss_callback,
)
def transformation_pipeline():

    # === Cosmos dbt operators (Docker execution mode) ===
    # Cosmos Docker operators run dbt inside the dbt-runner container image.
    # They connect to PG using environment variables, not Airflow connections.

    cosmos_operator_args = {
        "image": DBT_IMAGE,
        "network_mode": NETWORK_NAME,
        "auto_remove": "success",
        "docker_url": "unix://var/run/docker.sock",
        "mount_tmp_dir": False,
        "mem_limit": DBT_MEM_LIMIT,
        "mounts": [
            Mount(source=f"{PROJECT_ROOT}/transformations", target="/app", type="bind"),
        ],
        "environment": {**PG_ENV, **MINIO_ENV},
    }

    # --- dbt deps via Cosmos ---
    dbt_install_deps = DbtDepsDockerOperator(
        task_id="dbt_install_deps",
        project_dir=DBT_PROJECT_DIR,
        schema="public",
        conn_id="postgres_default",
        **cosmos_operator_args,
    )

    # --- dbt run: core + mart + analytic via Cosmos ---
    dbt_run_core = DbtRunDockerOperator(
        task_id="dbt_run_core_mart",
        project_dir=DBT_PROJECT_DIR,
        schema="public",
        conn_id="postgres_default",
        select="core mart analytic",
        **cosmos_operator_args,
    )

    # --- dbt test: core + mart + analytic via Cosmos ---
    dbt_test_core = DbtTestDockerOperator(
        task_id="dbt_test_core_mart",
        project_dir=DBT_PROJECT_DIR,
        schema="public",
        conn_id="postgres_default",
        select="core mart analytic",
        **cosmos_operator_args,
    )

    # --- GX validate mart ---
    gx_validate_mart = DockerOperator(
        task_id="gx_validate_mart",
        image=SODA_IMAGE,
        command="cd /app && python -m data_quality.expectations.run_validations --layer mart",
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/data_quality", target="/app/data_quality", type="bind"),
        ],
        environment={**PG_ENV, **MINIO_ENV, "PYTHONPATH": "/app"},
        mem_limit=SODA_MEM_LIMIT,
        execution_timeout=timedelta(minutes=10),
        **DOCKER_DEFAULTS,
    )

    # --- Soda scan mart ---
    soda_scan_mart = DockerOperator(
        task_id="soda_scan_mart",
        image=SODA_IMAGE,
        command=(
            "soda scan "
            "-d weather_db "
            "-c /app/data_quality/soda/configuration_mart.yml "
            "/app/data_quality/soda/checks/mart_checks.yml"
        ),
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/data_quality", target="/app/data_quality", type="bind"),
        ],
        environment=PG_ENV,
        mem_limit=SODA_MEM_LIMIT,
        execution_timeout=timedelta(minutes=10),
        **DOCKER_DEFAULTS,
    )

    # --- dbt docs generate + upload to S3 ---
    dbt_generate_docs = DockerOperator(
        task_id="dbt_generate_docs",
        image=DBT_IMAGE,
        command="docs",
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/transformations", target="/app", type="bind"),
        ],
        environment={**PG_ENV, **MINIO_ENV},
        mem_limit=DBT_MEM_LIMIT,
        execution_timeout=timedelta(minutes=10),
        **DOCKER_DEFAULTS,
    )

    # --- Elementary observability report ---
    elementary_report = DockerOperator(
        task_id="elementary_report",
        image=DBT_IMAGE,
        command=(
            "bash -c 'cd /app && edr report "
            "--profiles-dir . "
            "--file-path /tmp/elementary_report.html'"
        ),
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/transformations", target="/app", type="bind"),
        ],
        environment={**PG_ENV, **MINIO_ENV},
        mem_limit=DBT_MEM_LIMIT,
        execution_timeout=timedelta(minutes=15),
        **DOCKER_DEFAULTS,
    )

    # --- Push metrics to Prometheus ---
    @task()
    def push_metrics() -> None:
        """Push pipeline metrics to Prometheus Pushgateway."""
        from monitoring.metrics.exporter import (  # noqa: I001
            push_metrics as _push,
            record_pipeline_run,
        )

        record_pipeline_run("transformation_pipeline", duration=0, success=True)
        try:
            _push()
        except Exception:
            logger.warning("Prometheus push failed", exc_info=True)

    # --- Dependencies ---
    dbt_install_deps >> dbt_run_core >> dbt_test_core

    # Quality gates in parallel after dbt tests
    dbt_test_core >> gx_validate_mart
    dbt_test_core >> soda_scan_mart

    # Docs + observability after quality
    gx_validate_mart >> dbt_generate_docs
    gx_validate_mart >> elementary_report
    soda_scan_mart >> dbt_generate_docs
    soda_scan_mart >> elementary_report

    # Metrics after everything
    metrics = push_metrics()
    dbt_generate_docs >> metrics
    elementary_report >> metrics


transformation_pipeline()
