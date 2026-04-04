"""
Quality Gate Pipeline DAG — Validate staging tables before promotion to core/mart.

Flow:
  1. gx_validate_staging  — Great Expectations checks on PG staging tables
  2. soda_scan_staging    — Soda scan on PG staging tables
  3. finalize_quality     — Emit staging_validated Asset -> triggers transformation DAG

This DAG is the quality gate between silver (staging) and gold (core/mart).
It runs GX and Soda in parallel to validate business rules, freshness,
anomaly detection, and referential integrity on the persisted staging tables.

Schedule: Event-driven — triggered when ingestion_pipeline emits staging_weather Asset.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Asset, dag, task
from docker.types import Mount
from plugins.callbacks.handlers import (on_failure_callback, on_retry_callback,
                                        on_success_callback)
from plugins.constants import (ASSET_STAGING_VALIDATED, ASSET_STAGING_WEATHER,
                               DAG_START_DATE, DEFAULT_NETWORK,
                               DEFAULT_SODA_IMAGE, DEFAULT_SODA_MEM,
                               POOL_DATABASE)

logger = logging.getLogger(__name__)

staging_weather = Asset(name="staging_weather", uri=ASSET_STAGING_WEATHER)
staging_validated = Asset(name="staging_validated", uri=ASSET_STAGING_VALIDATED)

HOST_PROJECT_ROOT = os.environ.get("PROJECT_ROOT", ".")

DOCKER_DEFAULTS = {
    "auto_remove": "success",
    "docker_url": "unix://var/run/docker.sock",
    "mount_tmp_dir": False,
}

QUALITY_ENV = {
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "datawarehouse",
    "POSTGRES_USER": "datawarehouse_user",
    "POSTGRES_PASSWORD": "datawarehouse_password",
    "MINIO_ROOT_USER": "minioadmin",
    "MINIO_ROOT_PASSWORD": "minioadmin",
    "MINIO_ENDPOINT": "http://minio:9000",
    "MINIO_BUCKET_NAME": "data-lake",
    "PYTHONPATH": "/app",
    "PYTHONUNBUFFERED": "1",
}

QUALITY_MOUNTS = [
    Mount(source=f"{HOST_PROJECT_ROOT}/data_quality", target="/app/data_quality", type="bind"),
]

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": on_failure_callback,
    "on_retry_callback": on_retry_callback,
}


@dag(
    dag_id="quality_gate_pipeline",
    description="GX + Soda quality gate on staging tables -> emit staging_validated Asset",
    schedule=[staging_weather],
    start_date=DAG_START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_success_callback=on_success_callback,
    tags=["weather", "quality", "great-expectations", "soda", "staging", "medallion"],
    doc_md=__doc__,
    params={
        "send_notifications": {"type": "boolean", "default": True, "description": "Send alerts"},
    },
)
def quality_gate_pipeline():
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

    @task(outlets=[staging_validated])
    def finalize_quality(**context) -> str:
        """Emit staging_validated Asset to trigger the transformation DAG."""
        logger.info("Quality gate passed — emitting staging_validated Asset")
        return "staging_validated"

    quality_gate = [gx_validate_staging, soda_scan_staging]
    quality_gate >> finalize_quality()


quality_gate_pipeline()
