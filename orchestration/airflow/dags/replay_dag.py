"""
Replay DAG - Reprocess historical raw data from MinIO data lake.

Triggered manually with parameters:
  - date_start: Start date (YYYY-MM-DD)
  - date_end: End date (YYYY-MM-DD)

Use cases: fix transformations, update contracts, correct historical errors.

Processing tasks (dbt) run in isolated Docker containers via DockerOperator.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag, task

logger = logging.getLogger(__name__)

PROCESSING_IMAGE = "weather-pipeline-processing:latest"
NETWORK_NAME = "weather-pipeline_pipeline-network"
DBT_MOUNT = "/app/dbt"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


@dag(
    dag_id="replay_pipeline",
    description="Replay historical raw data from MinIO data lake into the warehouse",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["replay", "backfill", "maintenance"],
    params={
        "date_start": Param(
            default="2025-01-01",
            type="string",
            description="Start date (YYYY-MM-DD)",
        ),
        "date_end": Param(
            default="2025-01-01",
            type="string",
            description="End date (YYYY-MM-DD)",
        ),
    },
    doc_md=__doc__,
)
def replay_pipeline():

    @task()
    def replay_from_datalake(**context) -> dict[str, Any]:
        """Read raw files from MinIO and reload into PostgreSQL."""
        from replay.reprocess_pipeline import (
            _build_minio_client,
            list_raw_files,
            load_raw_to_postgres,
        )

        params = context["params"]
        date_start = datetime.strptime(params["date_start"], "%Y-%m-%d")
        date_end = datetime.strptime(params["date_end"], "%Y-%m-%d")

        client = _build_minio_client()
        bucket = "weather-data-lake"

        keys = list_raw_files(client, bucket, date_start, date_end)
        logger.info("Replay: found %d raw files", len(keys))

        if not keys:
            return {"files_found": 0, "records_loaded": 0, "status": "no_data"}

        postgres_dsn = "postgresql://airflow:airflow@postgres:5432/weather_db"
        records = load_raw_to_postgres(client, bucket, keys, postgres_dsn)
        return {"files_found": len(keys), "records_loaded": records, "status": "success"}

    dbt_deps = DockerOperator(
        task_id="dbt_deps",
        image=PROCESSING_IMAGE,
        command="cd /app/dbt && dbt deps --profiles-dir .",
        network_mode=NETWORK_NAME,
        mounts=[{"source": "transformations/dbt", "target": DBT_MOUNT, "type": "bind"}],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        environment={
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "weather_db",
            "POSTGRES_USER": "airflow",
            "POSTGRES_PASSWORD": "airflow",
        },
    )

    dbt_run = DockerOperator(
        task_id="dbt_run",
        image=PROCESSING_IMAGE,
        command="cd /app/dbt && dbt run --profiles-dir .",
        network_mode=NETWORK_NAME,
        mounts=[{"source": "transformations/dbt", "target": DBT_MOUNT, "type": "bind"}],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        environment={
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "weather_db",
            "POSTGRES_USER": "airflow",
            "POSTGRES_PASSWORD": "airflow",
        },
    )

    dbt_test = DockerOperator(
        task_id="dbt_test",
        image=PROCESSING_IMAGE,
        command="cd /app/dbt && dbt test --profiles-dir .",
        network_mode=NETWORK_NAME,
        mounts=[{"source": "transformations/dbt", "target": DBT_MOUNT, "type": "bind"}],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        environment={
            "POSTGRES_HOST": "postgres",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "weather_db",
            "POSTGRES_USER": "airflow",
            "POSTGRES_PASSWORD": "airflow",
        },
    )

    @task()
    def log_replay_result(replay_metrics: dict[str, Any]) -> None:
        """Log the replay summary."""
        logger.info("=" * 60)
        logger.info("REPLAY COMPLETE")
        logger.info("Files: %d", replay_metrics.get("files_found", 0))
        logger.info("Records: %d", replay_metrics.get("records_loaded", 0))
        logger.info("Status: %s", replay_metrics.get("status", "unknown"))
        logger.info("=" * 60)

    replay_result = replay_from_datalake()
    replay_result >> dbt_deps >> dbt_run >> dbt_test >> log_replay_result(replay_result)


replay_pipeline()
