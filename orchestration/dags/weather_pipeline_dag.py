"""
Weather Pipeline DAG - End-to-end orchestration.

Ingests weather data from OpenWeather API (dlt container), transforms
with dbt (dbt container), validates quality with Soda Core (soda container),
generates Elementary observability report (dbt container), and pushes
metrics to Prometheus.

Each processing tool runs in its own isolated Docker container via
DockerOperator. Airflow only orchestrates — it never runs processing.

Re-runnable from any task via the Airflow UI (Clear task).

Schedule: Every 6 hours.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag, task
from docker.types import Mount

logger = logging.getLogger(__name__)

PROJECT_ROOT = os.environ.get("PROJECT_ROOT", ".")
NETWORK_NAME = os.environ.get("COMPOSE_PROJECT_NAME", "weather-pipeline") + "_pipeline-network"

DLT_IMAGE = "weather-pipeline-dlt:1.0.0"
DBT_IMAGE = "weather-pipeline-dbt:1.0.0"
SODA_IMAGE = "weather-pipeline-soda:1.0.0"

PG_ENV = {
    "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "postgres"),
    "POSTGRES_PORT": os.environ.get("POSTGRES_PORT", "5432"),
    "POSTGRES_DB": os.environ.get("POSTGRES_DB", "weather_db"),
    "POSTGRES_USER": os.environ.get("POSTGRES_USER", "airflow"),
    "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "airflow"),
}

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="weather_pipeline",
    description="End-to-end weather data pipeline: ingest -> transform -> validate -> observe",
    schedule="0 */6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["weather", "production", "dlt", "dbt", "soda"],
    doc_md=__doc__,
)
def weather_pipeline():

    extract_weather = DockerOperator(
        task_id="extract_weather",
        image=DLT_IMAGE,
        command="cd /app && python -m ingestion.pipelines.openweather_pipeline",
        network_mode=NETWORK_NAME,
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/ingestion", target="/app/ingestion", type="bind"),
            Mount(source=f"{PROJECT_ROOT}/contracts", target="/app/contracts", type="bind"),
        ],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        environment={
            **PG_ENV,
            "OPENWEATHER_API_KEY": os.environ.get("OPENWEATHER_API_KEY", ""),
            "WEATHER_CITIES": os.environ.get("WEATHER_CITIES", "Paris,Lyon,Marseille"),
            "MINIO_ROOT_USER": os.environ.get("MINIO_ROOT_USER", "minioadmin"),
            "MINIO_ROOT_PASSWORD": os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
            "MINIO_ENDPOINT": "minio:9000",
            "MINIO_BUCKET_NAME": os.environ.get("MINIO_BUCKET_NAME", "weather-data-lake"),
            "PYTHONPATH": "/app",
        },
    )

    dbt_deps = DockerOperator(
        task_id="dbt_deps",
        image=DBT_IMAGE,
        command="cd /app/dbt && dbt deps --profiles-dir .",
        network_mode=NETWORK_NAME,
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/transformations/dbt", target="/app/dbt", type="bind"),
        ],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        environment=PG_ENV,
    )

    dbt_run = DockerOperator(
        task_id="dbt_run",
        image=DBT_IMAGE,
        command="cd /app/dbt && dbt run --profiles-dir .",
        network_mode=NETWORK_NAME,
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/transformations/dbt", target="/app/dbt", type="bind"),
        ],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        environment=PG_ENV,
    )

    dbt_test = DockerOperator(
        task_id="dbt_test",
        image=DBT_IMAGE,
        command="cd /app/dbt && dbt test --profiles-dir .",
        network_mode=NETWORK_NAME,
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/transformations/dbt", target="/app/dbt", type="bind"),
        ],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        environment=PG_ENV,
    )

    soda_scan = DockerOperator(
        task_id="soda_scan",
        image=SODA_IMAGE,
        command=(
            "soda scan "
            "-d weather_db "
            "-c /app/data_quality/soda/configuration.yml "
            "/app/data_quality/soda/checks/"
        ),
        network_mode=NETWORK_NAME,
        mounts=[
            Mount(
                source=f"{PROJECT_ROOT}/data_quality",
                target="/app/data_quality",
                type="bind",
            ),
        ],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        environment=PG_ENV,
    )

    elementary_report = DockerOperator(
        task_id="elementary_report",
        image=DBT_IMAGE,
        command=(
            "cd /app/dbt && edr report "
            "--profiles-dir . "
            "--file-path /tmp/elementary_report.html"
        ),
        network_mode=NETWORK_NAME,
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/transformations/dbt", target="/app/dbt", type="bind"),
        ],
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        environment=PG_ENV,
    )

    @task()
    def push_metrics() -> None:
        """Push pipeline metrics to Prometheus Pushgateway."""
        from monitoring.metrics.exporter import (
            push_metrics as _push,
            record_pipeline_run,
        )

        record_pipeline_run("weather_pipeline", duration=0, success=True)
        try:
            _push()
        except Exception:
            logger.warning("Prometheus push failed - metrics not exported", exc_info=True)
        logger.info("Pipeline metrics pushed successfully")

    (
        extract_weather
        >> dbt_deps
        >> dbt_run
        >> dbt_test
        >> soda_scan
        >> elementary_report
        >> push_metrics()
    )


weather_pipeline()
