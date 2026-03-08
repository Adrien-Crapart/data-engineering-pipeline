"""
Weather Pipeline DAG - End-to-end orchestration.

Extracts weather data from OpenWeather API using dlt, transforms it
with dbt (in a dedicated Docker container), and validates data quality
with Soda Core and Elementary.

Processing tasks (dbt, Soda, Elementary) run in isolated Docker containers
via DockerOperator to separate orchestration from processing.

Schedule: Every 6 hours.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag, task

logger = logging.getLogger(__name__)

PROCESSING_IMAGE = "weather-pipeline-processing:latest"
NETWORK_NAME = "weather-pipeline_pipeline-network"
DBT_MOUNT = "/app/dbt"
SODA_MOUNT = "/app/data_quality"

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

    @task()
    def extract_weather() -> dict[str, Any]:
        """Run the dlt ingestion pipeline for all configured cities."""
        from ingestion.pipelines.openweather_pipeline import run_pipeline

        start = time.time()
        metrics = run_pipeline()
        metrics["task_duration_seconds"] = round(time.time() - start, 2)
        logger.info("Extraction complete: %s", metrics)
        return metrics

    dbt_deps = DockerOperator(
        task_id="dbt_deps",
        image=PROCESSING_IMAGE,
        command="cd /app/dbt && dbt deps --profiles-dir .",
        network_mode=NETWORK_NAME,
        mounts=[
            {
                "source": "transformations/dbt",
                "target": DBT_MOUNT,
                "type": "bind",
            }
        ],
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
        mounts=[
            {
                "source": "transformations/dbt",
                "target": DBT_MOUNT,
                "type": "bind",
            }
        ],
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
        mounts=[
            {
                "source": "transformations/dbt",
                "target": DBT_MOUNT,
                "type": "bind",
            }
        ],
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

    soda_scan = DockerOperator(
        task_id="soda_scan",
        image=PROCESSING_IMAGE,
        command=(
            "soda scan "
            "-d weather_db "
            "-c /app/data_quality/soda/configuration.yml "
            "/app/data_quality/soda/checks/"
        ),
        network_mode=NETWORK_NAME,
        mounts=[
            {
                "source": "data_quality",
                "target": SODA_MOUNT,
                "type": "bind",
            }
        ],
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

    elementary_report = DockerOperator(
        task_id="elementary_report",
        image=PROCESSING_IMAGE,
        command=(
            "cd /app/dbt && edr report "
            "--profiles-dir . "
            "--file-path /tmp/elementary_report.html"
        ),
        network_mode=NETWORK_NAME,
        mounts=[
            {
                "source": "transformations/dbt",
                "target": DBT_MOUNT,
                "type": "bind",
            }
        ],
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
    def push_metrics(extraction_metrics: dict[str, Any]) -> None:
        """Push pipeline metrics to Prometheus and log summary."""
        from monitoring.metrics.exporter import (
            push_metrics as _push,
            record_pipeline_run,
        )

        duration = extraction_metrics.get("task_duration_seconds", 0)
        success = extraction_metrics.get("status") == "success"
        record_pipeline_run("weather_pipeline", duration, success)

        try:
            _push()
        except Exception:
            logger.warning("Prometheus push failed - metrics not exported", exc_info=True)

        logger.info("=" * 60)
        logger.info("PIPELINE RUN COMPLETE")
        logger.info("Extraction: %s", extraction_metrics.get("status", "unknown"))
        logger.info("Duration: %ss", duration)
        logger.info("Cities: %s", extraction_metrics.get("cities", []))
        logger.info("=" * 60)

    extraction = extract_weather()
    (
        extraction
        >> dbt_deps
        >> dbt_run
        >> dbt_test
        >> soda_scan
        >> elementary_report
        >> push_metrics(extraction)
    )


weather_pipeline()
