"""
Weather Pipeline DAG - End-to-end orchestration.

Extracts weather data from OpenWeather API using dlt, transforms it
with dbt, and validates data quality with Soda Core.

Schedule: Every 6 hours.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

logger = logging.getLogger(__name__)

DBT_DIR = "/opt/airflow/dbt"
DBT_BIN = "/usr/python/bin/dbt"
SODA_BIN = "/usr/python/bin/soda"
DATA_QUALITY_DIR = "/opt/airflow/data_quality"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}


@dag(
    dag_id="weather_pipeline",
    description="End-to-end weather data pipeline: ingest → transform → validate",
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
        from ingestion.openweather_pipeline import run_pipeline

        start = time.time()
        metrics = run_pipeline()
        metrics["task_duration_seconds"] = round(time.time() - start, 2)
        logger.info("Extraction complete: %s", metrics)
        return metrics

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} deps --profiles-dir .",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && {DBT_BIN} test --profiles-dir .",
    )

    soda_scan = BashOperator(
        task_id="soda_scan",
        bash_command=(
            f"{SODA_BIN} scan "
            f"-d weather_db "
            f"-c {DATA_QUALITY_DIR}/configuration.yml "
            f"{DATA_QUALITY_DIR}/checks/"
        ),
    )

    @task()
    def log_pipeline_metrics(extraction_metrics: dict[str, Any]) -> None:
        """Log final pipeline metrics summary."""
        logger.info("=" * 60)
        logger.info("PIPELINE RUN COMPLETE")
        logger.info("Extraction: %s", extraction_metrics.get("status", "unknown"))
        logger.info("Duration: %ss", extraction_metrics.get("task_duration_seconds", "?"))
        logger.info("Cities: %s", extraction_metrics.get("cities", []))
        logger.info("=" * 60)

    extraction = extract_weather()
    extraction >> dbt_deps >> dbt_run >> dbt_test >> soda_scan >> log_pipeline_metrics(extraction)


weather_pipeline()
