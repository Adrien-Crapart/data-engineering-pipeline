"""Centralized constants for all DAGs — timezone, pools, start date, asset URIs."""

from __future__ import annotations

import pendulum

TIMEZONE = "Europe/Paris"
TZ = pendulum.timezone(TIMEZONE)
DAG_START_DATE = pendulum.datetime(2025, 1, 1, tz=TIMEZONE)

# ---------------------------------------------------------------------------
# Custom Pools (created by orchestration/scripts/airflow_config.sh)
# ---------------------------------------------------------------------------
POOL_DOCKER = "docker_pool"
POOL_DATABASE = "database_pool"
POOL_API = "api_pool"

# ---------------------------------------------------------------------------
# Airflow Connection IDs
# ---------------------------------------------------------------------------
CONN_SLACK_WEBHOOK = "slack_webhook"
CONN_MINIO_S3 = "minio_s3"
CONN_POSTGRES = "weather_pipeline_pg"
CONN_SMTP = "smtp_default"

# ---------------------------------------------------------------------------
# Asset URIs (shared between producer / consumer DAGs)
# ---------------------------------------------------------------------------
ASSET_RAW_WEATHER_S3 = "s3://weather-data-lake/raw/openweather"
ASSET_MART_WEATHER = "pg://datawarehouse/mart/weather_daily_summary"

# ---------------------------------------------------------------------------
# Alert Severity Levels
# ---------------------------------------------------------------------------
SEVERITY_CRITICAL = "critical"
SEVERITY_WARNING = "warning"
SEVERITY_INFO = "info"

# ---------------------------------------------------------------------------
# Docker images & resources (defaults — overridden by Airflow Variables)
# ---------------------------------------------------------------------------
DEFAULT_DLT_IMAGE = "weather-pipeline-dlt:1.0.0"
DEFAULT_DBT_IMAGE = "weather-pipeline-dbt:1.0.0"
DEFAULT_SODA_IMAGE = "weather-pipeline-soda:1.0.0"
DEFAULT_DLT_MEM = "512m"
DEFAULT_DBT_MEM = "512m"
DEFAULT_SODA_MEM = "256m"
DEFAULT_NETWORK = "data-engineering-pipeline_pipeline-network"
