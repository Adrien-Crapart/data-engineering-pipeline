"""Centralized constants for all DAGs — timezone, pools, start date, asset URIs."""

from __future__ import annotations

import pendulum


def normalize_host_path(path: str) -> str:
    """Convert a Windows drive path to a Docker-compatible POSIX path.

    When DockerOperator bind-mounts a host directory, the path is sent
    directly to the Docker daemon running inside Docker Desktop's Linux VM.
    That daemon requires POSIX-absolute paths (starting with '/'), so a
    Windows path like ``C:/Users/foo`` must become ``/c/Users/foo``.

    On Linux/macOS the path is already POSIX-absolute and is returned as-is.
    """
    if len(path) >= 2 and path[1] == ":":
        drive = path[0].lower()
        rest = path[2:].replace("\\", "/")
        return f"/{drive}{rest}"
    return path.replace("\\", "/")


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
ASSET_RAW_WEATHER_S3 = "s3://data-lake/raw/openweather"
ASSET_STAGING_WEATHER = "pg://datawarehouse/staging/stg_weather_current"
ASSET_STAGING_VALIDATED = "pg://datawarehouse/staging/validated"
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
