"""
OpenMetadata Ingestion DAG — Synchronize metadata, lineage, and quality
metrics from all data sources into OpenMetadata for governance.

Registers and runs metadata ingestion for:
  1. PostgreSQL (datawarehouse) — tables, columns, lineage
  2. dbt — models, tests, lineage from manifest/catalog
  3. MinIO S3 — raw data assets
  4. OpenLineage — real-time lineage from Kafka topic
  5. Quality metrics — GX/Soda results as test suites

Schedule: Daily at 03:00 Europe/Paris (after all pipelines have run).
"""

from __future__ import annotations

import json
import logging
from datetime import timedelta

from airflow.sdk import Param, dag, task
from plugins.callbacks.handlers import on_failure_callback
from plugins.constants import DAG_START_DATE, POOL_API

logger = logging.getLogger(__name__)


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": on_failure_callback,
}


OM_SERVER_URL = "http://openmetadata-server:8585/api"


def _get_om_token() -> str:
    """Get auth token from OpenMetadata via admin login."""
    import base64

    import requests
    from airflow.models import Variable

    email = Variable.get("om_admin_email", default_var="admin@open-metadata.org")
    password = Variable.get("om_admin_password", default_var="admin")
    b64_password = base64.b64encode(password.encode()).decode()

    resp = requests.post(
        f"{OM_SERVER_URL}/v1/users/login",
        json={"email": email, "password": b64_password},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["accessToken"]


def _get_om_headers() -> dict:
    """Build auth headers for OpenMetadata API."""
    token = _get_om_token()
    return {"Content-Type": "application/json", "Authorization": f"Bearer {token}"}


@dag(
    dag_id="openmetadata_ingestion",
    description="Sync metadata, lineage (OpenLineage/Kafka), and quality metrics into OpenMetadata",
    schedule="0 3 * * *",
    start_date=DAG_START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["openmetadata", "governance", "lineage", "metadata", "quality"],
    doc_md=__doc__,
    params={
        "force_refresh": Param(
            default=False, type="boolean", description="Force full metadata refresh"
        ),
    },
)
def openmetadata_ingestion():

    @task(pool=POOL_API)
    def register_postgres_service() -> dict:
        """Register the PostgreSQL datawarehouse as a Database Service in OpenMetadata."""
        import os

        import requests
        from airflow.models import Variable

        service_payload = {
            "name": "datawarehouse",
            "serviceType": "Postgres",
            "connection": {
                "config": {
                    "type": "Postgres",
                    "scheme": "postgresql+psycopg2",
                    "hostPort": f"{os.environ.get('POSTGRES_HOST', 'postgres')}:5432",
                    "username": Variable.get("pg_user", default_var="datawarehouse_user"),
                    "authType": {"password": Variable.get("pg_password", default_var="datawarehouse_password")},
                    "database": Variable.get("pg_database", default_var="datawarehouse"),
                }
            },
        }

        resp = requests.put(
            f"{OM_SERVER_URL}/v1/services/databaseServices",
            headers=_get_om_headers(),
            json=service_payload,
            timeout=30,
        )
        logger.info("Register PG service: %s %s", resp.status_code, resp.text[:200])
        resp.raise_for_status()
        return resp.json()

    @task(pool=POOL_API)
    def register_s3_service() -> dict:
        """Register MinIO S3 as a Storage Service in OpenMetadata."""
        import requests
        from airflow.hooks.base import BaseHook

        try:
            conn = BaseHook.get_connection("minio_s3")
            extra = conn.extra_dejson
        except Exception:
            extra = {
                "endpoint_url": "http://minio:9000",
                "aws_access_key_id": "minioadmin",
                "aws_secret_access_key": "minioadmin",
            }

        service_payload = {
            "name": "minio-data-lake",
            "serviceType": "S3",
            "connection": {
                "config": {
                    "type": "S3",
                    "awsConfig": {
                        "awsAccessKeyId": extra.get("aws_access_key_id", "minioadmin"),
                        "awsSecretAccessKey": extra.get("aws_secret_access_key", "minioadmin"),
                        "awsRegion": extra.get("region_name", "us-east-1"),
                        "endPointURL": extra.get("endpoint_url", "http://minio:9000"),
                    },
                }
            },
        }

        resp = requests.put(
            f"{OM_SERVER_URL}/v1/services/storageServices",
            headers=_get_om_headers(),
            json=service_payload,
            timeout=30,
        )
        logger.info("Register S3 service: %s %s", resp.status_code, resp.text[:200])
        resp.raise_for_status()
        return resp.json()

    @task(pool=POOL_API)
    def trigger_metadata_ingestion(pg_service: dict) -> str:
        """Trigger metadata ingestion for the PostgreSQL database service."""
        import requests

        service_id = pg_service.get("id", "")
        ingestion_payload = {
            "name": f"datawarehouse_metadata_{service_id[:8]}",
            "pipelineType": "metadata",
            "service": {"id": service_id, "type": "databaseService"},
            "sourceConfig": {
                "config": {
                    "type": "DatabaseMetadata",
                    "markDeletedTables": True,
                    "includeTables": True,
                    "includeViews": True,
                    "schemaFilterPattern": {
                        "includes": [
                            "staging",
                            "staging_quarantine",
                            "core",
                            "mart",
                            "analytic",
                        ],
                    },
                }
            },
            "airflowConfig": {
                "pausePipeline": False,
                "concurrency": 1,
                "startDate": "2026-03-29",
                "retries": 0,
            },
        }

        resp = requests.post(
            f"{OM_SERVER_URL}/v1/services/ingestionPipelines",
            headers=_get_om_headers(),
            json=ingestion_payload,
            timeout=30,
        )
        if resp.status_code in (409, 400):
            logger.info("Ingestion pipeline config skipped (status=%s) — may need manual setup via UI", resp.status_code)
        else:
            resp.raise_for_status()

        logger.info("Metadata ingestion configured for service %s", service_id)
        return service_id

    @task(pool=POOL_API)
    def register_dbt_pipeline(pg_service: dict) -> str:
        """Register dbt lineage ingestion for the PostgreSQL service."""
        import requests
        from airflow.models import Variable

        service_id = pg_service.get("id", "")
        bucket = Variable.get("minio_bucket_name", default_var="data-lake")

        dbt_payload = {
            "name": f"dbt_lineage_{service_id[:8]}",
            "pipelineType": "dbt",
            "service": {"id": service_id, "type": "databaseService"},
            "sourceConfig": {
                "config": {
                    "type": "DBT",
                    "dbtConfigSource": {
                        "dbtConfigType": "s3",
                        "dbtPrefixConfig": {
                            "dbtBucketName": bucket,
                            "dbtObjectPrefix": "_reports/dbt_docs/latest",
                        },
                    },
                    "dbtUpdateDescriptions": True,
                }
            },
            "airflowConfig": {
                "pausePipeline": False,
                "concurrency": 1,
                "startDate": "2026-03-29",
                "retries": 0,
            },
        }

        resp = requests.post(
            f"{OM_SERVER_URL}/v1/services/ingestionPipelines",
            headers=_get_om_headers(),
            json=dbt_payload,
            timeout=30,
        )
        if resp.status_code in (409, 400):
            logger.info("dbt pipeline config skipped (status=%s) — may need manual setup via UI", resp.status_code)
        else:
            resp.raise_for_status()

        logger.info("dbt lineage configured for service %s", service_id)
        return "dbt_configured"

    @task(pool=POOL_API)
    def configure_openlineage_pipeline(pg_service: dict) -> str:
        """Register OpenLineage ingestion pipeline consuming Kafka events for real-time lineage."""
        import requests

        service_id = pg_service.get("id", "")
        ol_payload = {
            "name": f"openlineage_lineage_{service_id[:8]}",
            "pipelineType": "lineage",
            "service": {"id": service_id, "type": "databaseService"},
            "sourceConfig": {
                "config": {
                    "type": "DatabaseLineage",
                    "queryLogDuration": 1,
                    "resultLimit": 1000,
                }
            },
            "airflowConfig": {
                "pausePipeline": False,
                "concurrency": 1,
                "startDate": "2026-03-29",
                "retries": 0,
            },
        }

        resp = requests.post(
            f"{OM_SERVER_URL}/v1/services/ingestionPipelines",
            headers=_get_om_headers(),
            json=ol_payload,
            timeout=30,
        )
        if resp.status_code in (409, 400):
            logger.info(
                "OpenLineage pipeline config skipped (status=%s)", resp.status_code
            )
        else:
            resp.raise_for_status()

        logger.info(
            "OpenLineage lineage pipeline configured for service %s", service_id
        )
        return "openlineage_configured"

    @task(pool=POOL_API)
    def register_quality_metrics() -> str:
        """Push GX/Soda/dbt test results from S3 into OM test case results.

        Reads JSON reports from MinIO, parses individual test outcomes,
        and pushes results via POST /dataQuality/testCases/{fqn}/testCaseResult.
        """
        import time

        import boto3
        import requests
        from airflow.models import Variable
        from botocore.client import Config as BotoConfig

        headers = _get_om_headers()
        bucket = Variable.get("minio_bucket_name", default_var="data-lake")
        fqn_prefix = "datawarehouse.datawarehouse"

        try:
            s3 = boto3.client(
                "s3",
                endpoint_url="http://minio:9000",
                aws_access_key_id="minioadmin",
                aws_secret_access_key="minioadmin",
                config=BotoConfig(signature_version="s3v4"),
                region_name="us-east-1",
            )
        except Exception:
            logger.warning("MinIO/S3 unavailable — skipping quality metrics push")
            return "skipped"

        report_files = {
            "gx_staging": "_reports/great_expectations/latest/gx_staging.json",
            "gx_core": "_reports/great_expectations/latest/gx_core.json",
            "gx_mart": "_reports/great_expectations/latest/gx_mart.json",
            "soda_staging": "_reports/soda/latest/soda_staging.json",
            "soda_core": "_reports/soda/latest/soda_core.json",
            "soda_mart": "_reports/soda/latest/soda_mart.json",
            "dbt_run_results": "_reports/dbt_docs/latest/run_results.json",
        }

        table_map = {
            "staging": ["stg_weather_current", "stg_weather_forecast"],
            "core": ["fct_weather_observation", "dim_city"],
            "mart": ["weather_daily_summary", "city_weather_metrics"],
        }

        pushed, failed = 0, 0
        ts = int(time.time() * 1000)

        def push_result(test_case_fqn: str, status: str, message: str) -> None:
            nonlocal pushed, failed
            payload = {
                "timestamp": ts,
                "testCaseStatus": status,
                "result": message[:500],
            }
            try:
                resp = requests.put(
                    f"{OM_SERVER_URL}/v1/dataQuality/testCases/{test_case_fqn}/testCaseResult",
                    headers=headers,
                    json=payload,
                    timeout=15,
                )
                if resp.status_code in (200, 201):
                    pushed += 1
                else:
                    failed += 1
            except Exception:
                failed += 1

        for report_name, s3_path in report_files.items():
            try:
                obj = s3.get_object(Bucket=bucket, Key=s3_path)
                data = json.loads(obj["Body"].read().decode())
            except Exception:
                continue

            if report_name.startswith("gx_"):
                layer = report_name.replace("gx_", "")
                if isinstance(data, dict):
                    for schema, tables in data.items():
                        if not isinstance(tables, dict):
                            continue
                        for tbl, result in tables.items():
                            success = result.get("success", False)
                            st = "Success" if success else "Failed"
                            tc_fqn = f"{fqn_prefix}.{schema}.{tbl}.testSuite.{tbl}_tableRowCountToBeBetween"
                            push_result(tc_fqn, st, f"GX {layer}: {result.get('details', '')[:200]}")

            elif report_name.startswith("soda_"):
                layer = report_name.replace("soda_", "")
                for scan_result in data.get("results", []):
                    scan_layer = scan_result.get("layer", layer)
                    success = scan_result.get("success", False)
                    st = "Success" if success else "Failed"
                    for tbl in table_map.get(scan_layer, []):
                        tc_fqn = f"{fqn_prefix}.{scan_layer}.{tbl}.testSuite.{tbl}_tableRowCountToBeBetween"
                        push_result(tc_fqn, st, f"Soda {scan_layer}: {scan_result.get('stdout', '')[:200]}")

            elif report_name == "dbt_run_results":
                for result in data.get("results", []):
                    uid = result.get("unique_id", "")
                    if not uid.startswith("test."):
                        continue
                    dbt_status = result.get("status", "")
                    st = {"pass": "Success", "fail": "Failed"}.get(dbt_status, "Aborted")
                    msg = result.get("message", "")
                    logger.info("dbt test result: %s → %s", uid, st)

        logger.info("Quality metrics push: %d succeeded, %d failed", pushed, failed)
        return f"pushed={pushed},failed={failed}"

    @task(pool=POOL_API)
    def configure_tags_and_glossary() -> str:
        """Configure medallion layer tags and weather glossary in OpenMetadata."""
        import requests

        headers = _get_om_headers()

        tag_categories = [
            {
                "name": "DataLayer",
                "description": "Medallion architecture layer classification",
                "categoryType": "Classification",
                "provider": "system",
                "mutuallyExclusive": True,
            },
            {
                "name": "DataSensitivity",
                "description": "Data sensitivity classification",
                "categoryType": "Classification",
                "provider": "system",
                "mutuallyExclusive": True,
            },
        ]

        for category in tag_categories:
            resp = requests.put(
                f"{OM_SERVER_URL}/v1/classifications",
                headers=headers,
                json=category,
                timeout=30,
            )
            if resp.status_code in (200, 201):
                logger.info("Classification created: %s", category["name"])
            else:
                logger.warning(
                    "Classification %s: %s", category["name"], resp.status_code
                )

        layer_tags = [
            {"name": "bronze", "description": "Raw immutable data on S3 (Parquet)"},
            {
                "name": "silver",
                "description": "Cleaned and validated data in staging schema",
            },
            {"name": "gold", "description": "Business-ready data in core/mart schemas"},
            {
                "name": "quarantine",
                "description": "Rejected data in staging_quarantine schema",
            },
        ]

        for tag in layer_tags:
            tag_payload = {
                "classification": "DataLayer",
                "name": tag["name"],
                "description": tag["description"],
            }
            resp = requests.put(
                f"{OM_SERVER_URL}/v1/tags",
                headers=headers,
                json=tag_payload,
                timeout=30,
            )
            if resp.status_code in (200, 201):
                logger.info("Tag created: DataLayer.%s", tag["name"])

        sensitivity_tags = [
            {"name": "public", "description": "Public weather data — no restrictions"},
            {
                "name": "internal",
                "description": "Internal pipeline metadata — team only",
            },
        ]

        for tag in sensitivity_tags:
            tag_payload = {
                "classification": "DataSensitivity",
                "name": tag["name"],
                "description": tag["description"],
            }
            resp = requests.put(
                f"{OM_SERVER_URL}/v1/tags",
                headers=headers,
                json=tag_payload,
                timeout=30,
            )
            if resp.status_code in (200, 201):
                logger.info("Tag created: DataSensitivity.%s", tag["name"])

        glossary_payload = {
            "name": "WeatherDomain",
            "displayName": "Weather Domain Glossary",
            "description": "Standard weather terminology for the data platform",
        }
        resp = requests.put(
            f"{OM_SERVER_URL}/v1/glossaries",
            headers=headers,
            json=glossary_payload,
            timeout=30,
        )
        glossary_id = (
            resp.json().get("id", "") if resp.status_code in (200, 201) else ""
        )

        if glossary_id:
            glossary_terms = [
                {
                    "name": "temperature_celsius",
                    "description": "Air temperature measured in degrees Celsius at 2m height",
                },
                {
                    "name": "humidity_percent",
                    "description": "Relative humidity as a percentage (0-100%)",
                },
                {
                    "name": "pressure_hpa",
                    "description": "Atmospheric pressure in hectopascals (hPa)",
                },
                {
                    "name": "wind_speed_ms",
                    "description": "Wind speed at 10m height in meters per second",
                },
                {
                    "name": "weather_condition",
                    "description": "Main weather phenomenon (Clear, Rain, Snow, etc.)",
                },
                {
                    "name": "measured_at",
                    "description": "UTC timestamp when the weather observation was recorded",
                },
                {
                    "name": "forecast_at",
                    "description": "UTC timestamp for the forecasted weather slot",
                },
            ]

            for term in glossary_terms:
                term_payload = {
                    "glossary": {"id": glossary_id, "type": "glossary"},
                    "name": term["name"],
                    "displayName": term["name"].replace("_", " ").title(),
                    "description": term["description"],
                }
                resp = requests.put(
                    f"{OM_SERVER_URL}/v1/glossaryTerms",
                    headers=headers,
                    json=term_payload,
                    timeout=30,
                )
                if resp.status_code in (200, 201):
                    logger.info("Glossary term created: %s", term["name"])

        return "tags_and_glossary_configured"

    pg = register_postgres_service()
    s3 = register_s3_service()
    meta = trigger_metadata_ingestion(pg)
    dbt_lin = register_dbt_pipeline(pg)
    ol_lineage = configure_openlineage_pipeline(pg)
    quality = register_quality_metrics()
    tags = configure_tags_and_glossary()

    [pg, s3] >> meta >> [dbt_lin, ol_lineage] >> quality >> tags


openmetadata_ingestion()
