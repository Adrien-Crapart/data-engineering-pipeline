"""
OpenMetadata Ingestion DAG — Synchronize metadata from all data sources
into OpenMetadata for lineage, quality, and governance.

Registers and runs metadata ingestion for:
  1. PostgreSQL (datawarehouse) — tables, columns, lineage
  2. dbt — models, tests, lineage from manifest/catalog
  3. MinIO S3 — raw data assets
  4. Airflow — pipeline lineage

Schedule: Daily at 03:00 Europe/Paris (after all pipelines have run).
"""

from __future__ import annotations

import json
import logging
from datetime import timedelta

from airflow.models.param import Param
from airflow.sdk import dag, task

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
    description="Sync metadata from PostgreSQL, dbt, S3, and Airflow into OpenMetadata",
    schedule="0 3 * * *",
    start_date=DAG_START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["openmetadata", "governance", "lineage", "metadata"],
    doc_md=__doc__,
    params={
        "force_refresh": Param(default=False, type="boolean", description="Force full metadata refresh"),
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
                        "includes": ["staging", "core", "mart", "analytic", "raw"],
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
        bucket = Variable.get("minio_bucket_name", default_var="weather-data-lake")

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

    pg = register_postgres_service()
    s3 = register_s3_service()
    meta = trigger_metadata_ingestion(pg)
    dbt_lin = register_dbt_pipeline(pg)

    [pg, s3] >> meta >> dbt_lin


openmetadata_ingestion()
