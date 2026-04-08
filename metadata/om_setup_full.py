"""Full OpenMetadata setup: tests, profiler, Airflow service, lineage, data contracts.

Provisions:
  1. Airflow as pipeline service
  2. Update profiler agent (add staging schema)
  3. Comprehensive test suites + test cases on all medallion tables
  4. Table-to-table + pipeline-to-table lineage
  5. Sync Airflow DAGs (tasks + execution history) into OM
  6. Column descriptions for data contract enrichment
  7. Trigger agents: metadata → profiler → dbt

Usage:
    python metadata/om_setup_full.py [--url http://localhost:8585] [--dry-run] [--skip-trigger]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from typing import Any

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

OM_URL = "http://localhost:8585"
EMAIL = "admin@open-metadata.org"
PASSWORD = "YWRtaW4="

AIRFLOW_API_URL = "http://localhost:8080"
AIRFLOW_INTERNAL_URL = "http://airflow-api-server:8080"
AIRFLOW_USER = "airflow"
AIRFLOW_PASSWORD = "airflow"

FQN_PREFIX = "datawarehouse.datawarehouse"
DB_SERVICE = "datawarehouse"

# ---------------------------------------------------------------------------
# Lineage definitions: medallion architecture table-to-table edges
# ---------------------------------------------------------------------------
# Each tuple: (from_schema.table, to_schema.table, description)

TABLE_LINEAGE: list[tuple[str, str, str]] = [
    # Staging → Core
    ("staging.stg_weather_current", "core.fct_weather_observation",
     "dbt transformation: cleansed staging data → core fact table"),
    ("staging.stg_weather_current", "core.dim_city",
     "dbt transformation: extract distinct cities from staging"),
    # Core → Mart
    ("core.fct_weather_observation", "mart.weather_daily_summary",
     "dbt transformation: daily aggregation of observations"),
    ("core.fct_weather_observation", "mart.city_weather_metrics",
     "dbt transformation: per-city metrics from all observations"),
    ("core.dim_city", "mart.city_weather_metrics",
     "dbt transformation: city dimension joined for metrics"),
    # Mart → Analytic
    ("mart.weather_daily_summary", "analytic.weather_trend_analysis",
     "dbt transformation: 7-day moving averages and trend analysis"),
    ("mart.city_weather_metrics", "analytic.city_comparison_ranking",
     "dbt transformation: inter-city ranking from aggregated metrics"),
    # Staging → Quarantine (rejected rows)
    ("staging.stg_weather_current", "staging_quarantine.quarantine_weather_current",
     "dbt transformation: rows failing quality checks routed to quarantine"),
    ("staging.stg_weather_forecast", "staging_quarantine.quarantine_weather_forecast",
     "dbt transformation: forecast rows failing quality checks"),
]


# ---------------------------------------------------------------------------
# Test definitions for each table in the medallion architecture
# ---------------------------------------------------------------------------

TABLE_TESTS: dict[str, dict[str, Any]] = {
    # =================== CORE ===================
    "core.fct_weather_observation": {
        "displayName": "Weather Observation Quality Suite",
        "tests": [
            {"type": "tableRowCountToBeBetween", "params": {"minValue": 1, "maxValue": 1000000}},
            {"type": "tableColumnCountToEqual", "params": {"columnCount": 15}},
            {"type": "columnValuesToBeNotNull", "column": "row_id"},
            {"type": "columnValuesToBeUnique", "column": "row_id"},
            {"type": "columnValuesToBeNotNull", "column": "city_name"},
            {"type": "columnValuesToBeNotNull", "column": "measured_at"},
            {"type": "columnValuesToBeBetween", "column": "temperature_celsius", "params": {"minValue": -60, "maxValue": 60}},
            {"type": "columnValuesToBeBetween", "column": "humidity_percent", "params": {"minValue": 0, "maxValue": 100}},
            {"type": "columnValuesToBeBetween", "column": "pressure_hpa", "params": {"minValue": 870, "maxValue": 1084}},
            {"type": "columnValuesToBeBetween", "column": "wind_speed_ms", "params": {"minValue": 0, "maxValue": 120}},
            {"type": "columnValuesToBeNotNull", "column": "weather_condition"},
            {"type": "columnValuesToBeInSet", "column": "country_code", "params": {"allowedValues": ["FR"]}},
            {"type": "columnValuesToBeNotNull", "column": "feels_like_celsius"},
            {"type": "columnValuesToBeBetween", "column": "feels_like_celsius", "params": {"minValue": -60, "maxValue": 60}},
            {"type": "columnValuesToBeBetween", "column": "cloud_coverage_percent", "params": {"minValue": 0, "maxValue": 100}},
        ],
    },
    "core.dim_city": {
        "displayName": "City Dimension Quality Suite",
        "tests": [
            {"type": "tableRowCountToBeBetween", "params": {"minValue": 1, "maxValue": 100}},
            {"type": "tableColumnCountToEqual", "params": {"columnCount": 4}},
            {"type": "columnValuesToBeNotNull", "column": "city_name"},
            {"type": "columnValuesToBeUnique", "column": "city_name"},
            {"type": "columnValuesToBeNotNull", "column": "country_code"},
            {"type": "columnValuesToBeInSet", "column": "country_code", "params": {"allowedValues": ["FR"]}},
            {"type": "columnValuesToBeBetween", "column": "latitude", "params": {"minValue": 41.0, "maxValue": 52.0}},
            {"type": "columnValuesToBeBetween", "column": "longitude", "params": {"minValue": -5.5, "maxValue": 10.0}},
        ],
    },

    # =================== MART ===================
    "mart.weather_daily_summary": {
        "displayName": "Daily Summary Quality Suite",
        "tests": [
            {"type": "tableRowCountToBeBetween", "params": {"minValue": 1, "maxValue": 500000}},
            {"type": "columnValuesToBeNotNull", "column": "city_name"},
            {"type": "columnValuesToBeNotNull", "column": "date_day"},
            {"type": "columnValuesToBeBetween", "column": "avg_temperature_celsius", "params": {"minValue": -60, "maxValue": 60}},
            {"type": "columnValuesToBeBetween", "column": "min_temperature_celsius", "params": {"minValue": -60, "maxValue": 60}},
            {"type": "columnValuesToBeBetween", "column": "max_temperature_celsius", "params": {"minValue": -60, "maxValue": 60}},
            {"type": "columnValuesToBeBetween", "column": "avg_humidity_percent", "params": {"minValue": 0, "maxValue": 100}},
            {"type": "columnValuesToBeBetween", "column": "avg_wind_speed_ms", "params": {"minValue": 0, "maxValue": 120}},
            {"type": "columnValuesToBeNotNull", "column": "observation_count"},
            {"type": "columnValuesToBeBetween", "column": "observation_count", "params": {"minValue": 1, "maxValue": 100}},
        ],
    },
    "mart.city_weather_metrics": {
        "displayName": "City Metrics Quality Suite",
        "tests": [
            {"type": "tableRowCountToBeBetween", "params": {"minValue": 1, "maxValue": 100}},
            {"type": "columnValuesToBeNotNull", "column": "city_name"},
            {"type": "columnValuesToBeUnique", "column": "city_name"},
            {"type": "columnValuesToBeBetween", "column": "avg_temperature_celsius", "params": {"minValue": -60, "maxValue": 60}},
            {"type": "columnValuesToBeBetween", "column": "avg_humidity_percent", "params": {"minValue": 0, "maxValue": 100}},
            {"type": "columnValuesToBeNotNull", "column": "total_observations"},
            {"type": "columnValuesToBeBetween", "column": "total_observations", "params": {"minValue": 1, "maxValue": 1000000}},
        ],
    },

    # =================== ANALYTIC ===================
    "analytic.weather_trend_analysis": {
        "displayName": "Trend Analysis Quality Suite",
        "tests": [
            {"type": "tableRowCountToBeBetween", "params": {"minValue": 1, "maxValue": 500000}},
            {"type": "columnValuesToBeNotNull", "column": "city_name"},
            {"type": "columnValuesToBeNotNull", "column": "date_day"},
            {"type": "columnValuesToBeBetween", "column": "avg_temperature_celsius", "params": {"minValue": -60, "maxValue": 60}},
            {"type": "columnValuesToBeBetween", "column": "temp_7d_moving_avg", "params": {"minValue": -60, "maxValue": 60}},
            {"type": "columnValuesToBeBetween", "column": "humidity_7d_moving_avg", "params": {"minValue": 0, "maxValue": 100}},
        ],
    },
    "analytic.city_comparison_ranking": {
        "displayName": "City Ranking Quality Suite",
        "tests": [
            {"type": "tableRowCountToBeBetween", "params": {"minValue": 1, "maxValue": 100}},
            {"type": "columnValuesToBeNotNull", "column": "city_name"},
            {"type": "columnValuesToBeUnique", "column": "city_name"},
            {"type": "columnValuesToBeBetween", "column": "avg_temperature_celsius", "params": {"minValue": -60, "maxValue": 60}},
        ],
    },

    # =================== STAGING ===================
    "staging.stg_weather_current": {
        "displayName": "Staging Current Weather Quality Suite",
        "tests": [
            {"type": "tableRowCountToBeBetween", "params": {"minValue": 1, "maxValue": 1000000}},
            {"type": "columnValuesToBeNotNull", "column": "city_name"},
            {"type": "columnValuesToBeNotNull", "column": "temperature_celsius"},
            {"type": "columnValuesToBeBetween", "column": "temperature_celsius", "params": {"minValue": -80, "maxValue": 70}},
            {"type": "columnValuesToBeBetween", "column": "humidity_percent", "params": {"minValue": 0, "maxValue": 100}},
        ],
    },
    "staging.stg_weather_forecast": {
        "displayName": "Staging Forecast Quality Suite",
        "tests": [
            {"type": "tableRowCountToBeBetween", "params": {"minValue": 1, "maxValue": 5000000}},
            {"type": "columnValuesToBeNotNull", "column": "city_name"},
            {"type": "columnValuesToBeNotNull", "column": "temperature_celsius"},
            {"type": "columnValuesToBeBetween", "column": "temperature_celsius", "params": {"minValue": -80, "maxValue": 70}},
        ],
    },

    # =================== QUARANTINE ===================
    "staging_quarantine.quarantine_weather_current": {
        "displayName": "Quarantine Current Weather Suite",
        "tests": [
            {"type": "tableRowCountToBeBetween", "params": {"minValue": 0, "maxValue": 1000000}},
            {"type": "columnValuesToBeNotNull", "column": "city_name"},
            {"type": "columnValuesToBeNotNull", "column": "rejection_reason"},
        ],
    },
    "staging_quarantine.quarantine_weather_forecast": {
        "displayName": "Quarantine Forecast Suite",
        "tests": [
            {"type": "tableRowCountToBeBetween", "params": {"minValue": 0, "maxValue": 5000000}},
            {"type": "columnValuesToBeNotNull", "column": "city_name"},
            {"type": "columnValuesToBeNotNull", "column": "rejection_reason"},
        ],
    },
}


class OMSetup:
    def __init__(self, base_url: str, dry_run: bool = False, skip_trigger: bool = False):
        self.base_url = base_url.rstrip("/")
        self.api = f"{self.base_url}/api/v1"
        self.token: str | None = None
        self.dry_run = dry_run
        self.skip_trigger = skip_trigger
        self._table_id_cache: dict[str, str] = {}
        self._pipeline_ids: dict[str, str] = {}

    def authenticate(self) -> None:
        resp = requests.post(
            f"{self.api}/users/login",
            json={"email": EMAIL, "password": PASSWORD},
            timeout=15,
        )
        resp.raise_for_status()
        self.token = resp.json()["accessToken"]
        logger.info("Authenticated to %s", self.base_url)

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    @property
    def json_headers(self) -> dict[str, str]:
        return {**self.headers, "Content-Type": "application/json"}

    @property
    def patch_headers(self) -> dict[str, str]:
        return {**self.headers, "Content-Type": "application/json-patch+json"}

    def _get(self, path: str, **kw: Any) -> requests.Response:
        return requests.get(f"{self.api}{path}", headers=self.headers, timeout=30, **kw)

    def _post(self, path: str, payload: dict, **kw: Any) -> requests.Response | None:
        if self.dry_run:
            logger.info("[DRY-RUN] POST %s", path)
            return None
        return requests.post(f"{self.api}{path}", headers=self.json_headers, json=payload, timeout=30, **kw)

    def _put(self, path: str, payload: dict) -> requests.Response | None:
        if self.dry_run:
            logger.info("[DRY-RUN] PUT %s", path)
            return None
        return requests.put(f"{self.api}{path}", headers=self.json_headers, json=payload, timeout=30)

    def _patch(self, path: str, payload: list[dict]) -> requests.Response | None:
        if self.dry_run:
            logger.info("[DRY-RUN] PATCH %s", path)
            return None
        return requests.patch(f"{self.api}{path}", headers=self.patch_headers, json=payload, timeout=30)

    def _get_table_id(self, schema: str, table: str) -> str | None:
        fqn = f"{FQN_PREFIX}.{schema}.{table}"
        if fqn in self._table_id_cache:
            return self._table_id_cache[fqn]
        resp = self._get(f"/tables/name/{fqn}")
        if resp.status_code == 200:
            tid = resp.json()["id"]
            self._table_id_cache[fqn] = tid
            return tid
        logger.warning("  Table %s not found (status=%s)", fqn, resp.status_code)
        return None

    # ---------------------------------------------------------------
    # 0a. Register database + storage services
    # ---------------------------------------------------------------
    def register_database_service(self) -> str | None:
        """Register PostgreSQL datawarehouse as a Database Service."""
        logger.info("=" * 60)
        logger.info("0a. REGISTER POSTGRESQL DATABASE SERVICE")
        logger.info("=" * 60)

        payload = {
            "name": DB_SERVICE,
            "displayName": "Data Warehouse (PostgreSQL)",
            "description": "PostgreSQL 16 — stores staging, core, mart, analytic schemas.",
            "serviceType": "Postgres",
            "connection": {
                "config": {
                    "type": "Postgres",
                    "scheme": "postgresql+psycopg2",
                    "hostPort": "postgres:5432",
                    "username": "datawarehouse_user",
                    "authType": {"password": "datawarehouse_password"},
                    "database": "datawarehouse",
                }
            },
        }
        resp = self._put("/services/databaseServices", payload)
        if resp is not None and resp.status_code in (200, 201):
            svc_id = resp.json()["id"]
            logger.info("  PG service created/updated (id=%s)", svc_id[:8])
            return svc_id
        if resp is not None:
            logger.warning("  Failed (%s): %s", resp.status_code, resp.text[:300])
        return None

    def register_storage_service(self) -> str | None:
        """Register MinIO S3 as a Storage Service."""
        logger.info("=" * 60)
        logger.info("0b. REGISTER MINIO S3 STORAGE SERVICE")
        logger.info("=" * 60)

        payload = {
            "name": "minio-data-lake",
            "displayName": "MinIO Data Lake (S3)",
            "description": "MinIO S3-compatible storage — raw Parquet (bronze), reports, dbt docs.",
            "serviceType": "S3",
            "connection": {
                "config": {
                    "type": "S3",
                    "awsConfig": {
                        "awsAccessKeyId": "minioadmin",
                        "awsSecretAccessKey": "minioadmin",
                        "awsRegion": "us-east-1",
                        "endPointURL": "http://minio:9000",
                    },
                }
            },
        }
        resp = self._put("/services/storageServices", payload)
        if resp is not None and resp.status_code in (200, 201):
            svc_id = resp.json()["id"]
            logger.info("  S3 service created/updated (id=%s)", svc_id[:8])
            return svc_id
        if resp is not None:
            logger.warning("  Failed (%s): %s", resp.status_code, resp.text[:300])
        return None

    # ---------------------------------------------------------------
    # 0c. Create, deploy, trigger metadata agents
    # ---------------------------------------------------------------
    def create_and_deploy_agents(self, pg_service_id: str | None, s3_service_id: str | None) -> None:
        """Create metadata agents for PG, S3, and dbt, then deploy and trigger them."""
        logger.info("=" * 60)
        logger.info("0c. CREATE & DEPLOY METADATA AGENTS")
        logger.info("=" * 60)

        if pg_service_id:
            self._ensure_agent(
                name="datawarehouse_metadata",
                pipeline_type="metadata",
                service_id=pg_service_id,
                service_type="databaseService",
                source_config={
                    "type": "DatabaseMetadata",
                    "markDeletedTables": True,
                    "includeTables": True,
                    "includeViews": True,
                    "schemaFilterPattern": {
                        "includes": ["staging", "staging_quarantine", "core", "mart", "analytic"],
                    },
                },
            )

            self._ensure_agent(
                name="datawarehouse_dbt",
                pipeline_type="dbt",
                service_id=pg_service_id,
                service_type="databaseService",
                source_config={
                    "type": "DBT",
                    "dbtConfigSource": {
                        "dbtConfigType": "s3",
                        "dbtPrefixConfig": {
                            "dbtBucketName": "data-lake",
                            "dbtObjectPrefix": "_reports/dbt_docs/latest",
                        },
                        "dbtSecurityConfig": {
                            "awsAccessKeyId": "minioadmin",
                            "awsSecretAccessKey": "minioadmin",
                            "awsRegion": "us-east-1",
                            "endPointURL": "http://minio:9000",
                        },
                    },
                    "dbtUpdateDescriptions": True,
                },
            )

        if s3_service_id:
            self._ensure_agent(
                name="minio_metadata",
                pipeline_type="metadata",
                service_id=s3_service_id,
                service_type="storageService",
                source_config={
                    "type": "StorageMetadata",
                    "containerFilterPattern": {
                        "includes": ["data-lake"],
                    },
                },
            )

    def _ensure_agent(self, name: str, pipeline_type: str,
                      service_id: str, service_type: str,
                      source_config: dict) -> None:
        """Create (or update) an ingestion pipeline, then deploy and trigger it."""
        existing = self._get(f"/services/ingestionPipelines?limit=50")
        found_id = None
        if existing.status_code == 200:
            for p in existing.json().get("data", []):
                if p.get("name", "") == name:
                    found_id = p["id"]
                    break

        if found_id:
            logger.info("  Agent '%s' already exists (id=%s) — triggering", name, found_id[:8])
            self._deploy_and_trigger(found_id, name)
            return

        payload = {
            "name": name,
            "pipelineType": pipeline_type,
            "service": {"id": service_id, "type": service_type},
            "sourceConfig": {"config": source_config},
            "airflowConfig": {
                "pausePipeline": False,
                "concurrency": 1,
                "startDate": "2026-01-01",
                "retries": 0,
            },
        }

        resp = self._post(f"/services/ingestionPipelines", payload)
        if resp is not None and resp.status_code in (200, 201):
            pid = resp.json()["id"]
            logger.info("  Agent '%s' created (id=%s)", name, pid[:8])
            self._deploy_and_trigger(pid, name)
        elif resp is not None and resp.status_code in (400, 409):
            logger.info("  Agent '%s' config exists (status=%s)", name, resp.status_code)
        elif resp is not None:
            logger.warning("  Agent '%s' creation failed (%s): %s", name, resp.status_code, resp.text[:300])

    def _deploy_and_trigger(self, pipeline_id: str, name: str) -> None:
        """Deploy then trigger an ingestion pipeline."""
        if self.dry_run:
            logger.info("  [DRY-RUN] Would deploy+trigger %s", name)
            return

        deploy_resp = self._post(f"/services/ingestionPipelines/deploy/{pipeline_id}", {})
        if deploy_resp is not None and deploy_resp.status_code in (200, 201):
            logger.info("    Deployed '%s'", name)
        elif deploy_resp is not None:
            logger.warning("    Deploy '%s' failed (%s) — trying trigger anyway", name, deploy_resp.status_code)

        trigger_resp = self._post(f"/services/ingestionPipelines/trigger/{pipeline_id}", {})
        if trigger_resp is not None and trigger_resp.status_code in (200, 201):
            logger.info("    Triggered '%s'", name)
        elif trigger_resp is not None:
            logger.warning("    Trigger '%s' failed (%s): %s", name, trigger_resp.status_code,
                           trigger_resp.text[:200])

    # ---------------------------------------------------------------
    # 1. Register Airflow as Pipeline Service + Metadata Agent
    # ---------------------------------------------------------------
    def register_airflow_service(self) -> None:
        logger.info("=" * 60)
        logger.info("1. REGISTER AIRFLOW PIPELINE SERVICE + METADATA AGENT")
        logger.info("=" * 60)

        payload = {
            "name": "airflow",
            "displayName": "Airflow Orchestration",
            "description": "Apache Airflow 3.x orchestrating weather data pipelines.",
            "serviceType": "Airflow",
            "connection": {
                "config": {
                    "type": "Airflow",
                    "hostPort": AIRFLOW_INTERNAL_URL,
                    "numberOfStatus": 10,
                }
            },
        }
        resp = self._put("/services/pipelineServices", payload)
        if resp is not None and resp.status_code in (200, 201):
            svc_id = resp.json()["id"]
            logger.info("  Airflow service created/updated (id=%s)", svc_id[:8])
        elif resp is not None:
            logger.warning("  Failed (%s): %s", resp.status_code, resp.text[:300])

    # ---------------------------------------------------------------
    # 2. Update Profiler Agent (add staging schema)
    # ---------------------------------------------------------------
    def update_profiler_agent(self) -> None:
        logger.info("=" * 60)
        logger.info("2. UPDATE PROFILER AGENT (add staging, ensure full metrics)")
        logger.info("=" * 60)

        pipelines = self._get("/services/ingestionPipelines?limit=50&fields=sourceConfig").json()
        profiler_pipeline = None
        for p in pipelines.get("data", []):
            if p.get("pipelineType") == "profiler":
                profiler_pipeline = p
                self._pipeline_ids["profiler"] = p["id"]
                break

        if not profiler_pipeline:
            logger.warning("  No profiler pipeline found — create one via UI first")
            return

        pid = profiler_pipeline["id"]
        current_config = profiler_pipeline.get("sourceConfig", {}).get("config", {})
        current_includes = current_config.get("schemaFilterPattern", {}).get("includes", [])
        logger.info("  Current schema includes: %s", current_includes)

        required_schemas = {"staging", "core", "mart", "analytic"}
        if required_schemas.issubset(set(current_includes)):
            logger.info("  Profiler already includes all schemas — no changes needed")
            return

        new_config = {**current_config}
        new_config["schemaFilterPattern"] = {
            "includes": sorted(required_schemas | set(current_includes)),
            "excludes": [],
        }

        patch_payload = [
            {"op": "replace", "path": "/sourceConfig/config", "value": new_config},
        ]
        resp = self._patch(f"/services/ingestionPipelines/{pid}", patch_payload)
        if resp is not None and resp.status_code in (200, 201):
            logger.info("  Profiler updated: staging schema added to includes")
        elif resp is not None:
            logger.warning("  Patch failed (%s): %s", resp.status_code, resp.text[:400])

    # ---------------------------------------------------------------
    # 3. Create comprehensive test suites and test cases
    # ---------------------------------------------------------------
    def create_test_suites(self) -> None:
        logger.info("=" * 60)
        logger.info("3. CREATE TEST SUITES & TEST CASES")
        logger.info("=" * 60)

        for schema_table, cfg in TABLE_TESTS.items():
            schema, table = schema_table.split(".", 1)
            table_id = self._get_table_id(schema, table)
            if not table_id:
                logger.warning("  SKIP %s — table not found in OM", schema_table)
                continue

            table_fqn = f"{FQN_PREFIX}.{schema}.{table}"
            suite_name = f"{table}.testSuite"
            suite_display = cfg["displayName"]

            logger.info("\n  --- %s (%d tests) ---", schema_table, len(cfg["tests"]))

            suite_payload = {
                "name": suite_name,
                "displayName": suite_display,
                "description": f"Data quality test suite for {schema_table} ({schema} layer).",
                "executableEntityReference": table_fqn,
            }
            resp = self._put("/dataQuality/testSuites", suite_payload)
            if resp is not None and resp.status_code in (200, 201):
                logger.info("  Suite '%s' → OK (id=%s)", suite_name, resp.json()["id"][:8])
            elif resp is not None:
                logger.warning("  Suite '%s' → %s: %s", suite_name, resp.status_code, resp.text[:200])

            for test in cfg["tests"]:
                self._create_test_case(table_fqn, test)

    def _create_test_case(self, table_fqn: str, test: dict) -> None:
        test_type = test["type"]
        column = test.get("column")
        params = test.get("params", {})

        if column:
            entity_link = f"<#E::table::{table_fqn}::columns::{column}>"
            case_name = f"{table_fqn.split('.')[-1]}_{column}_{test_type}"
        else:
            entity_link = f"<#E::table::{table_fqn}>"
            case_name = f"{table_fqn.split('.')[-1]}_{test_type}"

        case_name = case_name[:128]

        param_values = [{"name": k, "value": str(v)} for k, v in params.items()]

        payload = {
            "name": case_name,
            "testDefinition": test_type,
            "entityLink": entity_link,
            "parameterValues": param_values,
        }

        resp = self._put("/dataQuality/testCases", payload)
        if resp is not None and resp.status_code in (200, 201):
            logger.info("    + %s → OK", case_name[:60])
        elif resp is not None and resp.status_code == 409:
            logger.info("    = %s → exists", case_name[:60])
        elif resp is not None:
            logger.warning("    ! %s → FAILED (%s): %s", case_name[:60], resp.status_code, resp.text[:200])

    # ---------------------------------------------------------------
    # 4. Create lineage edges (table-to-table + pipeline-to-table)
    # ---------------------------------------------------------------
    def create_lineage(self) -> None:
        logger.info("=" * 60)
        logger.info("4. CREATE LINEAGE (table→table + pipeline→table)")
        logger.info("=" * 60)

        self._create_table_lineage()
        self._create_pipeline_lineage()

    def _create_table_lineage(self) -> None:
        logger.info("\n  --- Table-to-Table Lineage (dbt medallion) ---")
        for from_st, to_st, desc in TABLE_LINEAGE:
            from_schema, from_table = from_st.split(".", 1)
            to_schema, to_table = to_st.split(".", 1)

            from_id = self._get_table_id(from_schema, from_table)
            to_id = self._get_table_id(to_schema, to_table)
            if not from_id or not to_id:
                logger.warning("  SKIP %s → %s (table not found)", from_st, to_st)
                continue

            payload = {
                "edge": {
                    "fromEntity": {"id": from_id, "type": "table"},
                    "toEntity": {"id": to_id, "type": "table"},
                    "lineageDetails": {
                        "description": desc,
                        "source": "Manual",
                    },
                },
            }
            resp = self._put("/lineage", payload)
            if resp is not None and resp.status_code in (200, 201):
                logger.info("    + %s → %s OK", from_st, to_st)
            elif resp is not None and resp.status_code == 409:
                logger.info("    = %s → %s (exists)", from_st, to_st)
            elif resp is not None:
                logger.warning("    ! %s → %s FAILED (%s): %s",
                               from_st, to_st, resp.status_code, resp.text[:300])

        self._create_s3_bronze_lineage()

    def _create_s3_bronze_lineage(self) -> None:
        """Create lineage edges from S3 bronze containers to staging tables."""
        logger.info("\n  --- S3 Bronze → Staging Lineage ---")
        s3_service_name = "minio-data-lake"

        s3_to_staging = [
            ("raw/openweather/data/weather_current", "staging.stg_weather_current",
             "DLT filesystem → dbt read_parquet(): raw Parquet to staging"),
            ("raw/openweather/data/weather_forecast", "staging.stg_weather_forecast",
             "DLT filesystem → dbt read_parquet(): raw Parquet to staging"),
        ]

        resp = self._get(f"/services/storageServices/name/{s3_service_name}")
        if resp.status_code != 200:
            logger.warning("  S3 service '%s' not found — skipping bronze lineage", s3_service_name)
            return

        for s3_path, to_st, desc in s3_to_staging:
            to_schema, to_table = to_st.split(".", 1)
            to_id = self._get_table_id(to_schema, to_table)
            if not to_id:
                logger.warning("  SKIP S3:%s → %s (table not found)", s3_path, to_st)
                continue

            container_fqn = f"{s3_service_name}.data-lake.{s3_path.replace('/', '.')}"
            cr = self._get(f"/containers/name/{container_fqn}")
            if cr.status_code == 200:
                container_id = cr.json()["id"]
                payload = {
                    "edge": {
                        "fromEntity": {"id": container_id, "type": "container"},
                        "toEntity": {"id": to_id, "type": "table"},
                        "lineageDetails": {
                            "description": desc,
                            "source": "Manual",
                        },
                    },
                }
                resp2 = self._put("/lineage", payload)
                if resp2 is not None and resp2.status_code in (200, 201):
                    logger.info("    + S3:%s → %s OK", s3_path, to_st)
                elif resp2 is not None:
                    logger.info("    ~ S3:%s → %s (%s)", s3_path, to_st, resp2.status_code)
            else:
                logger.info("    ~ S3 container '%s' not yet indexed — run S3 metadata agent first", container_fqn)

    def _get_or_create_pipeline(self, dag_id: str, display: str, description: str) -> str | None:
        """Get or create an Airflow pipeline entity in OM."""
        fqn = f"airflow.{dag_id}"
        resp = self._get(f"/pipelines/name/{fqn}")
        if resp.status_code == 200:
            return resp.json()["id"]

        payload = {
            "name": dag_id,
            "displayName": display,
            "description": description,
            "service": "airflow",
        }
        resp = self._put("/pipelines", payload)
        if resp is not None and resp.status_code in (200, 201):
            pid = resp.json()["id"]
            logger.info("    Created pipeline entity: %s (id=%s)", fqn, pid[:8])
            return pid
        if resp is not None:
            logger.warning("    Failed to create pipeline %s (%s): %s",
                           dag_id, resp.status_code, resp.text[:300])
        return None

    def _create_pipeline_lineage(self) -> None:
        logger.info("\n  --- Pipeline-to-Table Lineage (Airflow DAGs) ---")

        dags = {
            "ingestion_pipeline": {
                "display": "Ingestion Pipeline",
                "description": "DLT extracts weather data from OpenWeather API → S3 Parquet → dbt staging",
                "outputs": ["staging.stg_weather_current", "staging.stg_weather_forecast"],
            },
            "quality_gate_pipeline": {
                "display": "Quality Gate Pipeline",
                "description": "GX + Soda validate staging tables after ingestion",
                "inputs": ["staging.stg_weather_current", "staging.stg_weather_forecast"],
            },
            "transformation_pipeline": {
                "display": "Transformation Pipeline",
                "description": "dbt transforms staging → core → mart → analytic; GX/Soda validate core/mart",
                "inputs": ["staging.stg_weather_current", "staging.stg_weather_forecast"],
                "outputs": [
                    "core.fct_weather_observation", "core.dim_city",
                    "mart.weather_daily_summary", "mart.city_weather_metrics",
                    "analytic.weather_trend_analysis", "analytic.city_comparison_ranking",
                    "staging_quarantine.quarantine_weather_current",
                    "staging_quarantine.quarantine_weather_forecast",
                ],
            },
        }

        for dag_id, cfg in dags.items():
            pipeline_id = self._get_or_create_pipeline(
                dag_id, cfg["display"], cfg["description"],
            )
            if not pipeline_id:
                continue

            for schema_table in cfg.get("inputs", []):
                schema, table = schema_table.split(".", 1)
                table_id = self._get_table_id(schema, table)
                if not table_id:
                    continue
                payload = {
                    "edge": {
                        "fromEntity": {"id": table_id, "type": "table"},
                        "toEntity": {"id": pipeline_id, "type": "pipeline"},
                    },
                }
                resp = self._put("/lineage", payload)
                if resp is not None and resp.status_code in (200, 201):
                    logger.info("    + %s → pipeline:%s OK", schema_table, dag_id)
                elif resp is not None:
                    logger.warning("    ! %s → pipeline:%s FAILED (%s): %s",
                                   schema_table, dag_id, resp.status_code, resp.text[:200])

            for schema_table in cfg.get("outputs", []):
                schema, table = schema_table.split(".", 1)
                table_id = self._get_table_id(schema, table)
                if not table_id:
                    continue
                payload = {
                    "edge": {
                        "fromEntity": {"id": pipeline_id, "type": "pipeline"},
                        "toEntity": {"id": table_id, "type": "table"},
                    },
                }
                resp = self._put("/lineage", payload)
                if resp is not None and resp.status_code in (200, 201):
                    logger.info("    + pipeline:%s → %s OK", dag_id, schema_table)
                elif resp is not None:
                    logger.warning("    ! pipeline:%s → %s FAILED (%s): %s",
                                   dag_id, schema_table, resp.status_code, resp.text[:200])

    # ---------------------------------------------------------------
    # (helper) Cache pipeline IDs for triggering
    # ---------------------------------------------------------------
    def _load_pipeline_ids(self) -> None:
        pipelines = self._get("/services/ingestionPipelines?limit=50").json()
        for p in pipelines.get("data", []):
            pt = p.get("pipelineType", "")
            self._pipeline_ids[pt] = p["id"]

    # ---------------------------------------------------------------
    # 8. Trigger agents
    # ---------------------------------------------------------------
    def trigger_agents(self) -> None:
        logger.info("=" * 60)
        logger.info("8. TRIGGER AGENTS")
        logger.info("=" * 60)

        if self.skip_trigger:
            logger.info("  Skipped (--skip-trigger)")
            return

        self._load_pipeline_ids()

        order = ["metadata", "profiler", "dbt"]
        for agent_type in order:
            pid = self._pipeline_ids.get(agent_type)
            if not pid:
                logger.warning("  No %s pipeline found", agent_type)
                continue

            logger.info("  Triggering %s agent (id=%s)...", agent_type, pid[:8])
            resp = self._post(f"/services/ingestionPipelines/trigger/{pid}", {})
            if resp is not None and resp.status_code in (200, 201):
                logger.info("    %s agent triggered", agent_type)
            elif resp is not None:
                logger.warning("    Failed (%s): %s", resp.status_code, resp.text[:200])

            if agent_type != order[-1]:
                logger.info("    Waiting 5s before next agent...")
                time.sleep(5)

    # ---------------------------------------------------------------
    # 5. Sync Airflow DAGs → OM pipeline entities (tasks + executions)
    # ---------------------------------------------------------------

    TASK_DESCRIPTIONS: dict[str, dict[str, dict[str, str]]] = {
        "ingestion_pipeline": {
            "extract_weather": {
                "description": "Calls OpenWeather API for all configured French cities, "
                               "flattens JSON responses and stores raw Parquet on MinIO S3 via DLT.",
                "displayName": "Extract Weather Data",
            },
            "dbt_build_staging": {
                "description": "Runs dbt models to read S3 Parquet via DuckDB and materialize "
                               "staging + quarantine tables in PostgreSQL.",
                "displayName": "Build Staging Models (dbt)",
            },
            "finalize_ingestion": {
                "description": "Emits the staging_weather Asset to trigger downstream quality and "
                               "transformation pipelines via event-driven scheduling.",
                "displayName": "Finalize & Emit Asset",
            },
        },
        "quality_gate_pipeline": {
            "gx_validate_staging": {
                "description": "Runs Great Expectations validations on staging tables "
                               "(stg_weather_current, stg_weather_forecast) in PostgreSQL.",
                "displayName": "GX Validate Staging",
            },
            "soda_scan_staging": {
                "description": "Runs Soda Core scans on staging tables to check row counts, "
                               "freshness, nulls and schema conformance.",
                "displayName": "Soda Scan Staging",
            },
            "finalize_quality": {
                "description": "Collects GX/Soda results, uploads reports to S3, "
                               "and emits the quality_validated Asset.",
                "displayName": "Finalize Quality Gate",
            },
        },
        "transformation_pipeline": {
            "dbt_run_transform": {
                "description": "Runs dbt models for core, mart, and analytic layers. "
                               "DuckDB reads staging from memory, writes to PostgreSQL via ATTACH.",
                "displayName": "dbt Transform (core/mart/analytic)",
            },
            "gx_validate_core": {
                "description": "Great Expectations validation on core tables "
                               "(fct_weather_observation, dim_city).",
                "displayName": "GX Validate Core",
            },
            "gx_validate_mart": {
                "description": "Great Expectations validation on mart tables "
                               "(weather_daily_summary, city_weather_metrics).",
                "displayName": "GX Validate Mart",
            },
            "soda_scan_core": {
                "description": "Soda Core scan on core schema tables for data quality checks.",
                "displayName": "Soda Scan Core",
            },
            "soda_scan_mart": {
                "description": "Soda Core scan on mart schema tables for business rule validation.",
                "displayName": "Soda Scan Mart",
            },
            "dbt_generate_docs": {
                "description": "Generates dbt documentation (manifest + catalog) "
                               "and uploads to S3 under _reports/dbt_docs/.",
                "displayName": "Generate dbt Docs",
            },
            "finalize_transformation": {
                "description": "Collects all transformation results, uploads reports to S3, "
                               "emits the transformation_complete Asset.",
                "displayName": "Finalize Transformation",
            },
        },
        "openmetadata_ingestion": {
            "register_postgres_service": {
                "description": "Registers the PostgreSQL datawarehouse service in OpenMetadata "
                               "with connection details and schema filters.",
                "displayName": "Register PostgreSQL Service",
            },
            "register_s3_service": {
                "description": "Registers MinIO S3 as a storage service in OpenMetadata "
                               "for raw data lake browsing.",
                "displayName": "Register S3/MinIO Service",
            },
            "register_dbt_pipeline": {
                "description": "Configures the dbt ingestion pipeline in OpenMetadata "
                               "to read manifest/catalog from S3 for lineage.",
                "displayName": "Register dbt Pipeline",
            },
            "trigger_metadata_ingestion": {
                "description": "Triggers the metadata ingestion agent to scan PostgreSQL "
                               "schemas and populate the data catalog.",
                "displayName": "Trigger Metadata Ingestion",
            },
            "configure_openlineage_pipeline": {
                "description": "Configures OpenLineage event pipeline for real-time "
                               "lineage capture from Airflow task execution.",
                "displayName": "Configure OpenLineage",
            },
            "register_quality_metrics": {
                "description": "Registers data quality metrics and test results "
                               "from GX/Soda into OpenMetadata.",
                "displayName": "Register Quality Metrics",
            },
            "configure_tags_and_glossary": {
                "description": "Creates classification tags (PII, Tier, Domain) "
                               "and glossary terms for data governance.",
                "displayName": "Configure Tags & Glossary",
            },
        },
        "airflow_monitoring": {
            "get_api_token": {
                "description": "Authenticates to the Airflow REST API and returns "
                               "a JWT token for downstream monitoring tasks.",
                "displayName": "Get API Token",
            },
            "check_critical_dags_paused": {
                "description": "Checks if any critical DAGs (ingestion, quality, transformation) "
                               "are unexpectedly paused and raises alerts.",
                "displayName": "Check Paused DAGs",
            },
            "check_stuck_runs": {
                "description": "Detects DAG runs stuck in 'running' state beyond "
                               "expected duration thresholds.",
                "displayName": "Check Stuck Runs",
            },
            "check_missed_executions": {
                "description": "Verifies that scheduled DAGs have not missed their "
                               "expected execution windows.",
                "displayName": "Check Missed Executions",
            },
            "check_duration_anomalies": {
                "description": "Compares recent run durations against historical averages "
                               "to detect performance anomalies.",
                "displayName": "Check Duration Anomalies",
            },
            "send_consolidated_report": {
                "description": "Aggregates all monitoring findings and sends a consolidated "
                               "report via Slack and email notifications.",
                "displayName": "Send Monitoring Report",
            },
        },
    }

    PIPELINE_OWNERS: dict[str, str] = {
        "ingestion_pipeline": "Data Engineering",
        "quality_gate_pipeline": "Data Engineering",
        "transformation_pipeline": "Data Engineering",
        "openmetadata_ingestion": "Data Governance",
        "airflow_monitoring": "Platform Engineering",
    }

    def sync_airflow_pipelines(self) -> None:
        logger.info("=" * 60)
        logger.info("5. SYNC AIRFLOW DAGS (tasks + descriptions + executions)")
        logger.info("=" * 60)

        af_token = self._get_airflow_token()
        if not af_token:
            logger.warning("  Cannot authenticate to Airflow — skipping sync")
            return

        af_h = {"Authorization": f"Bearer {af_token}"}
        r = requests.get(f"{AIRFLOW_API_URL}/api/v2/dags?limit=50", headers=af_h, timeout=15)
        if r.status_code != 200:
            logger.warning("  Cannot list Airflow DAGs (%s): %s", r.status_code, r.text[:200])
            return

        for dag in r.json().get("dags", []):
            dag_id = dag["dag_id"]
            task_meta = self.TASK_DESCRIPTIONS.get(dag_id, {})

            tr = requests.get(
                f"{AIRFLOW_API_URL}/api/v2/dags/{dag_id}/tasks",
                headers=af_h, timeout=15,
            )
            tasks = []
            if tr.status_code == 200:
                for t in tr.json().get("tasks", []):
                    tid = t["task_id"]
                    meta = task_meta.get(tid, {})
                    tasks.append({
                        "name": tid,
                        "displayName": meta.get("displayName", tid),
                        "description": meta.get("description", ""),
                        "taskType": t.get("operator_name", "PythonOperator"),
                        "downstreamTasks": t.get("downstream_task_ids", []),
                        "sourceUrl": f"{AIRFLOW_API_URL}/dags/{dag_id}",
                    })

            schedule = dag.get("timetable_summary") or dag.get("schedule_interval") or ""
            source_url = f"{AIRFLOW_API_URL}/dags/{dag_id}"
            owner_team = self.PIPELINE_OWNERS.get(dag_id, "Data Engineering")

            pipeline_payload = {
                "name": dag_id,
                "displayName": dag.get("dag_display_name") or dag_id,
                "description": dag.get("description") or f"Airflow DAG: {dag_id}",
                "service": "airflow",
                "tasks": tasks,
                "scheduleInterval": str(schedule),
                "sourceUrl": source_url,
            }
            resp = self._put("/pipelines", pipeline_payload)
            if resp is not None and resp.status_code in (200, 201):
                pid = resp.json()["id"]
                logger.info("  %s: %d tasks synced (id=%s)", dag_id, len(tasks), pid[:8])
                self._set_pipeline_owner(pid, owner_team)

                rr = requests.get(
                    f"{AIRFLOW_API_URL}/api/v2/dags/{dag_id}/dagRuns?limit=10&order_by=-start_date",
                    headers=af_h, timeout=15,
                )
                if rr.status_code == 200:
                    self._push_pipeline_statuses(dag_id, pid, rr.json().get("dag_runs", []), af_h)
            elif resp is not None:
                logger.warning("  %s: FAILED (%s): %s", dag_id, resp.status_code, resp.text[:300])

    def _set_pipeline_owner(self, pipeline_id: str, team_name: str) -> None:
        """Set pipeline owner as a team via JSON Patch."""
        if self.dry_run:
            return
        team_resp = self._get(f"/teams/name/{team_name}")
        if team_resp.status_code != 200:
            team_payload = {
                "name": team_name.lower().replace(" ", "_"),
                "displayName": team_name,
                "teamType": "Group",
            }
            team_resp = self._put("/teams", team_payload)
            if team_resp is None or team_resp.status_code not in (200, 201):
                return
        team_id = team_resp.json()["id"]
        patches = [{"op": "add", "path": "/owners/0",
                     "value": {"id": team_id, "type": "team"}}]
        self._patch(f"/pipelines/{pipeline_id}", patches)

    def _push_pipeline_statuses(self, dag_id: str, pipeline_id: str,
                               dag_runs: list, af_h: dict) -> None:
        """Push execution statuses from Airflow DAG runs into OM."""
        state_map = {
            "success": "Successful",
            "failed": "Failed",
            "running": "Pending",
            "queued": "Pending",
        }
        synced = 0
        for run in dag_runs:
            state = run.get("state", "")
            om_state = state_map.get(state)
            if not om_state:
                continue
            start = run.get("start_date") or run.get("logical_date")
            if not start:
                continue

            task_statuses = []
            run_id = run.get("dag_run_id")
            if run_id:
                tir = requests.get(
                    f"{AIRFLOW_API_URL}/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances?limit=50",
                    headers=af_h, timeout=15,
                )
                if tir.status_code == 200:
                    for ti in tir.json().get("task_instances", []):
                        ti_state = ti.get("state", "")
                        ti_om = state_map.get(ti_state, "Pending")
                        ti_start = ti.get("start_date")
                        ti_end = ti.get("end_date")
                        entry = {"name": ti.get("task_id", ""), "executionStatus": ti_om}
                        if ti_start:
                            entry["startTime"] = self._iso_to_epoch_ms(ti_start)
                        if ti_end:
                            entry["endTime"] = self._iso_to_epoch_ms(ti_end)
                        task_statuses.append(entry)

            status_payload = {
                "executionStatus": om_state,
                "timestamp": self._iso_to_epoch_ms(start),
                "taskStatus": task_statuses,
            }
            fqn = f"airflow.{dag_id}"
            resp = self._put(f"/pipelines/{fqn}/pipelineStatus", status_payload)
            if resp is not None and resp.status_code in (200, 201):
                synced += 1
            elif synced == 0 and resp is not None and resp.status_code == 404:
                logger.info("    pipelineStatus endpoint not available (OM limitation)")
                return

        if synced > 0:
            logger.info("    %d executions synced for %s", synced, dag_id)

    def _get_airflow_token(self) -> str | None:
        try:
            r = requests.post(
                f"{AIRFLOW_API_URL}/auth/token",
                json={"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD},
                timeout=10,
            )
            if r.status_code in (200, 201):
                return r.json().get("access_token")
            logger.warning("  Airflow auth failed (%s): %s", r.status_code, r.text[:200])
        except requests.ConnectionError:
            logger.warning("  Cannot connect to Airflow at %s", AIRFLOW_API_URL)
        return None

    @staticmethod
    def _iso_to_epoch_ms(iso_str: str) -> int:
        from datetime import datetime
        iso_str = iso_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso_str)
        return int(dt.timestamp() * 1000)

    # ---------------------------------------------------------------
    # 6. Add data contracts on tables
    # ---------------------------------------------------------------
    TABLE_CONTRACTS: dict[str, dict[str, Any]] = {
        "core.fct_weather_observation": {
            "description": (
                "**Data Contract — Weather Observation Fact Table**\n\n"
                "- **Owner**: Data Engineering Team\n"
                "- **SLA**: Data must be available within 30 minutes of API extraction\n"
                "- **Freshness**: Maximum 6 hours lag (aligned with ingestion schedule)\n"
                "- **Schema**: 15 columns, no schema changes without approval\n"
                "- **Quality**: row_id unique & not null, temperature in [-60, 60]°C, "
                "humidity in [0, 100]%, country_code = 'FR'\n"
                "- **Retention**: Unlimited (append-only fact table)\n"
                "- **Consumers**: mart.weather_daily_summary, mart.city_weather_metrics"
            ),
        },
        "core.dim_city": {
            "description": (
                "**Data Contract — City Dimension**\n\n"
                "- **Owner**: Data Engineering Team\n"
                "- **SLA**: Updated after each ingestion pipeline run\n"
                "- **Schema**: 4 columns (city_name, country_code, latitude, longitude)\n"
                "- **Quality**: city_name unique & not null, coordinates within France bounds\n"
                "- **Consumers**: mart.city_weather_metrics"
            ),
        },
        "mart.weather_daily_summary": {
            "description": (
                "**Data Contract — Daily Weather Summary**\n\n"
                "- **Owner**: Data Analytics Team\n"
                "- **SLA**: Available within 1 hour after transformation pipeline completes\n"
                "- **Freshness**: Updated daily, one row per city per day\n"
                "- **Quality**: observation_count >= 1, temperatures in valid range, "
                "no null city_name or date_day\n"
                "- **Consumers**: analytic.weather_trend_analysis, BI dashboards"
            ),
        },
        "mart.city_weather_metrics": {
            "description": (
                "**Data Contract — City Weather Metrics**\n\n"
                "- **Owner**: Data Analytics Team\n"
                "- **SLA**: Available within 1 hour after transformation pipeline completes\n"
                "- **Schema**: One row per city (unique city_name)\n"
                "- **Quality**: total_observations >= 1, all metrics non-null\n"
                "- **Consumers**: analytic.city_comparison_ranking, BI dashboards, ML features"
            ),
        },
        "analytic.weather_trend_analysis": {
            "description": (
                "**Data Contract — Weather Trend Analysis**\n\n"
                "- **Owner**: Data Science Team\n"
                "- **SLA**: Available within 2 hours after transformation\n"
                "- **Quality**: 7-day moving averages in valid ranges\n"
                "- **Consumers**: Grafana dashboards, anomaly detection models"
            ),
        },
        "analytic.city_comparison_ranking": {
            "description": (
                "**Data Contract — City Comparison & Ranking**\n\n"
                "- **Owner**: Data Science Team\n"
                "- **SLA**: Available within 2 hours after transformation\n"
                "- **Quality**: One row per city (unique), ranking metrics non-null\n"
                "- **Consumers**: City comparison BI reports, executive dashboards"
            ),
        },
        "staging.stg_weather_current": {
            "description": (
                "**Data Contract — Staging Current Weather**\n\n"
                "- **Owner**: Data Engineering Team\n"
                "- **SLA**: Populated within 15 minutes of ingestion start\n"
                "- **Quality**: Temperature and city_name not null, schema validated\n"
                "- **Note**: Intermediate table — do not build external dependencies on it"
            ),
        },
        "staging.stg_weather_forecast": {
            "description": (
                "**Data Contract — Staging Weather Forecast**\n\n"
                "- **Owner**: Data Engineering Team\n"
                "- **SLA**: Populated within 15 minutes of ingestion start\n"
                "- **Quality**: City_name and temperature not null\n"
                "- **Note**: Intermediate table — do not build external dependencies on it"
            ),
        },
    }

    def add_table_contracts(self) -> None:
        """Apply data contracts as table descriptions (Contract section in markdown)."""
        logger.info("=" * 60)
        logger.info("6. ADD DATA CONTRACTS ON TABLES")
        logger.info("=" * 60)

        for schema_table, contract in self.TABLE_CONTRACTS.items():
            schema, table = schema_table.split(".", 1)
            fqn = f"{FQN_PREFIX}.{schema}.{table}"
            resp = self._get(f"/tables/name/{fqn}")
            if resp.status_code != 200:
                logger.warning("  Table %s not found", fqn)
                continue
            table_data = resp.json()
            table_id = table_data["id"]
            current_desc = table_data.get("description", "")

            contract_text = contract["description"]
            if "Data Contract" in (current_desc or ""):
                logger.info("  %s: contract already in description", schema_table)
                continue

            new_desc = f"{current_desc}\n\n---\n\n{contract_text}" if current_desc else contract_text
            patches = [{"op": "add", "path": "/description", "value": new_desc}]
            resp2 = self._patch(f"/tables/{table_id}", patches)
            if resp2 is not None and resp2.status_code in (200, 201):
                logger.info("  %s: contract added to description", schema_table)
            elif resp2 is not None:
                logger.warning("  %s: failed (%s): %s",
                               schema_table, resp2.status_code, resp2.text[:200])

    # ---------------------------------------------------------------
    # 7. Add column descriptions for data contract enrichment
    # ---------------------------------------------------------------
    def add_column_descriptions(self) -> None:
        logger.info("=" * 60)
        logger.info("7. ADD COLUMN DESCRIPTIONS (Data Contract Enrichment)")
        logger.info("=" * 60)

        col_descriptions = {
            "core.fct_weather_observation": {
                "row_id": "Unique identifier for each observation (SHA256 hash of city+timestamp).",
                "city_name": "Name of the city where the observation was recorded.",
                "country_code": "ISO 3166-1 alpha-2 country code (always FR for French cities).",
                "measured_at": "UTC timestamp when the weather observation was recorded by the station.",
                "temperature_celsius": "Air temperature in degrees Celsius at observation time.",
                "feels_like_celsius": "Perceived temperature accounting for wind chill and humidity.",
                "humidity_percent": "Relative humidity as percentage (0-100).",
                "pressure_hpa": "Atmospheric pressure at sea level in hectopascals.",
                "wind_speed_ms": "Wind speed in meters per second.",
                "cloud_coverage_percent": "Sky cloud coverage as percentage (0=clear, 100=overcast).",
                "visibility_meters": "Visibility distance in meters.",
                "weather_condition": "Primary weather condition group (Clear, Rain, Clouds, Snow, etc.).",
                "weather_description": "Detailed weather description (e.g. 'light rain', 'scattered clouds').",
                "load_id": "DLT load identifier linking to the ingestion batch.",
                "loaded_at": "Timestamp when the record was loaded by DLT.",
            },
            "core.dim_city": {
                "city_name": "Unique city name serving as natural key for the dimension.",
                "country_code": "ISO 3166-1 alpha-2 country code.",
                "latitude": "City center latitude in decimal degrees (WGS84).",
                "longitude": "City center longitude in decimal degrees (WGS84).",
            },
            "mart.weather_daily_summary": {
                "city_name": "City name for the daily aggregation.",
                "date_day": "Calendar date of the daily summary (truncated to day).",
                "avg_temperature_celsius": "Average temperature across all observations for the day.",
                "min_temperature_celsius": "Minimum temperature observed during the day.",
                "max_temperature_celsius": "Maximum temperature observed during the day.",
                "avg_humidity_percent": "Average relative humidity for the day.",
                "avg_pressure_hpa": "Average atmospheric pressure for the day.",
                "avg_wind_speed_ms": "Average wind speed for the day.",
                "dominant_weather_condition": "Most frequent weather condition observed during the day.",
                "observation_count": "Number of individual observations aggregated for this day.",
            },
            "mart.city_weather_metrics": {
                "city_name": "City name (unique per row).",
                "country_code": "ISO country code.",
                "latitude": "City latitude.",
                "longitude": "City longitude.",
                "avg_temperature_celsius": "Overall average temperature across all observations.",
                "min_temperature_celsius": "Absolute minimum temperature ever recorded.",
                "max_temperature_celsius": "Absolute maximum temperature ever recorded.",
                "avg_humidity_percent": "Overall average humidity.",
                "avg_pressure_hpa": "Overall average pressure.",
                "avg_wind_speed_ms": "Overall average wind speed.",
                "dominant_weather_condition": "Most common weather condition across all history.",
                "total_observations": "Total number of observations recorded for this city.",
                "last_observation_at": "Timestamp of the most recent observation.",
            },
        }

        for schema_table, columns in col_descriptions.items():
            schema, table = schema_table.split(".", 1)
            fqn = f"{FQN_PREFIX}.{schema}.{table}"

            resp = self._get(f"/tables/name/{fqn}", params={"fields": "columns"})
            if resp.status_code != 200:
                logger.warning("  Table %s not found", fqn)
                continue

            table_data = resp.json()
            table_id = table_data["id"]
            existing_cols = {c["name"]: i for i, c in enumerate(table_data.get("columns", []))}

            patches = []
            for col_name, desc in columns.items():
                if col_name in existing_cols:
                    idx = existing_cols[col_name]
                    patches.append({
                        "op": "add",
                        "path": f"/columns/{idx}/description",
                        "value": desc,
                    })

            if patches:
                resp2 = self._patch(f"/tables/{table_id}", patches)
                if resp2 is not None and resp2.status_code in (200, 201):
                    logger.info("  %s: %d column descriptions applied", schema_table, len(patches))
                elif resp2 is not None:
                    logger.warning("  %s: patch failed (%s): %s", schema_table, resp2.status_code, resp2.text[:300])
            else:
                logger.info("  %s: no columns to update", schema_table)

    # ---------------------------------------------------------------
    # Full setup
    # ---------------------------------------------------------------
    # ---------------------------------------------------------------
    # 9. Cleanup orphan test suites + run tests
    # ---------------------------------------------------------------
    def cleanup_and_run_tests(self) -> None:
        logger.info("=" * 60)
        logger.info("9. CLEANUP ORPHAN TEST SUITES + RUN TESTS")
        logger.info("=" * 60)

        r = self._get("/dataQuality/testSuites?limit=100&fields=tests")
        if r.status_code != 200:
            logger.warning("  Cannot list test suites")
            return

        suites = r.json().get("data", [])
        orphans = [s for s in suites if not s.get("tests")]
        valid = [s for s in suites if s.get("tests")]

        if orphans:
            logger.info("  Deleting %d orphan test suites (no tests attached)...", len(orphans))
            for s in orphans:
                sid = s["id"]
                sname = s.get("name", "?")
                if not self.dry_run:
                    dr = requests.delete(
                        f"{self.api}/dataQuality/testSuites/{sid}?hardDelete=true&recursive=true",
                        headers=self.headers, timeout=30,
                    )
                    if dr.status_code in (200, 204):
                        logger.info("    Deleted orphan: %s", sname)
                    else:
                        logger.warning("    Failed to delete %s (%s)", sname, dr.status_code)

        logger.info("  %d valid test suites remain", len(valid))

        self._trigger_test_suites()

    def _trigger_test_suites(self) -> None:
        """Attempt to trigger test execution via existing TestSuite ingestion pipelines."""
        r = self._get("/services/ingestionPipelines?limit=50")
        if r.status_code != 200:
            return
        triggered = False
        for p in r.json().get("data", []):
            ptype = p.get("pipelineType", "")
            if ptype == "TestSuite":
                pid = p["id"]
                pname = p.get("name", "?")
                logger.info("  Triggering TestSuite pipeline: %s", pname)
                tr = self._post(f"/services/ingestionPipelines/trigger/{pid}", {})
                if tr is not None and tr.status_code in (200, 201):
                    logger.info("    Triggered successfully")
                    triggered = True
                elif tr is not None:
                    logger.warning("    Failed (%s) — deleting broken pipeline", tr.status_code)
                    requests.delete(
                        f"{self.api}/services/ingestionPipelines/{pid}?hardDelete=true",
                        headers=self.headers, timeout=30,
                    )

        if not triggered:
            logger.info("  To run tests:")
            logger.info("    Option 1: OM UI → table → Data Observability → Data Quality → Run All")
            logger.info("    Option 2: Add TestSuite agent via OM UI → Settings → Services → datawarehouse → Ingestion")

    # ---------------------------------------------------------------
    # Full setup
    # ---------------------------------------------------------------
    def run(self) -> None:
        self.authenticate()

        pg_id = self.register_database_service()
        s3_id = self.register_storage_service()
        self.create_and_deploy_agents(pg_id, s3_id)

        logger.info("\n  Waiting 10s for agents to start indexing metadata...")
        time.sleep(10)

        self.register_airflow_service()
        self.update_profiler_agent()
        self.create_test_suites()
        self.create_lineage()
        self.sync_airflow_pipelines()
        self.add_table_contracts()
        self.add_column_descriptions()
        self.cleanup_and_run_tests()
        self.trigger_agents()
        logger.info("\n" + "=" * 60)
        logger.info("SETUP COMPLETE")
        logger.info("=" * 60)
        logger.info("Next steps:")
        logger.info("  1. Wait for agents to complete (~5-10 min)")
        logger.info("  2. Re-run 'just om-provision' to complete lineage after agents finish")
        logger.info("  3. Check lineage: OM UI → table → Lineage tab")
        logger.info("  4. Check tests: OM UI → table → Data Observability → Data Quality")
        logger.info("  5. Check contracts: OM UI → table → Contract tab")


def main() -> None:
    parser = argparse.ArgumentParser(description="Full OpenMetadata setup")
    parser.add_argument("--url", default=OM_URL)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-trigger", action="store_true")
    args = parser.parse_args()

    client = OMSetup(args.url, dry_run=args.dry_run, skip_trigger=args.skip_trigger)
    client.run()


if __name__ == "__main__":
    main()
