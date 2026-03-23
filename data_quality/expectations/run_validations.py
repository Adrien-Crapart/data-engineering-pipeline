#!/usr/bin/env python3
"""Run Great Expectations validations and upload results to MinIO S3.

Validates staging and mart tables against expectation suites derived from
data contracts. Produces JSON results and uploads them to S3.

Usage:
    python -m data_quality.expectations.run_validations [--layer staging|mart|all]
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sys
import warnings
from datetime import datetime, timezone

from minio import Minio

warnings.filterwarnings("ignore", message="Did not recognize type")
os.environ.setdefault("TQDM_DISABLE", "1")
logging.getLogger("great_expectations").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


def _get_minio() -> Minio | None:
    try:
        endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "")
        return Minio(
            endpoint=endpoint,
            access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
            secure=False,
        )
    except Exception:
        logger.warning("MinIO unavailable — report upload disabled")
        return None


def _upload_report(client: Minio, report: dict, name: str) -> None:
    bucket = os.getenv("MINIO_BUCKET_NAME", "weather-data-lake")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    key = f"_reports/great_expectations/{name}_{ts}.json"

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    payload = json.dumps(report, indent=2, default=str).encode()
    client.put_object(
        bucket_name=bucket,
        object_name=key,
        data=io.BytesIO(payload),
        length=len(payload),
        content_type="application/json",
    )
    logger.info("Uploaded GX report: s3://%s/%s", bucket, key)


def _pg_connection_string(schema: str = "public") -> str:
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "weather_db")
    user = os.getenv("POSTGRES_USER", "airflow")
    password = os.getenv("POSTGRES_PASSWORD", "airflow")
    return (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
        f"?options=-csearch_path%3D{schema}"
    )


def run_staging_validations() -> dict:
    import great_expectations as gx
    from great_expectations.core.expectation_suite import ExpectationSuite
    from great_expectations.core.validation_definition import ValidationDefinition

    context = gx.get_context()
    ds = context.data_sources.add_postgres(
        "staging_db", connection_string=_pg_connection_string("staging"),
    )

    results = {}
    tables = {
        "stg_weather_current": [
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
            gx.expectations.ExpectColumnToExist(column="city_name"),
            gx.expectations.ExpectColumnToExist(column="temperature_celsius"),
            gx.expectations.ExpectColumnToExist(column="humidity_percent"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="city_name"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="temperature_celsius"),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="temperature_celsius", min_value=-60.0, max_value=60.0,
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="humidity_percent", min_value=0.0, max_value=100.0,
            ),
        ],
        "stg_weather_forecast": [
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
            gx.expectations.ExpectColumnToExist(column="city_name"),
            gx.expectations.ExpectColumnToExist(column="temperature_celsius"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="city_name"),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="temperature_celsius", min_value=-60.0, max_value=60.0,
            ),
        ],
    }

    for table, expectations in tables.items():
        suite = context.suites.add(ExpectationSuite(name=f"gx_{table}"))
        for exp in expectations:
            suite.add_expectation(exp)

        asset = ds.add_table_asset(name=table, table_name=table)
        batch_def = asset.add_batch_definition_whole_table(f"{table}_batch")

        vd = context.validation_definitions.add(
            ValidationDefinition(name=f"validate_{table}", data=batch_def, suite=suite)
        )
        cp = context.checkpoints.add(
            gx.checkpoint.checkpoint.Checkpoint(
                name=f"cp_{table}", validation_definitions=[vd],
            )
        )
        result = cp.run()
        results[table] = {"success": result.success, "details": str(result.describe())}
        logger.info("GX %s: %s", table, "PASS" if result.success else "FAIL")

    return results


def run_mart_validations() -> dict:
    import great_expectations as gx
    from great_expectations.core.expectation_suite import ExpectationSuite
    from great_expectations.core.validation_definition import ValidationDefinition

    context = gx.get_context()
    ds = context.data_sources.add_postgres(
        "mart_db", connection_string=_pg_connection_string("mart"),
    )

    results = {}
    tables = {
        "weather_daily_summary": [
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
            gx.expectations.ExpectColumnToExist(column="city_name"),
            gx.expectations.ExpectColumnToExist(column="date_day"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="city_name"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="date_day"),
        ],
        "city_weather_metrics": [
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
            gx.expectations.ExpectColumnToExist(column="city_name"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="city_name"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="total_observations"),
        ],
    }

    for table, expectations in tables.items():
        suite = context.suites.add(ExpectationSuite(name=f"gx_mart_{table}"))
        for exp in expectations:
            suite.add_expectation(exp)

        asset = ds.add_table_asset(name=table, table_name=table)
        batch_def = asset.add_batch_definition_whole_table(f"{table}_batch")

        vd = context.validation_definitions.add(
            ValidationDefinition(name=f"validate_mart_{table}", data=batch_def, suite=suite)
        )
        cp = context.checkpoints.add(
            gx.checkpoint.checkpoint.Checkpoint(
                name=f"cp_mart_{table}", validation_definitions=[vd],
            )
        )
        result = cp.run()
        results[table] = {"success": result.success, "details": str(result.describe())}
        logger.info("GX mart %s: %s", table, "PASS" if result.success else "FAIL")

    return results


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Run GX validations")
    parser.add_argument("--layer", choices=["staging", "mart", "all"], default="all")
    args = parser.parse_args()

    minio = _get_minio()
    all_results = {}
    failed = False

    if args.layer in ("staging", "all"):
        logger.info("=== Running staging GX validations ===")
        staging = run_staging_validations()
        all_results["staging"] = staging
        if not all(r["success"] for r in staging.values()):
            failed = True

    if args.layer in ("mart", "all"):
        logger.info("=== Running mart GX validations ===")
        mart = run_mart_validations()
        all_results["mart"] = mart
        if not all(r["success"] for r in mart.values()):
            failed = True

    if minio:
        _upload_report(minio, all_results, f"gx_{args.layer}")

    status = "FAIL" if failed else "PASS"
    logger.info("=== GX validation complete: %s ===", status)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
