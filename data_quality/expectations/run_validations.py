#!/usr/bin/env python3
"""Run Great Expectations validations and upload results to MinIO S3.

Validates staging and mart tables against expectation suites derived from
data contracts. Produces JSON + HTML results with timestamped versioning.

Usage:
    python -m data_quality.expectations.run_validations [--layer staging|mart|all]

Report layout on S3:
    _reports/great_expectations/YYYY/MM/DD/HHMMSS/gx_{layer}.json
    _reports/great_expectations/YYYY/MM/DD/HHMMSS/gx_{layer}.html
    _reports/great_expectations/latest/gx_{layer}.json
    _reports/great_expectations/latest/gx_{layer}.html
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

REPORT_PREFIX = "_reports/great_expectations"


def _get_minio() -> Minio | None:
    try:
        endpoint = (
            os.getenv("MINIO_ENDPOINT", "minio:9000")
            .replace("http://", "")
            .replace("https://", "")
        )
        return Minio(
            endpoint=endpoint,
            access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
            secure=False,
        )
    except Exception:
        logger.warning("MinIO unavailable — report upload disabled")
        return None


def _upload(client: Minio, key: str, data: bytes, content_type: str) -> None:
    bucket = os.getenv("MINIO_BUCKET_NAME", "weather-data-lake")
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    client.put_object(
        bucket_name=bucket,
        object_name=key,
        data=io.BytesIO(data),
        length=len(data),
        content_type=content_type,
    )
    logger.info("Uploaded: s3://%s/%s (%d bytes)", bucket, key, len(data))


def _upload_reports(client: Minio | None, report: dict, name: str) -> None:
    if not client:
        return

    now = datetime.now(timezone.utc)
    ts_prefix = f"{REPORT_PREFIX}/{now.year}/{now.month:02d}/{now.day:02d}/{now.strftime('%H%M%S')}"
    latest_prefix = f"{REPORT_PREFIX}/latest"

    json_data = json.dumps(report, indent=2, default=str).encode()
    _upload(client, f"{ts_prefix}/{name}.json", json_data, "application/json")
    _upload(client, f"{latest_prefix}/{name}.json", json_data, "application/json")

    html_data = _build_html_report(report, name).encode()
    _upload(client, f"{ts_prefix}/{name}.html", html_data, "text/html")
    _upload(client, f"{latest_prefix}/{name}.html", html_data, "text/html")


def _build_html_report(report: dict, title: str) -> str:
    """Build a simple HTML report from validation results."""
    rows = []
    for layer, tables in report.items():
        for table, result in tables.items():
            status = "PASS" if result["success"] else "FAIL"
            color = "#22c55e" if result["success"] else "#ef4444"
            rows.append(
                f'<tr><td>{layer}</td><td>{table}</td>'
                f'<td style="color:{color};font-weight:bold">{status}</td>'
                f'<td><pre>{result.get("details", "")}</pre></td></tr>'
            )

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"><title>GX Report — {title}</title>
<style>
body {{ font-family: system-ui, sans-serif; margin: 2rem; background: #f8fafc; }}
h1 {{ color: #1e293b; }}
table {{ border-collapse: collapse; width: 100%; background: white; box-shadow: 0 1px 3px rgba(0,0,0,.1); }}
th, td {{ padding: .75rem 1rem; text-align: left; border-bottom: 1px solid #e2e8f0; }}
th {{ background: #1e293b; color: white; }}
pre {{ margin: 0; white-space: pre-wrap; font-size: .85rem; max-width: 60vw; overflow-x: auto; }}
.meta {{ color: #64748b; margin-bottom: 1.5rem; }}
</style>
</head>
<body>
<h1>Great Expectations — {title}</h1>
<p class="meta">Generated: {now}</p>
<table>
<thead><tr><th>Layer</th><th>Table</th><th>Status</th><th>Details</th></tr></thead>
<tbody>{''.join(rows)}</tbody>
</table>
</body></html>"""


def _pg_connection_string(schema: str = "public") -> str:
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "datawarehouse")
    user = os.getenv("POSTGRES_USER", "datawarehouse_user")
    password = os.getenv("POSTGRES_PASSWORD", "datawarehouse_password")
    return (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
        f"?options=-csearch_path%3D{schema}"
    )


def run_staging_validations() -> dict:
    """Validate core tables (dbt-duckdb writes staging views to DuckDB, core/mart to PG)."""
    import great_expectations as gx
    from great_expectations.core.expectation_suite import ExpectationSuite
    from great_expectations.core.validation_definition import ValidationDefinition

    context = gx.get_context()
    ds = context.data_sources.add_postgres(
        "core_db",
        connection_string=_pg_connection_string("core"),
    )

    results = {}
    tables = {
        "fct_weather_observation": [
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
            gx.expectations.ExpectColumnToExist(column="city_name"),
            gx.expectations.ExpectColumnToExist(column="temperature_celsius"),
            gx.expectations.ExpectColumnToExist(column="humidity_percent"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="city_name"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="temperature_celsius"),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="temperature_celsius",
                min_value=-60.0,
                max_value=60.0,
            ),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="humidity_percent",
                min_value=0.0,
                max_value=100.0,
            ),
        ],
        "dim_city": [
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
            gx.expectations.ExpectColumnToExist(column="city_name"),
            gx.expectations.ExpectColumnValuesToNotBeNull(column="city_name"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="city_name"),
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
                name=f"cp_{table}",
                validation_definitions=[vd],
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
        "mart_db",
        connection_string=_pg_connection_string("mart"),
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
            ValidationDefinition(
                name=f"validate_mart_{table}", data=batch_def, suite=suite
            )
        )
        cp = context.checkpoints.add(
            gx.checkpoint.checkpoint.Checkpoint(
                name=f"cp_mart_{table}",
                validation_definitions=[vd],
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
    all_results: dict[str, dict] = {}
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

    _upload_reports(minio, all_results, f"gx_{args.layer}")

    status = "FAIL" if failed else "PASS"
    logger.info("=== GX validation complete: %s ===", status)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
