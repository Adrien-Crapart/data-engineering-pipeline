"""Push GX, Soda, and dbt test results from S3 into OpenMetadata test case results.

Reads JSON reports from MinIO S3 (_reports/*/latest/*.json), parses individual
test outcomes, maps them to existing OM test cases, and pushes results via
POST /api/v1/dataQuality/testCases/{fqn}/testCaseResult.

This populates the Data Observability > Data Quality tab on each table in the UI.

Usage:
    python metadata/om_push_test_results.py [--url http://localhost:8585] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

OM_URL = "http://localhost:8585"
EMAIL = "admin@open-metadata.org"
PASSWORD = "YWRtaW4="

FQN_PREFIX = "datawarehouse.datawarehouse"

S3_REPORT_PATHS = {
    "gx_staging": "_reports/great_expectations/latest/gx_staging.json",
    "gx_core": "_reports/great_expectations/latest/gx_core.json",
    "gx_mart": "_reports/great_expectations/latest/gx_mart.json",
    "gx_all": "_reports/great_expectations/latest/gx_all.json",
    "soda_staging": "_reports/soda/latest/soda_staging.json",
    "soda_core": "_reports/soda/latest/soda_core.json",
    "soda_mart": "_reports/soda/latest/soda_mart.json",
    "soda_all": "_reports/soda/latest/soda_all.json",
    "dbt_run_results": "_reports/dbt_docs/latest/run_results.json",
}

GX_TEST_TYPE_MAP = {
    "expect_table_row_count_to_be_between": "tableRowCountToBeBetween",
    "expect_column_to_exist": "columnToExist",
    "expect_column_values_to_not_be_null": "columnValuesToBeNotNull",
    "expect_column_values_to_be_between": "columnValuesToBeBetween",
    "expect_column_values_to_be_unique": "columnValuesToBeUnique",
    "expect_column_values_to_be_in_set": "columnValuesToBeInSet",
}


class OMTestResultPusher:
    def __init__(self, base_url: str, dry_run: bool = False):
        self.base_url = base_url.rstrip("/")
        self.api = f"{self.base_url}/api/v1"
        self.token: str | None = None
        self.dry_run = dry_run
        self._test_case_cache: dict[str, str] = {}
        self.stats = {"pushed": 0, "skipped": 0, "failed": 0}

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
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _get_s3_client(self) -> Any:
        import os

        import boto3
        from botocore.client import Config as BotoConfig

        endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        return boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
            config=BotoConfig(signature_version="s3v4"),
            region_name="us-east-1",
        )

    def _read_s3_json(self, s3_client: Any, key: str) -> dict | None:
        import os

        bucket = os.getenv("MINIO_BUCKET_NAME", "data-lake")
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            return json.loads(obj["Body"].read().decode())
        except Exception:
            logger.debug("S3 key not found: %s", key)
            return None

    def _find_test_case_fqn(self, table_fqn: str, test_type: str, column: str | None = None) -> str | None:
        table_name = table_fqn.split(".")[-1]
        if column:
            case_name = f"{table_name}_{column}_{test_type}"
        else:
            case_name = f"{table_name}_{test_type}"
        case_name = case_name[:128]
        fqn = f"{table_fqn}.testSuite.{case_name}"

        if fqn in self._test_case_cache:
            return fqn

        resp = requests.get(
            f"{self.api}/dataQuality/testCases/name/{fqn}",
            headers=self.headers,
            timeout=15,
        )
        if resp.status_code == 200:
            self._test_case_cache[fqn] = resp.json()["id"]
            return fqn

        return None

    def _push_result(
        self,
        test_case_fqn: str,
        status: str,
        result_msg: str,
        timestamp: int | None = None,
        test_result_values: list[dict] | None = None,
    ) -> bool:
        if self.dry_run:
            logger.info("[DRY-RUN] Would push %s → %s", test_case_fqn, status)
            self.stats["pushed"] += 1
            return True

        ts = timestamp or int(time.time() * 1000)
        payload: dict[str, Any] = {
            "timestamp": ts,
            "testCaseStatus": status,
            "result": result_msg,
        }
        if test_result_values:
            payload["testResultValue"] = test_result_values

        resp = requests.put(
            f"{self.api}/dataQuality/testCases/{test_case_fqn}/testCaseResult",
            headers=self.headers,
            json=payload,
            timeout=15,
        )
        if resp.status_code in (200, 201):
            self.stats["pushed"] += 1
            return True

        logger.warning(
            "  Failed to push result for %s (%s): %s",
            test_case_fqn,
            resp.status_code,
            resp.text[:200],
        )
        self.stats["failed"] += 1
        return False

    def process_gx_report(self, report: dict, report_name: str) -> None:
        logger.info("  Processing GX report: %s", report_name)
        ts = int(time.time() * 1000)

        for layer, tables in report.items():
            if not isinstance(tables, dict):
                continue
            for table_name, result in tables.items():
                schema = layer
                table_fqn = f"{FQN_PREFIX}.{schema}.{table_name}"
                success = result.get("success", False)
                status = "Success" if success else "Failed"
                details = result.get("details", "")

                table_test_fqn = self._find_test_case_fqn(
                    table_fqn, "tableRowCountToBeBetween"
                )
                if table_test_fqn:
                    self._push_result(
                        table_test_fqn,
                        status,
                        f"GX validation {status.lower()}: {details[:200]}",
                        timestamp=ts,
                    )
                else:
                    logger.debug(
                        "    No matching OM test case for %s.%s", schema, table_name
                    )

    def process_soda_report(self, report: dict, report_name: str) -> None:
        logger.info("  Processing Soda report: %s", report_name)
        ts = int(time.time() * 1000)

        results_list = report.get("results", [])
        for scan_result in results_list:
            layer = scan_result.get("layer", "unknown")
            success = scan_result.get("success", False)
            status = "Success" if success else "Failed"
            stdout = scan_result.get("stdout", "")[:500]

            table_map = {
                "staging": [
                    "stg_weather_current",
                    "stg_weather_forecast",
                ],
                "core": [
                    "fct_weather_observation",
                    "dim_city",
                ],
                "mart": [
                    "weather_daily_summary",
                    "city_weather_metrics",
                ],
            }

            for table_name in table_map.get(layer, []):
                table_fqn = f"{FQN_PREFIX}.{layer}.{table_name}"
                test_fqn = self._find_test_case_fqn(
                    table_fqn, "tableRowCountToBeBetween"
                )
                if test_fqn:
                    self._push_result(
                        test_fqn,
                        status,
                        f"Soda scan {layer} {status.lower()}: {stdout[:200]}",
                        timestamp=ts,
                    )

    def process_dbt_run_results(self, report: dict) -> None:
        logger.info("  Processing dbt run_results.json")
        ts = int(time.time() * 1000)

        for result in report.get("results", []):
            unique_id = result.get("unique_id", "")
            if not unique_id.startswith("test."):
                continue

            dbt_status = result.get("status", "")
            if dbt_status == "pass":
                status = "Success"
            elif dbt_status == "fail":
                status = "Failed"
            else:
                status = "Aborted"

            message = result.get("message", "")
            node_info = result.get("node", {}) or {}
            test_name = node_info.get("test_metadata", {}).get("name", "")
            column_name = node_info.get("column_name")

            refs = node_info.get("refs", [])
            if not refs:
                depends = node_info.get("depends_on", {}).get("nodes", [])
                for dep in depends:
                    if dep.startswith("model."):
                        parts = dep.split(".")
                        if len(parts) >= 3:
                            refs.append({"name": parts[-1]})

            for ref in refs:
                ref_name = ref.get("name", "") if isinstance(ref, dict) else str(ref)
                if not ref_name:
                    continue

                schema = self._guess_schema(ref_name)
                if not schema:
                    continue

                table_fqn = f"{FQN_PREFIX}.{schema}.{ref_name}"
                om_test_type = self._dbt_test_to_om_type(test_name)
                if om_test_type:
                    test_fqn = self._find_test_case_fqn(
                        table_fqn, om_test_type, column_name
                    )
                    if test_fqn:
                        self._push_result(
                            test_fqn,
                            status,
                            f"dbt test '{test_name}': {message[:200]}",
                            timestamp=ts,
                        )

    @staticmethod
    def _guess_schema(table_name: str) -> str | None:
        if table_name.startswith("stg_"):
            return "staging"
        if table_name.startswith("quarantine_"):
            return "staging_quarantine"
        if table_name.startswith("fct_") or table_name.startswith("dim_"):
            return "core"
        if table_name in ("weather_daily_summary", "city_weather_metrics"):
            return "mart"
        if table_name in ("weather_trend_analysis", "city_comparison_ranking"):
            return "analytic"
        return None

    @staticmethod
    def _dbt_test_to_om_type(dbt_test_name: str) -> str | None:
        mapping = {
            "not_null": "columnValuesToBeNotNull",
            "unique": "columnValuesToBeUnique",
            "accepted_values": "columnValuesToBeInSet",
            "relationships": "columnValuesToBeNotNull",
        }
        return mapping.get(dbt_test_name)

    def run(self) -> None:
        self.authenticate()

        try:
            s3 = self._get_s3_client()
        except Exception as e:
            logger.error("Cannot connect to S3: %s", e)
            sys.exit(1)

        logger.info("=" * 60)
        logger.info("PUSHING TEST RESULTS TO OPENMETADATA")
        logger.info("=" * 60)

        for report_name, s3_key in S3_REPORT_PATHS.items():
            data = self._read_s3_json(s3, s3_key)
            if not data:
                continue

            if report_name.startswith("gx_"):
                self.process_gx_report(data, report_name)
            elif report_name.startswith("soda_"):
                self.process_soda_report(data, report_name)
            elif report_name == "dbt_run_results":
                self.process_dbt_run_results(data)

        logger.info("")
        logger.info("=" * 60)
        logger.info(
            "RESULTS: %d pushed, %d skipped, %d failed",
            self.stats["pushed"],
            self.stats["skipped"],
            self.stats["failed"],
        )
        logger.info("=" * 60)

        if self.stats["failed"] > 0:
            sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Push GX/Soda/dbt test results to OpenMetadata"
    )
    parser.add_argument("--url", default=OM_URL)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    pusher = OMTestResultPusher(args.url, dry_run=args.dry_run)
    pusher.run()


if __name__ == "__main__":
    main()
