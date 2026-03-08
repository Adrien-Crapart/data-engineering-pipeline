"""Replay pipeline: reprocess historical raw data from the MinIO data lake.

Use cases:
  - Fix transformation logic and re-run on historical data
  - Update schema contracts and revalidate past data
  - Correct historical errors by reloading raw JSON into the warehouse

Usage:
  python -m replay.reprocess_pipeline --start 2025-06-01 --end 2025-06-15
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from datetime import datetime, timedelta

from minio import Minio

logger = logging.getLogger(__name__)


def _build_minio_client(
    endpoint: str = "minio:9000",
    access_key: str = "minioadmin",
    secret_key: str = "minioadmin",
    secure: bool = False,
) -> Minio:
    return Minio(endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=secure)


def list_raw_files(
    client: Minio,
    bucket: str,
    date_start: datetime,
    date_end: datetime,
    source: str = "openweather",
) -> list[str]:
    """List all raw JSON keys in the date range (inclusive)."""
    keys: list[str] = []
    current = date_start
    while current <= date_end:
        prefix = (
            f"raw/{source}/"
            f"year={current.year}/month={current.month:02d}/day={current.day:02d}/"
        )
        for obj in client.list_objects(bucket, prefix=prefix):
            keys.append(obj.object_name)
        current += timedelta(days=1)
    return keys


def load_raw_to_postgres(
    client: Minio,
    bucket: str,
    keys: list[str],
    postgres_dsn: str,
) -> int:
    """Read raw JSON files from MinIO and load them into PostgreSQL raw schema via dlt."""
    import dlt

    records_current: list[dict] = []
    records_forecast: list[dict] = []

    for key in keys:
        response = client.get_object(bucket, key)
        try:
            data = json.loads(response.read().decode("utf-8"))
        finally:
            response.close()
            response.release_conn()

        if "weather_current" in key:
            records_current.append(data)
        elif "weather_forecast" in key:
            records_forecast.append(data)
        else:
            logger.warning("Unknown file type, skipping: %s", key)

    pipeline = dlt.pipeline(
        pipeline_name="replay_openweather",
        destination=dlt.destinations.postgres(postgres_dsn),
        dataset_name="raw",
    )

    @dlt.resource(write_disposition="append", table_name="weather_current")
    def replay_current():
        yield from records_current

    @dlt.resource(write_disposition="append", table_name="weather_forecast")
    def replay_forecast():
        yield from records_forecast

    total = len(records_current) + len(records_forecast)
    if total == 0:
        logger.info("No records to replay")
        return 0

    load_info = pipeline.run([replay_current, replay_forecast])
    logger.info("Replay load complete: %s", load_info)
    return total


def run_dbt(dbt_dir: str = "/opt/airflow/dbt", dbt_bin: str = "/usr/python/bin/dbt") -> None:
    """Run dbt transformations after replay."""
    logger.info("Running dbt transformations...")
    subprocess.run(
        [dbt_bin, "deps", "--profiles-dir", "."],
        cwd=dbt_dir,
        check=True,
    )
    subprocess.run(
        [dbt_bin, "run", "--profiles-dir", "."],
        cwd=dbt_dir,
        check=True,
    )
    subprocess.run(
        [dbt_bin, "test", "--profiles-dir", "."],
        cwd=dbt_dir,
        check=True,
    )
    logger.info("dbt transformations and tests complete")


def reprocess(
    date_start: datetime,
    date_end: datetime,
    minio_endpoint: str = "minio:9000",
    minio_access_key: str = "minioadmin",
    minio_secret_key: str = "minioadmin",
    bucket: str = "weather-data-lake",
    postgres_dsn: str = "postgresql://airflow:airflow@postgres:5432/weather_db",
    run_transforms: bool = True,
) -> dict:
    """Full replay: list files -> load into PG -> run dbt."""
    client = _build_minio_client(minio_endpoint, minio_access_key, minio_secret_key)
    keys = list_raw_files(client, bucket, date_start, date_end)
    logger.info("Found %d raw files for replay (%s to %s)", len(keys), date_start, date_end)

    if not keys:
        return {"files_found": 0, "records_loaded": 0, "status": "no_data"}

    records = load_raw_to_postgres(client, bucket, keys, postgres_dsn)

    if run_transforms:
        run_dbt()

    return {"files_found": len(keys), "records_loaded": records, "status": "success"}


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    parser = argparse.ArgumentParser(description="Replay historical raw data from MinIO data lake")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--no-transform", action="store_true", help="Skip dbt transformations")
    parser.add_argument("--minio-endpoint", default="minio:9000")
    parser.add_argument("--bucket", default="weather-data-lake")
    parser.add_argument("--postgres-dsn", default="postgresql://airflow:airflow@postgres:5432/weather_db")
    args = parser.parse_args()

    date_start = datetime.strptime(args.start, "%Y-%m-%d")
    date_end = datetime.strptime(args.end, "%Y-%m-%d")

    result = reprocess(
        date_start=date_start,
        date_end=date_end,
        minio_endpoint=args.minio_endpoint,
        bucket=args.bucket,
        postgres_dsn=args.postgres_dsn,
        run_transforms=not args.no_transform,
    )
    logger.info("Replay result: %s", result)
    sys.exit(0 if result["status"] == "success" else 1)


if __name__ == "__main__":
    main()
