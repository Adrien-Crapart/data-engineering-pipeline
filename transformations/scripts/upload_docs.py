#!/usr/bin/env python3
"""Upload dbt docs to MinIO S3 with timestamped versioning.

Uploads manifest.json, catalog.json, run_results.json, and index.html
to S3 under both a timestamped path (for history) and a latest path.

Layout:
  _reports/dbt_docs/YYYY/MM/DD/HHMMSS/  — versioned snapshot
  _reports/dbt_docs/latest/               — always-current pointer
"""
from __future__ import annotations

import io
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from minio import Minio

logger = logging.getLogger(__name__)

TARGET_DIR = Path(os.getenv("DBT_TARGET_DIR", "/app/target"))
BUCKET = os.getenv("MINIO_BUCKET_NAME", "data-lake")
PREFIX = os.getenv("REPORT_S3_PREFIX", "_reports/dbt_docs")


def _get_client() -> Minio:
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


def _upload(client: Minio, key: str, data: bytes, content_type: str) -> None:
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
    client.put_object(
        bucket_name=BUCKET,
        object_name=key,
        data=io.BytesIO(data),
        length=len(data),
        content_type=content_type,
    )
    logger.info("Uploaded s3://%s/%s (%d bytes)", BUCKET, key, len(data))


def _content_type(name: str) -> str:
    if name.endswith(".json"):
        return "application/json"
    if name.endswith(".html"):
        return "text/html"
    return "application/octet-stream"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    client = _get_client()

    now = datetime.now(timezone.utc)
    ts_prefix = f"{PREFIX}/{now.year}/{now.month:02d}/{now.day:02d}/{now.strftime('%H%M%S')}"
    latest_prefix = f"{PREFIX}/latest"

    artifacts = ["manifest.json", "catalog.json", "run_results.json", "index.html"]
    uploaded = 0

    for name in artifacts:
        path = TARGET_DIR / name
        if not path.exists():
            logger.warning("Artifact not found: %s — skipping", path)
            continue

        data = path.read_bytes()
        ct = _content_type(name)

        _upload(client, f"{ts_prefix}/{name}", data, ct)
        _upload(client, f"{latest_prefix}/{name}", data, ct)
        uploaded += 1

    logger.info(
        "dbt docs upload complete — %d artifacts uploaded to s3://%s/%s",
        uploaded,
        BUCKET,
        ts_prefix,
    )


if __name__ == "__main__":
    main()
