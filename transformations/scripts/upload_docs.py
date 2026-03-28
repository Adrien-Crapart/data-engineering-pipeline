#!/usr/bin/env python3
"""Upload dbt docs (manifest.json, catalog.json, index.html) to MinIO S3.

Generates a self-contained HTML by embedding the JSON artifacts, then uploads
all files to the configured S3 bucket under _reports/dbt_docs/.
"""
from __future__ import annotations

import io
import json
import logging
import os
import re
from pathlib import Path

from minio import Minio

logger = logging.getLogger(__name__)

TARGET_DIR = Path(os.getenv("DBT_TARGET_DIR", "/app/target"))
BUCKET = os.getenv("MINIO_BUCKET_NAME", "weather-data-lake")
PREFIX = os.getenv("REPORT_S3_PREFIX", "_reports/dbt_docs")


def _get_client() -> Minio:
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "").replace("https://", "")
    return Minio(
        endpoint=endpoint,
        access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        secure=False,
    )


def _build_single_page(target: Path) -> str:
    """Build a self-contained index.html with embedded manifest and catalog."""
    index_html = (target / "index.html").read_text()
    manifest = (target / "manifest.json").read_text()
    catalog = (target / "catalog.json").read_text()

    index_html = re.sub(
        r"o\(\[defined\(defined\(t\.defined\)\)\]\)",
        "",
        index_html,
    )

    search_str = 'o=[defined(googletag)]'
    if search_str in index_html:
        index_html = index_html.replace(search_str, "o=[]")

    content = index_html
    content = content.replace(
        '"defined(googletag)"',
        json.dumps(json.loads(manifest), separators=(",", ":")),
    )

    return content


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


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    client = _get_client()

    for name in ("manifest.json", "catalog.json", "run_results.json"):
        path = TARGET_DIR / name
        if path.exists():
            _upload(client, f"{PREFIX}/{name}", path.read_bytes(), "application/json")

    index_path = TARGET_DIR / "index.html"
    if index_path.exists():
        _upload(client, f"{PREFIX}/index.html", index_path.read_bytes(), "text/html")

    logger.info("dbt docs upload complete")


if __name__ == "__main__":
    main()
