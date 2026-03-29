#!/usr/bin/env python3
"""Run Soda scans and upload results to MinIO S3 with timestamped versioning.

Wraps ``soda scan`` to capture results and produce JSON + HTML reports.

Usage:
    python -m data_quality.soda.run_scan --layer staging
    python -m data_quality.soda.run_scan --layer mart
    python -m data_quality.soda.run_scan --layer all

Report layout on S3:
    _reports/soda/YYYY/MM/DD/HHMMSS/soda_{layer}.json
    _reports/soda/YYYY/MM/DD/HHMMSS/soda_{layer}.html
    _reports/soda/latest/soda_{layer}.json
    _reports/soda/latest/soda_{layer}.html
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timezone

from minio import Minio

logger = logging.getLogger(__name__)

REPORT_PREFIX = "_reports/soda"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SCAN_CONFIGS = {
    "staging": {
        "config": os.path.join(BASE_DIR, "configuration.yml"),
        "checks": os.path.join(BASE_DIR, "checks", "staging_checks.yml"),
    },
    "mart": {
        "config": os.path.join(BASE_DIR, "configuration_mart.yml"),
        "checks": os.path.join(BASE_DIR, "checks", "mart_checks.yml"),
    },
}


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


def _run_soda_scan(layer: str) -> dict:
    cfg = SCAN_CONFIGS[layer]
    cmd = [
        "soda", "scan",
        "-d", "datawarehouse",
        "-c", cfg["config"],
        cfg["checks"],
    ]
    logger.info("Running: %s", " ".join(cmd))

    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )

    logger.info("Soda stdout:\n%s", proc.stdout)
    if proc.stderr:
        logger.warning("Soda stderr:\n%s", proc.stderr)

    return {
        "layer": layer,
        "exit_code": proc.returncode,
        "success": proc.returncode == 0,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
        "command": " ".join(cmd),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def _build_html_report(results: list[dict], title: str) -> str:
    rows = []
    for r in results:
        status = "PASS" if r["success"] else "FAIL"
        color = "#22c55e" if r["success"] else "#ef4444"
        stdout_escaped = r["stdout"].replace("<", "&lt;").replace(">", "&gt;")
        rows.append(
            f'<tr><td>{r["layer"]}</td>'
            f'<td style="color:{color};font-weight:bold">{status}</td>'
            f'<td>exit code: {r["exit_code"]}</td>'
            f'<td><pre>{stdout_escaped}</pre></td></tr>'
        )

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"><title>Soda Report — {title}</title>
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
<h1>Soda Core Scan — {title}</h1>
<p class="meta">Generated: {now}</p>
<table>
<thead><tr><th>Layer</th><th>Status</th><th>Exit Code</th><th>Output</th></tr></thead>
<tbody>{''.join(rows)}</tbody>
</table>
</body></html>"""


def _upload_reports(client: Minio | None, results: list[dict], name: str) -> None:
    if not client:
        return

    now = datetime.now(timezone.utc)
    ts_prefix = f"{REPORT_PREFIX}/{now.year}/{now.month:02d}/{now.day:02d}/{now.strftime('%H%M%S')}"
    latest_prefix = f"{REPORT_PREFIX}/latest"

    report_data = {"results": results, "generated_at": now.isoformat()}
    json_data = json.dumps(report_data, indent=2, default=str).encode()
    _upload(client, f"{ts_prefix}/{name}.json", json_data, "application/json")
    _upload(client, f"{latest_prefix}/{name}.json", json_data, "application/json")

    html_data = _build_html_report(results, name).encode()
    _upload(client, f"{ts_prefix}/{name}.html", html_data, "text/html")
    _upload(client, f"{latest_prefix}/{name}.html", html_data, "text/html")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    parser = argparse.ArgumentParser(description="Run Soda scans with S3 report upload")
    parser.add_argument("--layer", choices=["staging", "mart", "all"], default="all")
    args = parser.parse_args()

    minio = _get_minio()
    layers = list(SCAN_CONFIGS.keys()) if args.layer == "all" else [args.layer]
    results = []
    failed = False

    for layer in layers:
        logger.info("=== Running Soda scan: %s ===", layer)
        result = _run_soda_scan(layer)
        results.append(result)
        if not result["success"]:
            failed = True

    _upload_reports(minio, results, f"soda_{args.layer}")

    status = "FAIL" if failed else "PASS"
    logger.info("=== Soda scan complete: %s ===", status)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
