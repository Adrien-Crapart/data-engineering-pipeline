"""MinIO client for storing raw API responses in the S3-compatible data lake."""

from __future__ import annotations

import io
import json
import logging
from datetime import datetime, timezone

from minio import Minio
from minio.error import S3Error

from ingestion.config import PipelineConfig

logger = logging.getLogger(__name__)


class DataLakeClient:
    """Writes immutable raw JSON objects to MinIO following a partitioned layout.

    Layout: raw/openweather/year=YYYY/month=MM/day=DD/{source}_{timestamp}.json
    """

    def __init__(self, config: PipelineConfig) -> None:
        self._client = Minio(
            endpoint=config.minio_endpoint,
            access_key=config.minio_access_key,
            secret_key=config.minio_secret_key,
            secure=config.minio_secure,
        )
        self._bucket = config.minio_bucket
        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        try:
            if not self._client.bucket_exists(self._bucket):
                self._client.make_bucket(self._bucket)
                logger.info("Created bucket %s", self._bucket)
        except S3Error as exc:
            logger.error("Failed to ensure bucket %s: %s", self._bucket, exc)
            raise

    def store_raw(self, data: dict, source: str, city: str) -> str:
        """Store a raw JSON response and return the object key."""
        now = datetime.now(timezone.utc)
        timestamp = int(now.timestamp())
        key = (
            f"raw/openweather/"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            f"{source}_{city.lower()}_{timestamp}.json"
        )

        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self._client.put_object(
            bucket_name=self._bucket,
            object_name=key,
            data=io.BytesIO(payload),
            length=len(payload),
            content_type="application/json",
        )
        logger.info("Stored raw data: s3://%s/%s (%d bytes)", self._bucket, key, len(payload))
        return key

    def list_raw_files(
        self, date_start: datetime, date_end: datetime, source: str = "openweather"
    ) -> list[str]:
        """List raw JSON keys within a date range (inclusive)."""
        keys: list[str] = []
        current = date_start
        while current <= date_end:
            prefix = (
                f"raw/{source}/"
                f"year={current.year}/month={current.month:02d}/day={current.day:02d}/"
            )
            for obj in self._client.list_objects(self._bucket, prefix=prefix):
                keys.append(obj.object_name)
            current = current.replace(day=current.day + 1) if current.day < 28 else (
                current.replace(month=current.month + 1, day=1) if current.month < 12
                else current.replace(year=current.year + 1, month=1, day=1)
            )
        return keys

    def get_raw(self, key: str) -> dict:
        """Download and parse a raw JSON file from the data lake."""
        response = self._client.get_object(self._bucket, key)
        try:
            return json.loads(response.read().decode("utf-8"))
        finally:
            response.close()
            response.release_conn()
