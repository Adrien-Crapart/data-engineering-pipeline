"""MinIO client for storing raw API responses as Parquet in the S3-compatible data lake."""

from __future__ import annotations

import io
import json
import logging
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio
from minio.error import S3Error

from ingestion.config import PipelineConfig

logger = logging.getLogger(__name__)


class DataLakeClient:
    """Writes immutable raw data as Parquet to MinIO, partitioned by date.

    Layout: raw/{source}/year=YYYY/month=MM/day=DD/{source}_{city}_{ts}.parquet
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
        logger.info(
            "DataLakeClient initialized — endpoint=%s bucket=%s",
            config.minio_endpoint,
            self._bucket,
        )

    def _ensure_bucket(self) -> None:
        try:
            if not self._client.bucket_exists(self._bucket):
                self._client.make_bucket(self._bucket)
                logger.info("Created bucket: %s", self._bucket)
            else:
                logger.info("Bucket exists: %s", self._bucket)
        except S3Error as exc:
            logger.error("Failed to ensure bucket %s: %s", self._bucket, exc)
            raise

    def store_raw_parquet(self, data: dict, source: str, city: str) -> str:
        """Store a raw API response as a Parquet file, return the S3 key.

        The dict is flattened into a single-row Parquet file with the full
        JSON payload stored alongside extracted top-level fields.
        """
        now = datetime.now(timezone.utc)
        timestamp = int(now.timestamp())
        key = (
            f"raw/{source}/"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            f"{source}_{city.lower()}_{timestamp}.parquet"
        )

        row = {
            "city": city,
            "source": source,
            "extracted_at": now.isoformat(),
            "raw_json": json.dumps(data, ensure_ascii=False),
        }
        if source == "weather_current":
            row["temperature"] = data.get("main", {}).get("temp")
            row["humidity"] = data.get("main", {}).get("humidity")
            row["wind_speed"] = data.get("wind", {}).get("speed")
            row["weather_main"] = (data.get("weather", [{}])[0].get("main", ""))
            row["dt"] = data.get("dt")
        elif source == "weather_forecast":
            row["forecast_count"] = data.get("cnt")
            row["city_name"] = data.get("city", {}).get("name")

        table = pa.Table.from_pylist([row])
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        payload_size = buf.getbuffer().nbytes

        self._client.put_object(
            bucket_name=self._bucket,
            object_name=key,
            data=buf,
            length=payload_size,
            content_type="application/octet-stream",
        )

        s3_url = f"s3://{self._bucket}/{key}"
        logger.info(
            "Stored Parquet: %s (%d bytes) | city=%s source=%s",
            s3_url, payload_size, city, source,
        )
        return s3_url

    def store_raw(self, data: dict, source: str, city: str) -> str:
        """Alias — delegates to store_raw_parquet for backward compat."""
        return self.store_raw_parquet(data, source, city)

    def list_raw_files(
        self, prefix: str = "raw/", recursive: bool = True,
    ) -> list[str]:
        """List all raw data files under a prefix."""
        keys: list[str] = []
        for obj in self._client.list_objects(self._bucket, prefix=prefix, recursive=recursive):
            keys.append(obj.object_name)
        return keys

    def get_raw(self, key: str) -> bytes:
        """Download a raw file from the data lake."""
        response = self._client.get_object(self._bucket, key)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()
