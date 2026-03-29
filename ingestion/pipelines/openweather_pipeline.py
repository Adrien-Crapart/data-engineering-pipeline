"""OpenWeather ingestion pipeline using dlt (Data Load Tool).

Extracts current weather and 5-day forecast data from the OpenWeather API
for a configurable list of cities:
  1. Fetches data from the API (with retry + detailed logging)
  2. Validates against data contracts
  3. Stores everything as versioned Parquet on MinIO S3 via dlt filesystem destination

S3 layout: s3://{bucket}/raw/openweather/{table_name}/{load_id}.{file_id}.parquet
DLT metadata (_dlt_loads, _dlt_pipeline_state, _dlt_version) also on S3.
"""

from __future__ import annotations

import logging
import sys
import time
from typing import Any, Iterator

import dlt
import requests

from ingestion.config import PipelineConfig

logger = logging.getLogger(__name__)

REQUESTS_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_BACKOFF = 2


def _validate(data: dict, contract_name: str) -> None:
    try:
        from contracts.validator import validate_contract

        errors = validate_contract(data, contract_name)
        if errors:
            logger.warning("Contract violations for %s: %s", contract_name, errors)
    except FileNotFoundError:
        logger.debug("No contract found for %s — skipping", contract_name)
    except ImportError:
        logger.debug("Contracts module unavailable — skipping validation")


def _fetch_with_retry(
    url: str,
    params: dict,
    retries: int = MAX_RETRIES,
) -> dict[str, Any]:
    safe_params = {k: (v if k != "appid" else "***") for k, v in params.items()}
    logger.info("API REQUEST: GET %s params=%s", url, safe_params)

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=REQUESTS_TIMEOUT)
            logger.info(
                "API RESPONSE: %s %d (%d bytes) in %.2fs",
                url.split("/")[-1],
                resp.status_code,
                len(resp.content),
                resp.elapsed.total_seconds(),
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            if attempt == retries:
                logger.error("Request FAILED after %d attempts: %s — url=%s", retries, exc, url)
                raise
            wait = RETRY_BACKOFF**attempt
            logger.warning(
                "Attempt %d/%d failed, retrying in %ds: %s",
                attempt,
                retries,
                wait,
                exc,
            )
            time.sleep(wait)
    return {}


@dlt.source(name="openweather")
def openweather_source(config: PipelineConfig):
    @dlt.resource(write_disposition="append", table_name="weather_current")
    def current_weather() -> Iterator[dict[str, Any]]:
        logger.info("=== Extracting current weather for %d cities ===", len(config.cities))
        for city in config.cities:
            start = time.time()
            url = f"{config.base_url}/weather"
            try:
                data = _fetch_with_retry(
                    url,
                    params={"q": city, "appid": config.api_key, "units": config.units},
                )
                elapsed = time.time() - start

                _validate(data, "weather_current")

                data["_extraction_city"] = city
                data["_extraction_timestamp"] = time.time()

                temp = data.get("main", {}).get("temp", "N/A")
                logger.info("OK current_weather city=%s temp=%s elapsed=%.2fs", city, temp, elapsed)
                yield data
            except requests.RequestException:
                logger.error("SKIP current_weather city=%s — all retries exhausted", city)
                continue

    @dlt.resource(write_disposition="append", table_name="weather_forecast")
    def weather_forecast() -> Iterator[dict[str, Any]]:
        logger.info("=== Extracting forecast for %d cities ===", len(config.cities))
        for city in config.cities:
            start = time.time()
            url = f"{config.base_url}/forecast"
            try:
                data = _fetch_with_retry(
                    url,
                    params={"q": city, "appid": config.api_key, "units": config.units},
                )
                elapsed = time.time() - start

                _validate(data, "weather_forecast")

                data["_extraction_city"] = city
                data["_extraction_timestamp"] = time.time()

                cnt = data.get("cnt", "N/A")
                logger.info("OK forecast city=%s entries=%s elapsed=%.2fs", city, cnt, elapsed)
                yield data
            except requests.RequestException:
                logger.error("SKIP forecast city=%s — all retries exhausted", city)
                continue

    return current_weather, weather_forecast


def _build_filesystem_destination(config: PipelineConfig):
    """Build a dlt filesystem destination pointing to MinIO S3."""
    endpoint_url = config.minio_endpoint
    if not endpoint_url.startswith("http"):
        endpoint_url = f"http://{endpoint_url}"

    return dlt.destinations.filesystem(
        bucket_url=f"s3://{config.minio_bucket}/raw/openweather",
        credentials={
            "aws_access_key_id": config.minio_access_key,
            "aws_secret_access_key": config.minio_secret_key,
            "endpoint_url": endpoint_url,
            "region_name": "us-east-1",
        },
    )


def run_pipeline(config: PipelineConfig | None = None) -> dict[str, Any]:
    if config is None:
        config = PipelineConfig.from_env()

    destination = _build_filesystem_destination(config)

    pipeline = dlt.pipeline(
        pipeline_name="openweather",
        destination=destination,
        dataset_name="data",
    )

    logger.info(
        "Starting ingestion — cities=%s s3=%s/raw/openweather minio=%s",
        config.cities,
        config.minio_bucket,
        config.minio_endpoint,
    )
    start = time.time()
    load_info = pipeline.run(openweather_source(config), loader_file_format="parquet")
    elapsed = time.time() - start

    metrics: dict[str, Any] = {
        "duration_seconds": round(elapsed, 2),
        "cities": config.cities,
        "status": "success",
        "destination": "filesystem (S3)",
        "load_info": str(load_info),
    }
    logger.info("Pipeline COMPLETED in %.2fs — %s", elapsed, metrics)
    return metrics


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )
    from dotenv import load_dotenv

    load_dotenv()
    result = run_pipeline()
    print(result)
