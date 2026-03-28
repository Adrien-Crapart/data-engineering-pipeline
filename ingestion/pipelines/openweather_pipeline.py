"""
OpenWeather ingestion pipeline using dlt (Data Load Tool).

Extracts current weather and 5-day forecast data from the OpenWeather API
for a configurable list of cities:
  1. Fetches data from the API (with retry + detailed logging of every call)
  2. Validates against data contracts
  3. Stores raw response as versioned Parquet on MinIO S3 (data lake)
  4. Loads structured data into the ``raw`` schema of PostgreSQL via dlt
"""

from __future__ import annotations

import logging
import time
from typing import Any, Iterator

import dlt
import requests

from contracts.validator import validate_contract
from ingestion.config import PipelineConfig

logger = logging.getLogger(__name__)

REQUESTS_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_BACKOFF = 2


def _validate(data: dict, contract_name: str) -> None:
    try:
        errors = validate_contract(data, contract_name)
        if errors:
            logger.warning("Contract violations for %s: %s", contract_name, errors)
    except FileNotFoundError:
        logger.debug("No contract found for %s — skipping", contract_name)


def _get_datalake_client(config: PipelineConfig):
    try:
        from ingestion.storage.minio_client import DataLakeClient

        client = DataLakeClient(config)
        logger.info(
            "MinIO DataLake client ready — endpoint=%s bucket=%s",
            config.minio_endpoint,
            config.minio_bucket,
        )
        return client
    except Exception:
        logger.warning("MinIO data lake unavailable — raw archiving disabled", exc_info=True)
        return None


def _fetch_with_retry(
    url: str, params: dict, retries: int = MAX_RETRIES,
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
                attempt, retries, wait, exc,
            )
            time.sleep(wait)
    return {}


@dlt.source(name="openweather")
def openweather_source(config: PipelineConfig):
    lake = _get_datalake_client(config)

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

                s3_path = None
                if lake:
                    s3_path = lake.store_raw(data, source="weather_current", city=city)

                data["_extraction_city"] = city
                data["_extraction_timestamp"] = time.time()

                temp = data.get("main", {}).get("temp", "N/A")
                logger.info(
                    "OK current_weather city=%s temp=%s elapsed=%.2fs s3=%s",
                    city, temp, elapsed, s3_path or "disabled",
                )
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

                s3_path = None
                if lake:
                    s3_path = lake.store_raw(data, source="weather_forecast", city=city)

                data["_extraction_city"] = city
                data["_extraction_timestamp"] = time.time()

                cnt = data.get("cnt", "N/A")
                logger.info(
                    "OK forecast city=%s entries=%s elapsed=%.2fs s3=%s",
                    city, cnt, elapsed, s3_path or "disabled",
                )
                yield data
            except requests.RequestException:
                logger.error("SKIP forecast city=%s — all retries exhausted", city)
                continue

    return current_weather, weather_forecast


def run_pipeline(config: PipelineConfig | None = None) -> dict[str, Any]:
    if config is None:
        config = PipelineConfig.from_env()

    pipeline = dlt.pipeline(
        pipeline_name="openweather",
        destination=dlt.destinations.postgres(config.postgres_dsn),
        dataset_name="raw",
    )

    logger.info(
        "Starting ingestion — cities=%s postgres=%s:%s/%s minio=%s/%s",
        config.cities,
        config.postgres_host,
        config.postgres_port,
        config.postgres_db,
        config.minio_endpoint,
        config.minio_bucket,
    )
    start = time.time()
    load_info = pipeline.run(openweather_source(config))
    elapsed = time.time() - start

    metrics: dict[str, Any] = {
        "duration_seconds": round(elapsed, 2),
        "cities": config.cities,
        "status": "success",
        "load_info": str(load_info),
    }
    logger.info("Pipeline COMPLETED in %.2fs — %s", elapsed, metrics)
    return metrics


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    from dotenv import load_dotenv

    load_dotenv()
    result = run_pipeline()
    print(result)
