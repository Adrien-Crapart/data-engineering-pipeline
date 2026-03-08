"""
OpenWeather ingestion pipeline using dlt (Data Load Tool).

Extracts current weather and 5-day forecast data from the OpenWeather API
for a configurable list of cities, stores raw JSON in MinIO (data lake),
then loads structured data into the ``raw`` schema of PostgreSQL.
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
    """Validate data against its contract, log warnings on violations."""
    try:
        errors = validate_contract(data, contract_name)
        if errors:
            logger.warning("Contract violations for %s: %s", contract_name, errors)
    except FileNotFoundError:
        logger.debug("No contract found for %s – skipping validation", contract_name)


def _get_datalake_client(config: PipelineConfig):
    """Lazy-init the MinIO data lake client (returns None when unavailable)."""
    try:
        from ingestion.storage.minio_client import DataLakeClient

        return DataLakeClient(config)
    except Exception:
        logger.warning("MinIO data lake unavailable – raw archiving disabled")
        return None


def _fetch_with_retry(url: str, params: dict, retries: int = MAX_RETRIES) -> dict[str, Any]:
    """GET request with exponential backoff retry."""
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=REQUESTS_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            if attempt == retries:
                logger.error("Request failed after %d attempts: %s", retries, exc)
                raise
            wait = RETRY_BACKOFF ** attempt
            logger.warning(
                "Attempt %d/%d failed, retrying in %ds: %s", attempt, retries, wait, exc
            )
            time.sleep(wait)
    return {}


@dlt.source(name="openweather")
def openweather_source(config: PipelineConfig):
    """Extract weather data from OpenWeather API for configured cities."""
    lake = _get_datalake_client(config)

    @dlt.resource(write_disposition="append", table_name="weather_current")
    def current_weather() -> Iterator[dict[str, Any]]:
        """Fetch current weather for each city."""
        for city in config.cities:
            start = time.time()
            try:
                data = _fetch_with_retry(
                    f"{config.base_url}/weather",
                    params={"q": city, "appid": config.api_key, "units": config.units},
                )
                elapsed = time.time() - start

                _validate(data, "weather_current")

                if lake:
                    lake.store_raw(data, source="weather_current", city=city)

                data["_extraction_city"] = city
                data["_extraction_timestamp"] = time.time()
                logger.info("Extracted current weather for %s in %.2fs", city, elapsed)
                yield data
            except requests.RequestException:
                logger.error("Skipping current weather for %s after all retries failed", city)
                continue

    @dlt.resource(write_disposition="append", table_name="weather_forecast")
    def weather_forecast() -> Iterator[dict[str, Any]]:
        """Fetch 5-day / 3-hour forecast for each city."""
        for city in config.cities:
            start = time.time()
            try:
                data = _fetch_with_retry(
                    f"{config.base_url}/forecast",
                    params={"q": city, "appid": config.api_key, "units": config.units},
                )
                elapsed = time.time() - start

                _validate(data, "weather_forecast")

                if lake:
                    lake.store_raw(data, source="weather_forecast", city=city)

                data["_extraction_city"] = city
                data["_extraction_timestamp"] = time.time()
                logger.info("Extracted forecast for %s in %.2fs", city, elapsed)
                yield data
            except requests.RequestException:
                logger.error("Skipping forecast for %s after all retries failed", city)
                continue

    return current_weather, weather_forecast


def run_pipeline(config: PipelineConfig | None = None) -> dict[str, Any]:
    """Run the full ingestion pipeline.

    Returns a dict of load metrics (duration, status, etc.).
    """
    if config is None:
        config = PipelineConfig.from_env()

    pipeline = dlt.pipeline(
        pipeline_name="openweather",
        destination=dlt.destinations.postgres(config.postgres_dsn),
        dataset_name="raw",
    )

    logger.info("Starting ingestion for cities: %s", config.cities)
    start = time.time()
    load_info = pipeline.run(openweather_source(config))
    elapsed = time.time() - start

    metrics: dict[str, Any] = {
        "duration_seconds": round(elapsed, 2),
        "cities": config.cities,
        "status": "success",
        "load_info": str(load_info),
    }
    logger.info("Pipeline completed: %s", metrics)
    return metrics


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    from dotenv import load_dotenv

    load_dotenv()
    result = run_pipeline()
    print(result)
