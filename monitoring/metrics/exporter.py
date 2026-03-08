"""Prometheus metrics exporter for the weather data pipeline.

Exposes pipeline-level metrics that can be pushed to a Prometheus Pushgateway
or scraped directly when run as a standalone HTTP server.
"""

from __future__ import annotations

import logging
import time

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    push_to_gateway,
)

logger = logging.getLogger(__name__)

registry = CollectorRegistry()

PIPELINE_DURATION = Histogram(
    "pipeline_duration_seconds",
    "Total pipeline execution duration",
    ["pipeline_name"],
    registry=registry,
)

INGESTION_LATENCY = Histogram(
    "ingestion_latency_seconds",
    "Time to ingest data from a single API call",
    ["source", "city"],
    registry=registry,
)

API_RESPONSE_TIME = Histogram(
    "api_response_time_seconds",
    "OpenWeather API response time",
    ["endpoint", "city"],
    registry=registry,
)

RECORDS_INGESTED = Counter(
    "records_ingested_total",
    "Total number of records ingested",
    ["source"],
    registry=registry,
)

PIPELINE_FAILURES = Counter(
    "pipeline_failures_total",
    "Total number of pipeline failures",
    ["pipeline_name", "task"],
    registry=registry,
)

DATA_FRESHNESS = Gauge(
    "data_freshness_seconds",
    "Age of the most recent data in seconds",
    ["source"],
    registry=registry,
)

PIPELINE_RUN_STATUS = Gauge(
    "pipeline_last_run_success",
    "1 if the last pipeline run was successful, 0 otherwise",
    ["pipeline_name"],
    registry=registry,
)


def record_api_call(endpoint: str, city: str, duration: float) -> None:
    API_RESPONSE_TIME.labels(endpoint=endpoint, city=city).observe(duration)


def record_ingestion(source: str, city: str, duration: float, record_count: int = 1) -> None:
    INGESTION_LATENCY.labels(source=source, city=city).observe(duration)
    RECORDS_INGESTED.labels(source=source).inc(record_count)


def record_pipeline_run(pipeline_name: str, duration: float, success: bool) -> None:
    PIPELINE_DURATION.labels(pipeline_name=pipeline_name).observe(duration)
    PIPELINE_RUN_STATUS.labels(pipeline_name=pipeline_name).set(1 if success else 0)
    if not success:
        PIPELINE_FAILURES.labels(pipeline_name=pipeline_name, task="pipeline").inc()


def update_freshness(source: str, last_timestamp: float) -> None:
    age = time.time() - last_timestamp
    DATA_FRESHNESS.labels(source=source).set(age)


def push_metrics(gateway: str = "prometheus-pushgateway:9091", job: str = "weather_pipeline") -> None:
    """Push all collected metrics to the Prometheus Pushgateway."""
    try:
        push_to_gateway(gateway, job=job, registry=registry)
        logger.info("Metrics pushed to %s", gateway)
    except Exception:
        logger.warning("Failed to push metrics to %s", gateway, exc_info=True)
