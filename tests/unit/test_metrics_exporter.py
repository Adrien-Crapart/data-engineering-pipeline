"""Tests for the Prometheus metrics exporter."""

from __future__ import annotations

import time
from unittest.mock import patch

from monitoring.metrics.exporter import (
    PIPELINE_DURATION,
    PIPELINE_RUN_STATUS,
    RECORDS_INGESTED,
    record_api_call,
    record_ingestion,
    record_pipeline_run,
    update_freshness,
)


class TestRecordApiCall:
    def test_observes_histogram(self):
        record_api_call(endpoint="/weather", city="Paris", duration=0.5)


class TestRecordIngestion:
    def test_records_latency_and_count(self):
        record_ingestion(source="weather_current", city="Paris", duration=1.2, record_count=5)


class TestRecordPipelineRun:
    def test_success_sets_status_to_1(self):
        record_pipeline_run("test_pipeline", duration=10.0, success=True)
        assert PIPELINE_RUN_STATUS.labels(pipeline_name="test_pipeline")._value.get() == 1

    def test_failure_sets_status_to_0(self):
        record_pipeline_run("test_pipeline_fail", duration=5.0, success=False)
        assert PIPELINE_RUN_STATUS.labels(pipeline_name="test_pipeline_fail")._value.get() == 0


class TestUpdateFreshness:
    def test_sets_freshness_gauge(self):
        ts = time.time() - 300
        update_freshness("weather_current", ts)


class TestPushMetrics:
    @patch("monitoring.metrics.exporter.push_to_gateway")
    def test_push_calls_gateway(self, mock_push):
        from monitoring.metrics.exporter import push_metrics

        push_metrics(gateway="localhost:9091", job="test")
        mock_push.assert_called_once()

    @patch("monitoring.metrics.exporter.push_to_gateway", side_effect=Exception("Connection refused"))
    def test_push_handles_failure_gracefully(self, mock_push):
        from monitoring.metrics.exporter import push_metrics

        push_metrics(gateway="unreachable:9091", job="test")
