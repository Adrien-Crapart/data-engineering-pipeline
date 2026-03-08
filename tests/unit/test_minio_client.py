"""Tests for the MinIO DataLakeClient."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from ingestion.config import PipelineConfig


@pytest.fixture
def mock_config():
    return PipelineConfig(
        api_key="test_key",
        cities=["Paris"],
        minio_endpoint="localhost:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        minio_bucket="test-bucket",
        minio_secure=False,
    )


@pytest.fixture
def mock_minio_client():
    with patch("ingestion.storage.minio_client.Minio") as mock_cls:
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        mock_instance.bucket_exists.return_value = True
        yield mock_instance, mock_cls


class TestDataLakeClient:
    def test_init_creates_client(self, mock_config, mock_minio_client):
        mock_instance, mock_cls = mock_minio_client

        from ingestion.storage.minio_client import DataLakeClient

        client = DataLakeClient(mock_config)

        mock_cls.assert_called_once_with(
            endpoint="localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )
        mock_instance.bucket_exists.assert_called_once_with("test-bucket")

    def test_init_creates_bucket_if_missing(self, mock_config, mock_minio_client):
        mock_instance, _ = mock_minio_client
        mock_instance.bucket_exists.return_value = False

        from ingestion.storage.minio_client import DataLakeClient

        DataLakeClient(mock_config)
        mock_instance.make_bucket.assert_called_once_with("test-bucket")

    def test_store_raw_generates_correct_key(self, mock_config, mock_minio_client):
        mock_instance, _ = mock_minio_client

        from ingestion.storage.minio_client import DataLakeClient

        client = DataLakeClient(mock_config)
        data = {"temp": 20.5, "city": "Paris"}

        key = client.store_raw(data, source="weather_current", city="Paris")

        assert key.startswith("raw/openweather/year=")
        assert "weather_current_paris_" in key
        assert key.endswith(".json")
        mock_instance.put_object.assert_called_once()

    def test_store_raw_payload_is_valid_json(self, mock_config, mock_minio_client):
        mock_instance, _ = mock_minio_client

        from ingestion.storage.minio_client import DataLakeClient

        client = DataLakeClient(mock_config)
        data = {"temp": 20.5}

        client.store_raw(data, source="weather_current", city="Paris")

        call_args = mock_instance.put_object.call_args
        assert call_args.kwargs["content_type"] == "application/json"
        payload = call_args.kwargs["data"].read()
        parsed = json.loads(payload.decode("utf-8"))
        assert parsed == data

    def test_get_raw_returns_parsed_json(self, mock_config, mock_minio_client):
        mock_instance, _ = mock_minio_client

        from ingestion.storage.minio_client import DataLakeClient

        client = DataLakeClient(mock_config)

        expected = {"temp": 20.5}
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps(expected).encode("utf-8")
        mock_instance.get_object.return_value = mock_response

        result = client.get_raw("some/key.json")
        assert result == expected
        mock_response.close.assert_called_once()
        mock_response.release_conn.assert_called_once()

    def test_list_raw_files(self, mock_config, mock_minio_client):
        mock_instance, _ = mock_minio_client

        from ingestion.storage.minio_client import DataLakeClient

        client = DataLakeClient(mock_config)

        mock_obj = MagicMock()
        mock_obj.object_name = "raw/openweather/year=2025/month=06/day=15/weather.json"
        mock_instance.list_objects.return_value = [mock_obj]

        keys = client.list_raw_files(
            date_start=datetime(2025, 6, 15, tzinfo=timezone.utc),
            date_end=datetime(2025, 6, 15, tzinfo=timezone.utc),
        )
        assert len(keys) == 1
        assert keys[0] == mock_obj.object_name
