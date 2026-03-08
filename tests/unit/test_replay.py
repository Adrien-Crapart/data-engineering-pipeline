"""Tests for the replay pipeline."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from replay.reprocess_pipeline import (
    _build_minio_client,
    list_raw_files,
    reprocess,
)


class TestBuildMinioClient:
    def test_returns_minio_client(self):
        with patch("replay.reprocess_pipeline.Minio") as mock_cls:
            client = _build_minio_client(
                endpoint="localhost:9000",
                access_key="admin",
                secret_key="admin",
            )
            mock_cls.assert_called_once_with(
                endpoint="localhost:9000",
                access_key="admin",
                secret_key="admin",
                secure=False,
            )


class TestListRawFiles:
    def test_lists_files_for_single_day(self):
        mock_client = MagicMock()
        mock_obj = MagicMock()
        mock_obj.object_name = "raw/openweather/year=2025/month=06/day=15/file.json"
        mock_client.list_objects.return_value = [mock_obj]

        keys = list_raw_files(
            client=mock_client,
            bucket="test-bucket",
            date_start=datetime(2025, 6, 15),
            date_end=datetime(2025, 6, 15),
        )
        assert len(keys) == 1
        mock_client.list_objects.assert_called_once()

    def test_lists_files_for_date_range(self):
        mock_client = MagicMock()
        mock_client.list_objects.return_value = []

        keys = list_raw_files(
            client=mock_client,
            bucket="test-bucket",
            date_start=datetime(2025, 6, 15),
            date_end=datetime(2025, 6, 17),
        )
        assert keys == []
        assert mock_client.list_objects.call_count == 3

    def test_empty_bucket_returns_empty_list(self):
        mock_client = MagicMock()
        mock_client.list_objects.return_value = []

        keys = list_raw_files(
            client=mock_client,
            bucket="empty-bucket",
            date_start=datetime(2025, 1, 1),
            date_end=datetime(2025, 1, 1),
        )
        assert keys == []


class TestReprocess:
    @patch("replay.reprocess_pipeline._build_minio_client")
    @patch("replay.reprocess_pipeline.list_raw_files")
    def test_no_files_returns_no_data(self, mock_list, mock_build):
        mock_list.return_value = []

        result = reprocess(
            date_start=datetime(2025, 1, 1),
            date_end=datetime(2025, 1, 1),
            run_transforms=False,
        )
        assert result["status"] == "no_data"
        assert result["files_found"] == 0

    @patch("replay.reprocess_pipeline.run_dbt")
    @patch("replay.reprocess_pipeline.load_raw_to_postgres")
    @patch("replay.reprocess_pipeline.list_raw_files")
    @patch("replay.reprocess_pipeline._build_minio_client")
    def test_with_files_returns_success(self, mock_build, mock_list, mock_load, mock_dbt):
        mock_list.return_value = ["file1.json", "file2.json"]
        mock_load.return_value = 2

        result = reprocess(
            date_start=datetime(2025, 1, 1),
            date_end=datetime(2025, 1, 1),
            run_transforms=True,
        )
        assert result["status"] == "success"
        assert result["files_found"] == 2
        assert result["records_loaded"] == 2
        mock_dbt.assert_called_once()

    @patch("replay.reprocess_pipeline.load_raw_to_postgres")
    @patch("replay.reprocess_pipeline.list_raw_files")
    @patch("replay.reprocess_pipeline._build_minio_client")
    def test_skip_transforms(self, mock_build, mock_list, mock_load):
        mock_list.return_value = ["file.json"]
        mock_load.return_value = 1

        with patch("replay.reprocess_pipeline.run_dbt") as mock_dbt:
            result = reprocess(
                date_start=datetime(2025, 1, 1),
                date_end=datetime(2025, 1, 1),
                run_transforms=False,
            )
            mock_dbt.assert_not_called()
        assert result["status"] == "success"
