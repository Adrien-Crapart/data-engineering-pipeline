"""Unit tests for the ingestion CLI entry point."""

from unittest.mock import MagicMock, patch

import pytest

from ingestion.cli import PIPELINES, _parse_args, main


class TestParseArgs:
    """Validate CLI argument parsing."""

    def test_default_pipeline(self):
        args = _parse_args([])
        assert args.pipeline == "openweather"
        assert args.cities is None
        assert args.full_refresh is False

    def test_explicit_pipeline(self):
        args = _parse_args(["--pipeline", "openweather"])
        assert args.pipeline == "openweather"

    def test_cities_override(self):
        args = _parse_args(["--cities", "Berlin,London"])
        assert args.cities == "Berlin,London"

    def test_full_refresh_flag(self):
        args = _parse_args(["--full-refresh"])
        assert args.full_refresh is True

    def test_invalid_pipeline_rejected(self):
        with pytest.raises(SystemExit):
            _parse_args(["--pipeline", "nonexistent"])


class TestPipelinesRegistry:
    """Validate pipeline module registry."""

    def test_openweather_registered(self):
        assert "openweather" in PIPELINES
        assert PIPELINES["openweather"] == "ingestion.pipelines.openweather_pipeline"


class TestMain:
    """Validate main() execution flow."""

    @patch("ingestion.cli.importlib.import_module")
    def test_main_calls_run_pipeline(self, mock_import):
        mock_module = MagicMock()
        mock_module.run_pipeline.return_value = {"status": "success"}
        mock_import.return_value = mock_module

        exit_code = main(["--pipeline", "openweather"])

        assert exit_code == 0
        mock_import.assert_called_once_with("ingestion.pipelines.openweather_pipeline")
        mock_module.run_pipeline.assert_called_once()

    @patch("ingestion.cli.importlib.import_module")
    def test_main_returns_1_on_failure(self, mock_import):
        mock_module = MagicMock()
        mock_module.run_pipeline.side_effect = RuntimeError("boom")
        mock_import.return_value = mock_module

        exit_code = main(["--pipeline", "openweather"])

        assert exit_code == 1

    @patch("ingestion.cli.importlib.import_module")
    @patch.dict("os.environ", {}, clear=False)
    def test_cities_override_sets_env(self, mock_import):
        import os

        mock_module = MagicMock()
        mock_module.run_pipeline.return_value = {"status": "success"}
        mock_import.return_value = mock_module

        main(["--cities", "Berlin,London"])

        assert os.environ.get("WEATHER_CITIES") == "Berlin,London"

    @patch("ingestion.cli.importlib.import_module")
    def test_import_error_returns_1(self, mock_import):
        mock_import.side_effect = ImportError("no module")

        exit_code = main(["--pipeline", "openweather"])

        assert exit_code == 1
