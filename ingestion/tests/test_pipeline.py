"""Unit tests for the OpenWeather ingestion pipeline (mocked API calls)."""

from unittest.mock import MagicMock, patch

import pytest

from ingestion.config import PipelineConfig
from ingestion.openweather_pipeline import _fetch_with_retry, openweather_source

SAMPLE_CURRENT_WEATHER = {
    "coord": {"lon": 2.3488, "lat": 48.8534},
    "weather": [{"id": 800, "main": "Clear", "description": "clear sky", "icon": "01d"}],
    "main": {"temp": 15.2, "feels_like": 14.1, "humidity": 65, "pressure": 1013},
    "wind": {"speed": 3.5, "deg": 180},
    "dt": 1709827200,
    "sys": {"country": "FR"},
    "name": "Paris",
    "cod": 200,
}

SAMPLE_FORECAST = {
    "city": {"name": "Paris", "country": "FR"},
    "list": [
        {
            "dt": 1709827200,
            "main": {"temp": 15.2, "humidity": 65, "pressure": 1013},
            "weather": [{"main": "Clear", "description": "clear sky"}],
            "wind": {"speed": 3.5},
        }
    ],
    "cnt": 1,
}


def _make_config(cities: list[str] | None = None) -> PipelineConfig:
    return PipelineConfig(
        api_key="fake_key",
        cities=cities or ["Paris", "Lyon"],
    )


class TestFetchWithRetry:
    """Validate retry behaviour on the HTTP helper."""

    @patch("ingestion.openweather_pipeline.requests.get")
    def test_success_on_first_attempt(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"result": "ok"}
        mock_resp.raise_for_status.return_value = None
        mock_get.return_value = mock_resp

        result = _fetch_with_retry("http://example.com", {})
        assert result == {"result": "ok"}
        assert mock_get.call_count == 1

    @patch("ingestion.openweather_pipeline.time.sleep")
    @patch("ingestion.openweather_pipeline.requests.get")
    def test_retries_on_failure_then_succeeds(self, mock_get, mock_sleep):
        import requests

        fail = MagicMock()
        fail.raise_for_status.side_effect = requests.RequestException("500")

        success = MagicMock()
        success.json.return_value = {"result": "ok"}
        success.raise_for_status.return_value = None

        mock_get.side_effect = [fail, success]

        result = _fetch_with_retry("http://example.com", {}, retries=3)
        assert result == {"result": "ok"}
        assert mock_get.call_count == 2

    @patch("ingestion.openweather_pipeline.time.sleep")
    @patch("ingestion.openweather_pipeline.requests.get")
    def test_raises_after_all_retries_exhausted(self, mock_get, mock_sleep):
        import requests

        fail = MagicMock()
        fail.raise_for_status.side_effect = requests.RequestException("timeout")
        mock_get.return_value = fail

        with pytest.raises(requests.RequestException):
            _fetch_with_retry("http://example.com", {}, retries=2)
        assert mock_get.call_count == 2


class TestOpenweatherSource:
    """Validate dlt source yields correct data."""

    @patch("ingestion.openweather_pipeline._fetch_with_retry")
    def test_current_weather_yields_one_per_city(self, mock_fetch):
        mock_fetch.return_value = SAMPLE_CURRENT_WEATHER
        config = _make_config(["Paris", "Lyon"])
        source = openweather_source(config)

        records = list(source.resources["current_weather"])
        assert len(records) == 2
        assert all("_extraction_city" in r for r in records)

    @patch("ingestion.openweather_pipeline._fetch_with_retry")
    def test_forecast_yields_one_per_city(self, mock_fetch):
        mock_fetch.return_value = SAMPLE_FORECAST
        config = _make_config(["Paris"])
        source = openweather_source(config)

        records = list(source.resources["weather_forecast"])
        assert len(records) == 1
        assert records[0]["_extraction_city"] == "Paris"

    @patch("ingestion.openweather_pipeline._fetch_with_retry")
    def test_source_continues_on_city_error(self, mock_fetch):
        import requests

        mock_fetch.side_effect = [
            requests.RequestException("API error"),
            SAMPLE_CURRENT_WEATHER,
        ]
        config = _make_config(["FailCity", "Paris"])
        source = openweather_source(config)

        records = list(source.resources["current_weather"])
        assert len(records) == 1
        assert records[0]["name"] == "Paris"
