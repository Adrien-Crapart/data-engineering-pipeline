"""Unit tests for pipeline configuration."""

import os
from unittest.mock import patch

import pytest

from ingestion.config import PipelineConfig


class TestPipelineConfig:
    """Validate environment-based configuration loading."""

    def test_from_env_with_valid_values(self):
        env = {
            "OPENWEATHER_API_KEY": "test_key_123",
            "WEATHER_CITIES": "Paris,London,Tokyo",
        }
        with patch.dict(os.environ, env, clear=False):
            config = PipelineConfig.from_env()
            assert config.api_key == "test_key_123"
            assert config.cities == ["Paris", "London", "Tokyo"]

    def test_from_env_raises_on_missing_api_key(self):
        with patch.dict(os.environ, {"OPENWEATHER_API_KEY": ""}, clear=False):
            with pytest.raises(ValueError, match="OPENWEATHER_API_KEY"):
                PipelineConfig.from_env()

    def test_from_env_raises_on_placeholder_api_key(self):
        with patch.dict(os.environ, {"OPENWEATHER_API_KEY": "your_api_key_here"}, clear=False):
            with pytest.raises(ValueError, match="OPENWEATHER_API_KEY"):
                PipelineConfig.from_env()

    def test_from_env_raises_on_empty_cities(self):
        env = {"OPENWEATHER_API_KEY": "test_key_123", "WEATHER_CITIES": ""}
        with patch.dict(os.environ, env, clear=False):
            with pytest.raises(ValueError, match="WEATHER_CITIES"):
                PipelineConfig.from_env()

    def test_cities_strips_whitespace(self):
        env = {
            "OPENWEATHER_API_KEY": "test_key_123",
            "WEATHER_CITIES": " Paris , Lyon , Marseille ",
        }
        with patch.dict(os.environ, env, clear=False):
            config = PipelineConfig.from_env()
            assert config.cities == ["Paris", "Lyon", "Marseille"]

    def test_postgres_dsn_format(self):
        config = PipelineConfig(
            api_key="key",
            cities=["Paris"],
            postgres_user="user",
            postgres_password="pass",
            postgres_host="localhost",
            postgres_port=5432,
            postgres_db="testdb",
        )
        assert config.postgres_dsn == "postgresql://user:pass@localhost:5432/testdb"

    def test_default_values(self):
        config = PipelineConfig(api_key="key", cities=["Paris"])
        assert config.base_url == "https://api.openweathermap.org/data/2.5"
        assert config.units == "metric"
