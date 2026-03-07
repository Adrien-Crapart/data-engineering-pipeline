"""Unit tests for Great Expectations integration."""

import great_expectations as gx
import pandas as pd
import pytest
from great_expectations.core.expectation_suite import ExpectationSuite

from data_quality.expectations.weather_expectations import GXConfig


class TestGXConfig:
    """Validate Great Expectations configuration."""

    def test_default_values(self):
        config = GXConfig()
        assert config.postgres_host == "postgres"
        assert config.postgres_port == 5432
        assert config.postgres_db == "weather_db"

    def test_connection_string_format(self):
        config = GXConfig(
            postgres_host="myhost",
            postgres_port=5433,
            postgres_db="testdb",
            postgres_user="user",
            postgres_password="pass",
        )
        assert config.connection_string == "postgresql+psycopg2://user:pass@myhost:5433/testdb"


class TestGXExpectationsInMemory:
    """Test GX expectations using in-memory DataFrames (no PostgreSQL needed)."""

    @pytest.fixture()
    def sample_weather_df(self) -> pd.DataFrame:
        return pd.DataFrame({
            "city_name": ["Paris", "Lyon", "Marseille"],
            "temperature_celsius": [15.2, 12.8, 18.5],
            "humidity_percent": [65.0, 72.0, 55.0],
            "wind_speed_ms": [3.5, 2.1, 5.8],
            "measured_at": pd.to_datetime(["2025-01-01", "2025-01-01", "2025-01-01"]),
        })

    @pytest.fixture()
    def invalid_weather_df(self) -> pd.DataFrame:
        return pd.DataFrame({
            "city_name": ["Paris", None, "Marseille"],
            "temperature_celsius": [15.2, 999.0, 18.5],
            "humidity_percent": [65.0, 150.0, -10.0],
            "wind_speed_ms": [3.5, 2.1, 5.8],
            "measured_at": pd.to_datetime(["2025-01-01", "2025-01-01", "2025-01-01"]),
        })

    def _get_batch(self, df: pd.DataFrame):
        context = gx.get_context()
        ds = context.data_sources.add_pandas("test_pandas")
        asset = ds.add_dataframe_asset(name="test_asset")
        batch_def = asset.add_batch_definition_whole_dataframe("test_batch")
        return batch_def.get_batch(batch_parameters={"dataframe": df})

    def test_valid_temperature_range(self, sample_weather_df):
        batch = self._get_batch(sample_weather_df)
        result = batch.validate(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="temperature_celsius", min_value=-60.0, max_value=60.0
            )
        )
        assert result.success is True

    def test_invalid_temperature_detected(self, invalid_weather_df):
        batch = self._get_batch(invalid_weather_df)
        result = batch.validate(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="temperature_celsius", min_value=-60.0, max_value=60.0
            )
        )
        assert result.success is False

    def test_valid_humidity_range(self, sample_weather_df):
        batch = self._get_batch(sample_weather_df)
        result = batch.validate(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="humidity_percent", min_value=0.0, max_value=100.0
            )
        )
        assert result.success is True

    def test_invalid_humidity_detected(self, invalid_weather_df):
        batch = self._get_batch(invalid_weather_df)
        result = batch.validate(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="humidity_percent", min_value=0.0, max_value=100.0
            )
        )
        assert result.success is False

    def test_not_null_city_name(self, sample_weather_df):
        batch = self._get_batch(sample_weather_df)
        result = batch.validate(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="city_name")
        )
        assert result.success is True

    def test_null_city_detected(self, invalid_weather_df):
        batch = self._get_batch(invalid_weather_df)
        result = batch.validate(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="city_name")
        )
        assert result.success is False

    def test_row_count_check(self, sample_weather_df):
        batch = self._get_batch(sample_weather_df)
        result = batch.validate(
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)
        )
        assert result.success is True

    def test_column_exists(self, sample_weather_df):
        batch = self._get_batch(sample_weather_df)
        result = batch.validate(
            gx.expectations.ExpectColumnToExist(column="temperature_celsius")
        )
        assert result.success is True

    def test_suite_with_multiple_expectations(self, sample_weather_df):
        context = gx.get_context()
        suite = context.suites.add(ExpectationSuite(name="test_suite"))

        suite.add_expectation(
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(column="city_name")
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="temperature_celsius", min_value=-60.0, max_value=60.0
            )
        )

        ds = context.data_sources.add_pandas("suite_test_pandas")
        asset = ds.add_dataframe_asset(name="suite_test_asset")
        batch_def = asset.add_batch_definition_whole_dataframe("suite_batch")

        from great_expectations.core.validation_definition import ValidationDefinition

        val_def = context.validation_definitions.add(
            ValidationDefinition(name="suite_test_val", data=batch_def, suite=suite)
        )

        checkpoint = context.checkpoints.add(
            gx.checkpoint.checkpoint.Checkpoint(
                name="suite_test_cp", validation_definitions=[val_def]
            )
        )

        result = checkpoint.run(batch_parameters={"dataframe": sample_weather_df})
        assert result.success is True
