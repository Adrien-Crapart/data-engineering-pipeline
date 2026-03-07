"""
Great Expectations - Weather Data Quality Suites.

Defines expectation suites for validating weather pipeline data at each layer:
  - Raw: basic schema and freshness checks
  - Staging: data type, range, and completeness checks
  - Analytics: aggregation integrity checks

Usage:
    from data_quality.expectations.weather_expectations import WeatherExpectations

    validator = WeatherExpectations.from_env()
    results = validator.validate_staging()
    print(results)
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_definition import ValidationDefinition

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GXConfig:
    """Configuration for Great Expectations PostgreSQL connection."""

    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "weather_db"
    postgres_user: str = "airflow"
    postgres_password: str = "airflow"

    @classmethod
    def from_env(cls) -> GXConfig:
        return cls(
            postgres_host=os.getenv("POSTGRES_HOST", "postgres"),
            postgres_port=int(os.getenv("POSTGRES_PORT", "5432")),
            postgres_db=os.getenv("POSTGRES_DB", "weather_db"),
            postgres_user=os.getenv("POSTGRES_USER", "airflow"),
            postgres_password=os.getenv("POSTGRES_PASSWORD", "airflow"),
        )

    @property
    def connection_string(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


class WeatherExpectations:
    """Great Expectations validation engine for weather pipeline data."""

    def __init__(self, config: GXConfig | None = None) -> None:
        self.config = config or GXConfig.from_env()
        self.context = gx.get_context()
        self._setup_datasource()

    def _setup_datasource(self) -> None:
        """Configure PostgreSQL data source."""
        self.datasource = self.context.data_sources.add_postgres(
            "weather_db",
            connection_string=self.config.connection_string,
        )

    def _create_staging_current_suite(self) -> ExpectationSuite:
        """Create expectation suite for stg_weather_current."""
        suite = self.context.suites.add(
            ExpectationSuite(name="staging_current_weather")
        )

        suite.add_expectation(
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column="city_name")
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column="temperature_celsius")
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column="humidity_percent")
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column="wind_speed_ms")
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column="measured_at")
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="city_name", severity="critical"
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="temperature_celsius", severity="critical"
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="measured_at", severity="critical"
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="temperature_celsius",
                min_value=-60.0,
                max_value=60.0,
                severity="warning",
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="humidity_percent",
                min_value=0.0,
                max_value=100.0,
                severity="warning",
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="wind_speed_ms",
                min_value=0.0,
                max_value=200.0,
                severity="warning",
            )
        )
        return suite

    def _create_staging_forecast_suite(self) -> ExpectationSuite:
        """Create expectation suite for stg_weather_forecast."""
        suite = self.context.suites.add(
            ExpectationSuite(name="staging_weather_forecast")
        )

        suite.add_expectation(
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column="city_name")
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column="temperature_celsius")
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column="forecast_at")
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="city_name", severity="critical"
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="temperature_celsius", severity="critical"
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="temperature_celsius",
                min_value=-60.0,
                max_value=60.0,
                severity="warning",
            )
        )
        return suite

    def _create_analytics_suite(self) -> ExpectationSuite:
        """Create expectation suite for analytics.weather_daily_summary."""
        suite = self.context.suites.add(
            ExpectationSuite(name="analytics_daily_summary")
        )

        suite.add_expectation(
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column="city_name")
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column="date_day")
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="city_name", severity="critical"
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="date_day", severity="critical"
            )
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="observation_count",
                min_value=1,
                severity="warning",
            )
        )
        return suite

    def validate_staging_current(self) -> dict[str, Any]:
        """Validate stg_weather_current table."""
        suite = self._create_staging_current_suite()

        asset = self.datasource.add_table_asset(
            name="stg_weather_current",
            table_name="stg_weather_current",
            schema_name="staging",
        )
        batch_def = asset.add_batch_definition_whole_table("stg_current_batch")

        validation_def = self.context.validation_definitions.add(
            ValidationDefinition(
                name="validate_stg_current",
                data=batch_def,
                suite=suite,
            )
        )

        checkpoint = self.context.checkpoints.add(
            gx.checkpoint.checkpoint.Checkpoint(
                name="staging_current_checkpoint",
                validation_definitions=[validation_def],
            )
        )

        result = checkpoint.run()
        logger.info("Staging current weather validation: success=%s", result.success)
        return {"success": result.success, "results": result.describe()}

    def validate_staging_forecast(self) -> dict[str, Any]:
        """Validate stg_weather_forecast table."""
        suite = self._create_staging_forecast_suite()

        asset = self.datasource.add_table_asset(
            name="stg_weather_forecast",
            table_name="stg_weather_forecast",
            schema_name="staging",
        )
        batch_def = asset.add_batch_definition_whole_table("stg_forecast_batch")

        validation_def = self.context.validation_definitions.add(
            ValidationDefinition(
                name="validate_stg_forecast",
                data=batch_def,
                suite=suite,
            )
        )

        checkpoint = self.context.checkpoints.add(
            gx.checkpoint.checkpoint.Checkpoint(
                name="staging_forecast_checkpoint",
                validation_definitions=[validation_def],
            )
        )

        result = checkpoint.run()
        logger.info("Staging forecast validation: success=%s", result.success)
        return {"success": result.success, "results": result.describe()}

    def validate_analytics(self) -> dict[str, Any]:
        """Validate analytics.weather_daily_summary table."""
        suite = self._create_analytics_suite()

        asset = self.datasource.add_table_asset(
            name="weather_daily_summary",
            table_name="weather_daily_summary",
            schema_name="analytics",
        )
        batch_def = asset.add_batch_definition_whole_table("analytics_batch")

        validation_def = self.context.validation_definitions.add(
            ValidationDefinition(
                name="validate_analytics",
                data=batch_def,
                suite=suite,
            )
        )

        checkpoint = self.context.checkpoints.add(
            gx.checkpoint.checkpoint.Checkpoint(
                name="analytics_checkpoint",
                validation_definitions=[validation_def],
            )
        )

        result = checkpoint.run()
        logger.info("Analytics validation: success=%s", result.success)
        return {"success": result.success, "results": result.describe()}

    def validate_all(self) -> dict[str, Any]:
        """Run all expectation suites and return combined results."""
        results = {}

        logger.info("Running Great Expectations validation suites...")

        try:
            results["staging_current"] = self.validate_staging_current()
        except Exception as e:
            logger.error("Staging current validation failed: %s", e)
            results["staging_current"] = {"success": False, "error": str(e)}

        try:
            results["staging_forecast"] = self.validate_staging_forecast()
        except Exception as e:
            logger.error("Staging forecast validation failed: %s", e)
            results["staging_forecast"] = {"success": False, "error": str(e)}

        try:
            results["analytics"] = self.validate_analytics()
        except Exception as e:
            logger.error("Analytics validation failed: %s", e)
            results["analytics"] = {"success": False, "error": str(e)}

        all_passed = all(r.get("success", False) for r in results.values())
        logger.info("All validations passed: %s", all_passed)

        return {"all_passed": all_passed, "suites": results}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("=== Great Expectations - Weather Data Quality ===\n")

    validator = WeatherExpectations()
    results = validator.validate_all()

    print(f"\nOverall: {'PASS' if results['all_passed'] else 'FAIL'}")
    for suite_name, result in results["suites"].items():
        status = "PASS" if result.get("success") else "FAIL"
        print(f"  {suite_name}: {status}")
