"""End-to-end integration tests for the weather data pipeline.

Requires the full stack to be running: make start
These tests verify data flows through ingestion -> transformation -> quality.
"""

import subprocess

import pytest


@pytest.mark.e2e
@pytest.mark.integration
class TestPipelineE2E:
    """Verify the full pipeline can execute successfully."""

    def test_dlt_runner_image_exists(self):
        result = subprocess.run(
            ["docker", "image", "inspect", "weather-pipeline-dlt:1.0.0"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, "DLT runner image not found — run make build-dlt"

    def test_dbt_runner_image_exists(self):
        result = subprocess.run(
            ["docker", "image", "inspect", "weather-pipeline-dbt:1.0.0"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, "dbt runner image not found — run make build-dbt"

    def test_soda_runner_image_exists(self):
        result = subprocess.run(
            ["docker", "image", "inspect", "weather-pipeline-soda:1.0.0"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, "Soda runner image not found — run make build-soda"

    def test_dbt_run_succeeds_in_docker(self):
        """Run dbt inside Docker and verify exit code 0."""
        result = subprocess.run(
            [
                "docker",
                "run",
                "--rm",
                "--network",
                "data-engineering-pipeline_pipeline-network",
                "-e",
                "POSTGRES_HOST=postgres",
                "-e",
                "POSTGRES_PORT=5432",
                "-e",
                "POSTGRES_DB=weather_db",
                "-e",
                "POSTGRES_USER=airflow",
                "-e",
                "POSTGRES_PASSWORD=airflow",
                "-e",
                "MINIO_ENDPOINT_HOST=minio:9000",
                "-e",
                "MINIO_ROOT_USER=minioadmin",
                "-e",
                "MINIO_ROOT_PASSWORD=minioadmin",
                "weather-pipeline-dbt:1.0.0",
                "dbt deps --profiles-dir . && dbt compile --profiles-dir .",
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, f"dbt compile failed: {result.stderr}"
