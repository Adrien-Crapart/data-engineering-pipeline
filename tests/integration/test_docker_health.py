"""Integration tests that verify Docker services are healthy.

Requires the core stack to be running: make start-warehouse start-lake
"""

import subprocess

import pytest


@pytest.mark.integration
class TestDockerHealth:
    """Verify core Docker containers are running and healthy."""

    def _container_is_healthy(self, service_name: str) -> bool:
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-p",
                "data-engineering-pipeline",
                "ps",
                "--format",
                "{{.Name}} {{.Health}}",
            ],
            capture_output=True,
            text=True,
        )
        for line in result.stdout.strip().splitlines():
            if service_name in line and "healthy" in line.lower():
                return True
        return False

    def _service_is_running(self, service_name: str) -> bool:
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-p",
                "data-engineering-pipeline",
                "ps",
                "--format",
                "{{.Name}} {{.State}}",
            ],
            capture_output=True,
            text=True,
        )
        for line in result.stdout.strip().splitlines():
            if service_name in line and "running" in line.lower():
                return True
        return False

    def test_postgres_is_healthy(self):
        assert self._container_is_healthy("postgres"), "PostgreSQL container is not healthy"

    def test_redis_is_healthy(self):
        assert self._container_is_healthy("redis"), "Redis container is not healthy"

    def test_minio_is_healthy(self):
        assert self._container_is_healthy("minio"), "MinIO container is not healthy"

    def test_postgres_accepts_connections(self):
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-p",
                "data-engineering-pipeline",
                "exec",
                "-T",
                "postgres",
                "pg_isready",
                "-U",
                "airflow",
                "-d",
                "weather_db",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"PostgreSQL not accepting connections: {result.stderr}"

    def test_weather_db_schemas_exist(self):
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-p",
                "data-engineering-pipeline",
                "exec",
                "-T",
                "postgres",
                "psql",
                "-U",
                "airflow",
                "-d",
                "weather_db",
                "-t",
                "-c",
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('staging','mart','core','analytic') ORDER BY 1;",
            ],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        output = result.stdout.strip()
        for schema in ["analytic", "core", "mart", "staging"]:
            assert schema in output, f"Schema '{schema}' not found in weather_db"
