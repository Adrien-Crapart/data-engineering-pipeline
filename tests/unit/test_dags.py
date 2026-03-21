"""Tests for Airflow DAG parsing and validation.

Ensures all DAGs can be parsed without import errors and follow conventions.
"""

from __future__ import annotations

from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[2] / "orchestration" / "airflow" / "dags"


@pytest.mark.unit
class TestDagFilesExist:
    def test_dags_directory_exists(self):
        assert DAGS_DIR.exists(), f"DAGs directory not found: {DAGS_DIR}"

    def test_weather_pipeline_dag_exists(self):
        assert (DAGS_DIR / "weather_pipeline_dag.py").exists()

    def test_no_replay_dag(self):
        assert not (DAGS_DIR / "replay_dag.py").exists(), "replay_dag.py should not exist"

    def test_no_metadata_ingestion_dag(self):
        assert not (DAGS_DIR / "metadata_ingestion_dag.py").exists(), (
            "metadata_ingestion_dag.py should not exist"
        )


@pytest.mark.unit
class TestDagParsing:
    """Verify DAG files are syntactically valid Python and follow conventions."""

    def test_dag_files_are_valid_python(self):
        for dag_file in DAGS_DIR.glob("*.py"):
            if dag_file.name.startswith("."):
                continue
            try:
                compile(dag_file.read_text(encoding="utf-8"), dag_file.name, "exec")
            except SyntaxError as exc:
                pytest.fail(f"Syntax error in {dag_file.name}: {exc}")

    def test_all_dag_files_have_docstrings(self):
        for dag_file in DAGS_DIR.glob("*.py"):
            if dag_file.name.startswith("."):
                continue
            content = dag_file.read_text(encoding="utf-8")
            assert '"""' in content or "'''" in content, (
                f"{dag_file.name} is missing a module-level docstring"
            )

    def test_weather_pipeline_uses_docker_operator(self):
        content = (DAGS_DIR / "weather_pipeline_dag.py").read_text(encoding="utf-8")
        assert "DockerOperator" in content, "weather_pipeline must use DockerOperator"
        assert "BashOperator" not in content, "weather_pipeline must not use BashOperator"

    def test_weather_pipeline_has_separate_images(self):
        content = (DAGS_DIR / "weather_pipeline_dag.py").read_text(encoding="utf-8")
        assert "weather-pipeline-dlt" in content
        assert "weather-pipeline-dbt" in content
        assert "weather-pipeline-soda" in content

    def test_weather_pipeline_uses_mount_objects(self):
        content = (DAGS_DIR / "weather_pipeline_dag.py").read_text(encoding="utf-8")
        assert "from docker.types import Mount" in content
        assert "Mount(" in content
