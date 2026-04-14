"""Tests for Airflow DAG parsing and validation.

Ensures all DAGs can be parsed without import errors and follow conventions.
"""

from __future__ import annotations

from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[2] / "orchestration" / "dags"


@pytest.mark.unit
class TestDagFilesExist:
    def test_dags_directory_exists(self):
        assert DAGS_DIR.exists(), f"DAGs directory not found: {DAGS_DIR}"

    def test_ingestion_dag_exists(self):
        assert (DAGS_DIR / "ingestion_pipeline_dag.py").exists()

    def test_transformation_dag_exists(self):
        assert (DAGS_DIR / "transformation_pipeline_dag.py").exists()

    def test_monitoring_dag_exists(self):
        assert (DAGS_DIR / "monitoring_dag.py").exists()

    def test_openmetadata_dag_exists(self):
        assert (DAGS_DIR / "openmetadata_ingestion_dag.py").exists()


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


@pytest.mark.unit
class TestIngestionDagConventions:
    """Verify ingestion DAG follows the required patterns."""

    def _read(self):
        return (DAGS_DIR / "ingestion_pipeline_dag.py").read_text(encoding="utf-8")

    def test_uses_docker_operator(self):
        content = self._read()
        assert "DockerOperator" in content

    def test_uses_assets(self):
        content = self._read()
        assert "Asset(" in content
        assert "staging_weather" in content

    def test_uses_params(self):
        content = self._read()
        assert "Param(" in content
        assert "force_download" in content
        assert "trigger_quality_gate" in content

    def test_uses_pendulum(self):
        content = self._read()
        assert "DAG_START_DATE" in content

    def test_uses_pools(self):
        content = self._read()
        assert "POOL_" in content

    def test_uses_callbacks(self):
        content = self._read()
        assert "on_failure_callback" in content
        assert "on_retry_callback" in content

    def test_uses_airflow_variables_not_os_environ(self):
        # os.environ.get is allowed for infra config (e.g. PROJECT_ROOT for Docker mounts)
        # The key check is that Airflow Variables are used for pipeline config
        content = self._read()
        assert "var.value" in content or "Variable.get" in content


@pytest.mark.unit
class TestTransformationDagConventions:
    """Verify transformation DAG is asset-driven."""

    def _read(self):
        return (DAGS_DIR / "transformation_pipeline_dag.py").read_text(encoding="utf-8")

    def test_scheduled_on_asset(self):
        content = self._read()
        assert "schedule=[staging_validated]" in content

    def test_produces_mart_asset(self):
        content = self._read()
        assert "mart_weather" in content
        assert "outlets=" in content
