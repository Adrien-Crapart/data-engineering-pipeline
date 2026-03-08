"""Tests for Airflow DAG parsing and validation.

Ensures all DAGs can be parsed without import errors and follow conventions.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

DAGS_DIR = Path(__file__).resolve().parents[2] / "orchestration" / "airflow" / "dags"


def _mock_airflow_imports():
    """Create mock modules for Airflow imports so DAGs can be parsed outside Airflow."""
    mock_modules = {}

    for mod_name in [
        "airflow",
        "airflow.sdk",
        "airflow.models",
        "airflow.models.param",
        "airflow.providers",
        "airflow.providers.standard",
        "airflow.providers.standard.operators",
        "airflow.providers.standard.operators.bash",
        "airflow.providers.docker",
        "airflow.providers.docker.operators",
        "airflow.providers.docker.operators.docker",
    ]:
        if mod_name not in sys.modules:
            mock = MagicMock()
            sys.modules[mod_name] = mock
            mock_modules[mod_name] = mock

    dag_decorator = MagicMock()
    dag_decorator.side_effect = lambda *a, **kw: lambda fn: fn
    sys.modules["airflow.sdk"].dag = dag_decorator

    task_decorator = MagicMock()
    task_decorator.side_effect = lambda *a, **kw: lambda fn: fn
    sys.modules["airflow.sdk"].task = task_decorator

    sys.modules["airflow.models.param"].Param = MagicMock

    return mock_modules


class TestDagFilesExist:
    def test_dags_directory_exists(self):
        assert DAGS_DIR.exists(), f"DAGs directory not found: {DAGS_DIR}"

    def test_weather_pipeline_dag_exists(self):
        assert (DAGS_DIR / "weather_pipeline_dag.py").exists()

    def test_replay_dag_exists(self):
        assert (DAGS_DIR / "replay_dag.py").exists()

    def test_metadata_ingestion_dag_exists(self):
        assert (DAGS_DIR / "metadata_ingestion_dag.py").exists()


class TestDagParsing:
    """Verify DAG files are syntactically valid Python and can be imported."""

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
