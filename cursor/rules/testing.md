# Testing Standards

## Mandatory Testing

- **Every new module** must have corresponding unit tests in `tests/unit/`.
- **Every new feature branch** must include tests before being merged.
- Tests must pass locally before any merge to `master`.

## Test Location

- Unit tests go in `tests/unit/test_<module_name>.py`.
- Integration tests go in `tests/integration/test_<feature>.py`.
- Tools that have their own test directories (e.g., `transformations/dbt/tests/`) keep tests there.
- Airflow DAGs must have a parsing/validation test in `tests/unit/test_dags.py`.

## Test Coverage Requirements

- All Python modules at the project root or in subpackages must be tested.
- New utility functions must have at least one test per function.
- Mock external dependencies (APIs, databases, MinIO, Docker) in unit tests.
- Use `pytest` as the test runner. Use `unittest.mock` or `pytest-mock` for mocking.

## Pre-Merge Checklist

1. Run `ruff check .` and `ruff format --check .` — no errors.
2. Run `pytest tests/ -v --tb=short` — all tests pass.
3. If Docker images changed, verify `docker compose build` succeeds.
4. If dbt models changed, verify `dbt compile` succeeds.
