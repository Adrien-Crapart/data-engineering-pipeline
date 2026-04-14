# Testing Standards

Applies to: `tests/**`, `**/*test*.py`

## Test Location

| Type | Location | Naming |
|------|---------|--------|
| Unit tests | `tests/unit/test_<module>.py` | Mock all external deps (APIs, DBs, MinIO, Docker) |
| Integration tests | `tests/integration/test_<feature>.py` | Requires live `just up` stack |
| dbt tests | `transformations/tests/singular/*.sql` | Schema + data integrity |
| DAG validation | `tests/unit/test_dags.py` | All DAGs parsed; conventions checked |

## Mandatory Coverage

- Every new module → unit tests in `tests/unit/`.
- Every new DAG → parsing test entry in `tests/unit/test_dags.py`.
- New utility functions → at least one test per function.
- New dbt models → schema tests in `transformations/tests/` and `_models.yml`.

## Running Tests

```bash
just test                   # all unit tests
just test-unit              # unit only
just test-integration       # integration (requires running stack)
just test-cov               # with coverage report
uv run pytest tests/unit/test_contracts.py -v   # single file
uv run pytest -m unit       # by marker
uv run pytest -m integration
```

## pytest Markers

Defined in root `pyproject.toml`:

```toml
[tool.pytest.ini_options]
markers = [
    "unit: fast, no external deps",
    "integration: requires live Docker stack",
    "slow: long-running tests",
]
```

## Pre-Merge Checklist

1. `just check` — no lint/format errors.
2. `just test` — all unit tests pass.
3. `just build-all` — if any Docker image changed.
4. `just dbt-test` — if any dbt model changed.
5. `just review` — full self-review gate.
