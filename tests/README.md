# Tests

Unit and integration tests for all pipeline components.

## Purpose

Ensures correctness and reliability of all Python modules through automated
testing. Every module in the project must have corresponding tests here.

## Structure

```
tests/
├── unit/
│   ├── test_config.py              — PipelineConfig validation
│   ├── test_pipeline.py            — OpenWeather ingestion pipeline
│   ├── test_duckdb_analytics.py    — DuckDB analytics engine
│   ├── test_great_expectations.py  — Great Expectations suites
│   ├── test_contracts.py           — Contract validator
│   ├── test_minio_client.py        — MinIO DataLakeClient
│   ├── test_metrics_exporter.py    — Prometheus metrics exporter
│   ├── test_replay.py              — Replay pipeline
│   └── test_dags.py                — Airflow DAG parsing validation
└── integration/
    └── (reserved for end-to-end tests with running services)
```

## Running Tests

```bash
# All tests
pytest tests/ -v --tb=short

# Unit tests only
pytest tests/unit/ -v

# Inside Docker
make test
```

## Conventions

- Mock all external dependencies (APIs, databases, MinIO, Docker).
- Use `pytest` as the test runner.
- Test file naming: `test_<module_name>.py`.
