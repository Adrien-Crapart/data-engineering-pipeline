# Orchestration

Apache Airflow DAGs for pipeline orchestration.

## Purpose

Airflow acts as the **orchestrator only** — it never runs processing. Every
processing step (dlt ingestion, dbt transformations, Soda quality checks,
Elementary reports) runs in its own isolated Docker container via
`DockerOperator`.

## Structure

```
orchestration/
└── airflow/
    ├── dags/
    │   └── weather_pipeline_dag.py  — Single end-to-end pipeline DAG
    └── plugins/                     — Custom Airflow plugins (reserved)
```

## DAG: `weather_pipeline`

| Property | Value |
|----------|-------|
| Schedule | Every 6 hours (`0 */6 * * *`) |
| Max active runs | 1 |
| Catchup | Disabled |
| Retries | 2 (5-minute delay) |

### Task Graph

```
extract_weather (dlt container)
  → dbt_deps (dbt container)
  → dbt_run (dbt container)
  → dbt_test (dbt container)
  → soda_scan (soda container)
  → elementary_report (dbt container)
  → push_metrics (Airflow TaskFlow)
```

### Docker Images

| Task | Image | Purpose |
|------|-------|---------|
| extract_weather | `weather-pipeline-dlt:1.0.0` | API ingestion + MinIO archive |
| dbt_deps/run/test | `weather-pipeline-dbt:1.0.0` | Transformations + Elementary |
| soda_scan | `weather-pipeline-soda:1.0.0` | Data quality checks |

### Re-run from any task

Use the Airflow UI **Clear** functionality to re-run the pipeline from any
failed task. No separate replay DAG is needed.

## Metadata Ingestion

Metadata ingestion into OpenMetadata is handled by the **official
`openmetadata/ingestion` container** (separate from this Airflow instance).
See `metadata/README.md` for details.
