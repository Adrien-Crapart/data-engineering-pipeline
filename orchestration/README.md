# Orchestration

Apache Airflow DAGs and plugins for pipeline orchestration.

## Purpose

Airflow acts as the **orchestrator** (not a processing engine) for all data
pipelines. It schedules, triggers, and monitors the execution of tasks that
run in dedicated Docker containers.

## Structure

```
orchestration/
└── airflow/
    ├── dags/
    │   ├── weather_pipeline_dag.py    — Main pipeline: ingest → transform → validate → observe
    │   ├── replay_dag.py              — Manual replay of historical data from MinIO
    │   └── metadata_ingestion_dag.py  — OpenMetadata catalog sync
    └── plugins/                       — Custom Airflow plugins (reserved)
```

## DAGs

| DAG ID | Schedule | Description |
|--------|----------|-------------|
| `weather_pipeline` | Every 6 hours | End-to-end weather data pipeline |
| `replay_pipeline` | Manual (parameterized) | Replay historical raw data from MinIO |
| `metadata_ingestion` | Daily at 06:00 UTC | Sync metadata catalog with OpenMetadata |

## Best Practices

- Airflow orchestrates; processing runs in Docker containers via `DockerOperator`.
- All DAGs have `doc_md`, retries, timeouts, and SLA definitions.
- Logs are stored remotely in MinIO to avoid disk saturation.
