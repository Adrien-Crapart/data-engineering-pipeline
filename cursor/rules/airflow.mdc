# Airflow Standards

## Orchestration vs Processing

- Airflow is an **orchestrator**, not a processing engine.
- **NEVER** run heavy processing (dbt, Soda, Great Expectations, data transformations) directly inside Airflow workers via BashOperator or PythonOperator.
- Use `DockerOperator` to run processing tasks in dedicated containers.
- This ensures isolation, reproducibility, and avoids resource contention on the Airflow scheduler/worker.

## DockerOperator Pattern

- Create a dedicated Docker image for processing tools (dbt, Soda, Elementary).
- DAG tasks that run dbt, Soda, or any transformation tool must use `DockerOperator`.
- Mount only the necessary volumes (dbt project, config files).
- Pass configuration via environment variables.

## DAG Best Practices

- Every DAG must have `doc_md` set with a clear docstring.
- Every DAG must define `default_args` with:
  - `retries` (at least 1)
  - `retry_delay`
  - `execution_timeout`
  - `owner`
- Set `max_active_runs=1` for data pipelines to avoid race conditions.
- Set `catchup=False` unless backfill behavior is explicitly needed.
- Use `tags` for filtering DAGs in the UI.

## Logging

- Use Python `logging` module, not `print()`.
- Airflow logs must be stored remotely (MinIO/S3) to avoid disk saturation.
