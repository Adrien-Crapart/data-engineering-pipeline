# Airflow Standards

Applies to: `orchestration/**`, `**/dags/**`

## Orchestration vs Processing

Airflow is an **orchestrator only** — never a processing engine.

- **NEVER** run dbt, Soda, Great Expectations, or data transformations inside workers via `BashOperator` or `PythonOperator`.
- Use `DockerOperator` for all processing tasks. This ensures isolation, reproducibility, and no resource contention on the scheduler/worker.
- All Docker image names and Airflow Asset URIs are centralized in `orchestration/plugins/constants.py` — always import from there.

## DockerOperator Pattern

```python
DockerOperator(
    task_id="dbt_transform",
    image=DBT_IMAGE,          # from constants.py
    command=["dbt", "run"],
    environment={...},
    mem_limit=os.getenv("DBT_MEM_LIMIT", "512m"),
    network_mode="data-engineering-pipeline_pipeline-network",
    mount_tmp_dir=False,
    auto_remove="force",
    pool="docker_pool",
)
```

## DAG Mandatory Fields

Every DAG must have:

```python
default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

dag = DAG(
    dag_id="...",
    schedule=...,
    max_active_runs=1,   # always 1 for data pipelines
    catchup=False,       # unless backfill is explicitly needed
    tags=["..."],
    doc_md="""...""",
)
```

## Asset-Driven Chain (Airflow 3.x)

```python
# Producer
from airflow.sdk import Asset
MY_ASSET = Asset(name="staging_weather", uri="s3://...")  # name is first arg, uri is keyword

@task(outlets=[MY_ASSET])
def finalize(): ...

# Consumer
@dag(schedule=[MY_ASSET])
def downstream_dag(): ...
```

**Never** pass URI as a positional arg — causes `multiple values for argument 'name'`.

## Logging & Variables

- Use `logging` module, never `print()`.
- Never call `Variable.get()` at module level (parse-time penalty); call inside task functions only.
- Pool assignments: `docker_pool` for DockerOperator tasks, `database_pool` for DB tasks, `api_pool` for external API calls.
- Logs stored remotely on MinIO (`airflow-logs` bucket) — never rely on local disk.
