Scaffold a new Airflow DAG following project conventions. Ask the user for the DAG name and purpose if not provided.

## Information to gather (ask if missing)

- DAG name/ID (e.g., `forecast_pipeline_dag`)
- Trigger: schedule (cron), event-driven (which Asset), or manual
- What processing tasks it needs (DLT extract, dbt run, Soda check, etc.)
- What Asset it emits (if any)
- Which existing Asset it consumes (if event-driven)

## Steps

1. Read `orchestration/dags/ingestion_pipeline_dag.py` to understand the existing DAG pattern.
2. Read `orchestration/plugins/constants.py` to check existing Asset names, image constants, and pool names.
3. Create the new DAG file in `orchestration/dags/<dag_id>.py` using this structure:

```python
"""
<dag_id> — <one-line description>
"""
import logging
from datetime import timedelta
from airflow.sdk import DAG, Asset, task
from airflow.providers.docker.operators.docker import DockerOperator
from plugins.constants import PIPELINE_NETWORK, DOCKER_POOL, <IMAGE_CONSTANTS>

log = logging.getLogger(__name__)

# Assets (import or define here)
MY_INPUT_ASSET = Asset(name="...", uri="s3://...")
MY_OUTPUT_ASSET = Asset(name="...", uri="s3://...")

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="<dag_id>",
    schedule=[MY_INPUT_ASSET],   # or cron string, or None
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    tags=["<layer>", "<domain>"],
    doc_md="""
    ## <DAG Name>
    <Description of what this DAG does and why.>

    **Trigger**: <Asset or schedule>
    **Emits**: <Asset>
    **Layers**: <Bronze/Silver/Gold>
    """,
) as dag:
    # tasks here...
    pass
```

4. Add a DAG convention test in `tests/unit/test_dags.py` for the new DAG.
5. If the DAG emits a new Asset, add it to `orchestration/plugins/constants.py`.
6. Update `docs/pipeline.md` with the new DAG description.
7. Add OpenMetadata provisioning if needed (see `.claude/rules/openmetadata.md`).

## Validation

```bash
just check
uv run pytest tests/unit/test_dags.py -v
```
