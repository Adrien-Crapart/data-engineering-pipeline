# Skill: Add a New Data Source

End-to-end playbook for adding a new external data source to the pipeline. Follow this exactly — it covers all 8 layers that must be updated.

## Layer 1 — Data Contract

Create `contracts/<source>_contract.yaml`:

```yaml
name: <source>_contract
version: "1.0"
description: "Contract for <source> API data"
schemas:
  <table_name>:
    columns:
      - name: id
        type: string
        required: true
      - name: value
        type: float
        required: true
        constraints:
          min: 0
          max: 1000
      - name: recorded_at
        type: datetime
        required: true
```

Update `contracts/validator.py` to load and validate the new contract.

## Layer 2 — Ingestion Pipeline (Bronze)

Create `ingestion/pipelines/<source>_pipeline.py`:

```python
import dlt
from contracts.validator import validate_contract

@dlt.resource(name="<table_name>", write_disposition="append")
def fetch_<source>():
    data = call_api()
    validated = validate_contract(data, "<source>_contract")
    yield validated

pipeline = dlt.pipeline(
    pipeline_name="<source>_pipeline",
    destination=dlt.destinations.filesystem(bucket_url="s3://data-lake"),
    dataset_name="raw/<source>",
)
```

Update `ingestion/config.py` with new `PipelineConfig` fields if needed.

## Layer 3 — Staging Model (Silver)

Create `transformations/models/staging/stg_<source>.sql` — reads S3 Parquet, casts types, rejects invalid rows:

```sql
{{ config(materialized='view') }}

SELECT
    cast(id as varchar)           as id,
    cast(value as double)         as value,
    cast(recorded_at as timestamp) as recorded_at
FROM read_parquet('s3://data-lake/raw/<source>/**/*.parquet')
WHERE value IS NOT NULL AND value >= 0
```

Add schema tests to `transformations/models/staging/_models.yml`.

## Layer 4 — Core/Mart Models (Gold)

Create business-logic models in `transformations/models/marts/`. Follow the DuckDB ATTACH pattern — `+database: pg` must be set in `dbt_project.yml` for these models.

## Layer 5 — Quality Checks

Add Soda checks in `data_quality/soda/checks/staging_checks.yml`:
```yaml
checks for staging.stg_<source>:
  - row_count > 0
  - missing_count(id) = 0
  - freshness(recorded_at) < 7h
```

Add Great Expectations suite in `data_quality/expectations/weather_expectations.py`.

## Layer 6 — Orchestration

Add extraction task to `orchestration/dags/ingestion_pipeline_dag.py`:

```python
extract_<source> = DockerOperator(
    task_id="extract_<source>",
    image=DLT_IMAGE,
    command=["python", "-m", "ingestion.pipelines.<source>_pipeline"],
    ...
)
```

If this is an independent pipeline, create a new DAG using `/new-dag`.

## Layer 7 — Tests

Add unit tests in `tests/unit/test_<source>.py`. At minimum test:
- Contract validation (valid and invalid payloads)
- Config dataclass with new fields
- Pipeline output schema

## Layer 8 — OpenMetadata

Follow `.claude/rules/openmetadata.md`:
1. Add `TABLE_TESTS`, `TABLE_LINEAGE`, `TABLE_CONFIG`, `TABLE_CONTRACTS`, column descriptions in `metadata/om_setup_full.py`
2. Run `just om-provision`

## Validation Checklist

```bash
just check                    # lint
just test                     # unit tests
just dlt-run                  # end-to-end ingestion
just dbt-run && just dbt-test # transformations
just soda-check && just gx-check # quality gates
just om-provision             # metadata catalog
just review                   # full self-review
```
