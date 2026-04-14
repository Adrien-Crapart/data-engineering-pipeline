# Skill: Add a New Pipeline Layer

Playbook for adding a new layer to an existing pipeline (e.g., a new staging model, a new mart, or a new quality gate).

## Understand the Existing Layer Structure

Before starting, read:
- `transformations/dbt_project.yml` — model groups, materialization, `+database: pg`
- `transformations/profiles.yml` — DuckDB connection (S3 + PG ATTACH)
- `orchestration/plugins/constants.py` — existing Asset names and Docker image constants
- `orchestration/dags/transformation_pipeline_dag.py` — how Cosmos DbtTaskGroup is wired

## Medallion Layer Decision

| What you're adding | Layer | Schema | Materialization |
|-------------------|-------|--------|----------------|
| Clean/typed view of raw S3 data | Silver | `staging` | view (DuckDB reads S3) |
| Business domain entity | Gold — Core | `core` | table (written to PG via ATTACH) |
| BI-ready aggregation | Gold — Mart | `mart` | table (written to PG via ATTACH) |
| Analytical/BI export | Analytic | `analytic` | table or view |

## Adding a New Staging Model

1. Create `transformations/models/staging/stg_<name>.sql` using `read_parquet()` — no `ref()` to source tables.
2. Add `not_null`, `unique` tests in `transformations/models/staging/_models.yml`.
3. Add Soda check in `data_quality/soda/checks/staging_checks.yml`.
4. The DAG automatically picks up the new model via Cosmos (`dbt_staging` task group).

## Adding a New Core/Mart Model

1. Create `transformations/models/marts/<name>.sql` with `{{ config(materialized='table', schema='mart') }}`.
2. Reference staging models via `{{ ref('stg_<name>') }}`.
3. Ensure `+database: pg` is set for the `marts` folder in `dbt_project.yml`:
   ```yaml
   models:
     my_project:
       marts:
         +database: pg
         +materialized: table
   ```
4. Add singular SQL tests in `transformations/tests/singular/assert_<name>.sql`.
5. Add Soda check in `data_quality/soda/checks/mart_checks.yml`.
6. Add GX expectations in `data_quality/expectations/weather_expectations.py`.

## Adding a New Quality Gate

To add Gate 2.5 (between staging_validated and transformation):

1. Create a new DAG in `orchestration/dags/` triggered by an existing Asset.
2. Use DockerOperator for Soda and GX checks.
3. Emit a new Asset when all checks pass.
4. Update the downstream DAG to consume the new Asset instead of the old one.
5. Update `orchestration/plugins/constants.py` with the new Asset definition.
6. Add the new Asset to OpenMetadata lineage in `metadata/om_setup_full.py`.

## Connecting a New Layer to the Event Chain

```python
# In constants.py
NEW_VALIDATED_ASSET = Asset(name="new_layer_validated", uri="s3://data-lake/assets/new_layer_validated")

# In the producer DAG
@task(outlets=[NEW_VALIDATED_ASSET])
def finalize_new_layer():
    ...

# In the consumer DAG
@dag(schedule=[NEW_VALIDATED_ASSET])
def downstream_dag():
    ...
```

## Updating OpenMetadata

For every new model/layer added:
1. `metadata/om_setup_full.py`: add to `TABLE_TESTS`, `TABLE_LINEAGE`, `TABLE_CONTRACTS`, `add_column_descriptions()`
2. `metadata/om_bootstrap.py`: add to `TABLE_CONFIG` (tier, owner, domain)
3. Run `just om-provision`

## Validation

```bash
just dbt-run              # compile and run all models
just dbt-test             # schema + singular tests
just soda-check           # quality gates
just gx-check
uv run pytest tests/unit/ -v
just review               # full self-review before committing
```
