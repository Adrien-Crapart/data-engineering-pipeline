Scaffold a new dbt model (staging + mart) following the DuckDB ATTACH pattern. Ask for the model name and source if not provided.

## Information to gather (ask if missing)

- Model name (e.g., `weather_alerts`)
- Source layer: staging only, or staging + core + mart
- Source data: S3 Parquet path or existing staging model
- Business logic: what aggregations or transformations are needed
- Materialization: view (staging), table (core/mart)

## Steps

1. Read an existing staging model (e.g., `transformations/models/staging/stg_weather_current.sql`) for the pattern.
2. Read an existing mart model (e.g., `transformations/models/marts/weather_daily_summary.sql`) for the pattern.
3. Read `transformations/dbt_project.yml` to understand model groups and `+database: pg` config.

### Create the staging model

```sql
-- transformations/models/staging/stg_<name>.sql
{{ config(materialized='view') }}

SELECT
    cast(col_a as varchar)    as col_a,
    cast(col_b as double)     as col_b,
    cast(recorded_at as timestamp) as recorded_at
FROM read_parquet('s3://data-lake/raw/<path>/**/*.parquet')
WHERE col_b IS NOT NULL   -- Gate 1: reject obviously invalid rows
```

### Create the mart model

```sql
-- transformations/models/marts/<name>.sql
{{ config(materialized='table', schema='mart') }}

SELECT
    city_name,
    date_trunc('day', recorded_at) as day,
    avg(col_b)                     as avg_col_b
FROM {{ ref('stg_<name>') }}
GROUP BY 1, 2
```

### Add schema tests to `_models.yml`

```yaml
models:
  - name: stg_<name>
    description: "..."
    columns:
      - name: col_a
        tests: [not_null, unique]
      - name: col_b
        tests: [not_null]
```

4. Add singular dbt tests in `transformations/tests/singular/` if custom assertions needed.
5. Add Soda checks in `data_quality/soda/checks/staging_checks.yml` and `mart_checks.yml`.
6. Add Great Expectations in `data_quality/expectations/weather_expectations.py`.
7. Add OpenMetadata provisioning (tests, lineage, column descriptions) per `.claude/rules/openmetadata.md`.

## Validation

```bash
just dbt-run
just dbt-test
just soda-check
uv run pytest tests/unit/test_dags.py -v
```
