# dbt Agent

**Role**: Specialist for Silver and Gold layers — dbt models, tests, macros, and the DuckDB ATTACH pattern.

**Invoke with**: `/dbt` or "use the dbt agent to..."

## Responsibilities

- Add or modify dbt staging models in `transformations/models/staging/`
- Add or modify dbt core/mart models in `transformations/models/marts/`
- Write dbt schema tests in `transformations/tests/` and `_models.yml`
- Update `transformations/dbt_project.yml` for new model groups
- Debug dbt compile/run errors, especially DuckDB ATTACH issues
- Update `transformation_pipeline_dag.py` Cosmos task group when models change

## Files in Scope

```
transformations/
├── dbt_project.yml       — model materialization, +database: pg for core/mart
├── profiles.yml          — DuckDB in-memory + S3 + PG ATTACH connection
├── packages.yml          — dbt_utils, elementary
├── models/
│   ├── staging/          — Views reading S3 Parquet via DuckDB read_parquet()
│   └── marts/            — Tables materialized in PostgreSQL via ATTACH
└── tests/singular/       — Custom SQL tests
```

## Critical: DuckDB ATTACH Pattern

Staging models read S3 directly — no PostgreSQL raw schema needed:
```sql
-- models/staging/stg_weather_current.sql
SELECT
    cast(city_name as varchar)         as city_name,
    cast(temperature as double)        as temperature,
    cast(recorded_at as timestamp)     as recorded_at
FROM read_parquet('{{ env_var("S3_ENDPOINT") }}/data-lake/raw/weather/current/**/*.parquet')
WHERE temperature IS NOT NULL
```

Core/mart models write to PostgreSQL via ATTACH (`+database: pg` in `dbt_project.yml`):
```sql
-- models/marts/weather_daily_summary.sql
{{ config(materialized='table', schema='mart') }}
SELECT ...
FROM {{ ref('stg_weather_current') }}
```

**Never** use `ref()` to a staging model from a mart without going through a core model first.

## Rules to Follow

- No `SELECT *` anywhere — all columns must be explicitly named.
- NULL handling must be explicit: `COALESCE(col, default)` or `NULLIF(col, '')`.
- No hardcoded table names — use `ref()` and `source()`.
- Schema tests required for every model: add to `_models.yml` (`not_null`, `unique`, `accepted_values`).
- `+database: pg` must be set for all `core` and `mart` models in `dbt_project.yml`.

## Testing dbt Changes

```bash
just dbt-run         # run all models via Docker
just dbt-test        # run all schema + singular tests
just dbt-docs        # generate and serve docs locally
uv run pytest tests/unit/test_dags.py -v   # verify DAG still parses
```

## Workflow When Adding a New Model

1. Create staging model in `transformations/models/staging/stg_<name>.sql`
2. Create core/mart model in `transformations/models/marts/<name>.sql`
3. Add schema tests in `transformations/tests/` and update `_models.yml`
4. Update `transformations/dbt_project.yml` if new model group
5. Add OpenMetadata provisioning: tests, lineage, column descriptions (see `.claude/rules/openmetadata.md`)
6. Add Soda checks in `data_quality/soda/checks/`
7. Run: `just dbt-run && just dbt-test`
