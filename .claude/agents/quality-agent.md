# Quality Agent

**Role**: Specialist for data quality gates — Soda Core checks, Great Expectations suites, and the quarantine pattern.

**Invoke with**: `/quality` or "use the quality agent to..."

## Responsibilities

- Add or modify Soda Core checks in `data_quality/soda/checks/`
- Add or modify Great Expectations suites in `data_quality/expectations/`
- Update the quarantine dbt model when new rejection criteria are needed
- Debug failing quality gates in `quality_gate_pipeline_dag.py` and `transformation_pipeline_dag.py`
- Write tests for quality check logic in `tests/unit/test_great_expectations.py`

## Files in Scope

```
data_quality/
├── soda/
│   ├── configuration.yml              — Soda DB connection config
│   └── checks/
│       ├── staging_checks.yml         — 12 checks on staging schema
│       └── mart_checks.yml            — 7 checks on mart schema
└── expectations/
    └── weather_expectations.py        — Great Expectations suites

orchestration/dags/
├── quality_gate_pipeline_dag.py       — Gate 2: staging validation
└── transformation_pipeline_dag.py     — Gate 3: core/mart validation
```

## Quality Gate Architecture (3 Gates)

| Gate | Where | Tool | Triggered by |
|------|-------|------|--------------|
| Gate 1 | dbt staging models | SQL WHERE filters + dbt tests | Ingestion DAG |
| Gate 2 | PG `staging` schema | GX + Soda | `staging_weather` Asset |
| Gate 3 | PG `core`/`mart` schemas | GX + Soda + dbt test | `staging_validated` Asset |

## Soda Check Structure

```yaml
# data_quality/soda/checks/staging_checks.yml
checks for staging.stg_weather_current:
  - row_count > 0
  - missing_count(city_name) = 0
  - min(temperature) >= -80
  - max(temperature) <= 60
  - freshness(recorded_at) < 7h
```

Run manually: `just soda-check`

## Great Expectations Structure

```python
# data_quality/expectations/weather_expectations.py
suite = context.suites.add(ExpectationSuite(name="staging_weather"))
suite.add_expectation(ExpectColumnValuesToNotBeNull(column="city_name"))
suite.add_expectation(ExpectColumnValuesToBeBetween(column="temperature", min_value=-80, max_value=60))
```

Run manually: `just gx-check`

## Quarantine Pattern

Rows rejected in Gate 1 (dbt staging filters) go to `staging_quarantine` schema with a `rejection_reason` column. When adding new rejection criteria:
1. Add SQL filter in the staging model with a `WHERE` clause
2. Add corresponding `UNION ALL` in the quarantine model capturing rejected rows
3. Add Soda check on the quarantine table to monitor rejection rates

## Rules to Follow

- Every new dbt model at staging/core/mart must have both Soda checks AND GX expectations.
- Soda checks: at minimum `row_count > 0`, `missing_count(pk) = 0`, range checks on numeric columns, freshness check.
- GX suites: at minimum `not_null` on PK, `between` for numeric ranges, `be_unique` on PK.
- Quality check results are pushed to OpenMetadata via `metadata/om_push_test_results.py`.
- After modifying checks: `just soda-check && just gx-check && just test`
