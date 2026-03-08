# Transformations

dbt (data build tool) models for transforming raw weather data into analytical models.

## Purpose

Implements the transformation layer of the pipeline using dbt-core. Raw data
loaded by dlt into the `raw` schema is transformed through staging views and
materialized into analytical tables in the `mart` schema.

## Structure

```
transformations/
└── dbt/
    ├── dbt_project.yml     — Project configuration
    ├── profiles.yml        — Connection profiles (PostgreSQL)
    ├── packages.yml        — dbt packages (dbt_utils, elementary)
    ├── models/
    │   ├── staging/        — Cleaned and typed views (stg_weather_current, stg_weather_forecast)
    │   └── marts/          — Analytical tables (weather_daily_summary, city_weather_metrics)
    └── tests/
        └── singular/       — Custom SQL tests (assert_temperature_range)
```

## Data Layers

| Schema | Materialization | Description |
|--------|----------------|-------------|
| `raw` | — | Raw API responses loaded by dlt |
| `staging` | View | Cleaned, typed, renamed columns |
| `mart` | Table | Aggregated analytical models |
| `elementary` | Table | dbt observability metadata |

## Commands

```bash
cd transformations/dbt
dbt deps --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```
