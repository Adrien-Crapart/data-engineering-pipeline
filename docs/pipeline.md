# Pipeline Documentation

## DAG: `weather_pipeline`

**Schedule:** `0 */6 * * *` (every 6 hours)
**Max active runs:** 1
**Retries:** 2 (5-minute delay)

### Task Graph

```
extract_weather → dbt_deps → dbt_run → dbt_test → soda_scan → elementary_report → push_metrics
```

### Task Details

#### 1. `extract_weather` (TaskFlow)

Runs the dlt ingestion pipeline. For each city in `WEATHER_CITIES`:
- Validates data against contracts (`weather_current`, `weather_forecast`)
- Archives raw JSON response to MinIO data lake (`s3://weather-data-lake/raw/...`)
- Calls `/data/2.5/weather` (current weather)
- Calls `/data/2.5/forecast` (5-day forecast)
- Writes to `raw.weather_current` and `raw.weather_forecast` in PostgreSQL

**Retry logic:** 3 attempts with exponential backoff (2s, 4s, 8s).
**Metrics returned:** duration, city count, status.

#### 2. `dbt_deps` (DockerOperator)

Installs dbt packages (dbt_utils, elementary) from `packages.yml`.
Runs in isolated `weather-pipeline-processing` container.

#### 3. `dbt_run` (DockerOperator)

Executes all dbt models in isolated container:
- **Staging:** `stg_weather_current` (view), `stg_weather_forecast` (view)
- **Mart:** `weather_daily_summary` (table), `city_weather_metrics` (table)
- **Elementary:** observability metadata tables

#### 4. `dbt_test` (DockerOperator)

Runs 11+ data tests in isolated container:
- `not_null` on city_name, temperature, timestamps
- `unique` on city_name in metrics
- Custom `assert_temperature_range` (-90C to 60C)
- Elementary `volume_anomalies` and `schema_changes`

#### 5. `soda_scan` (DockerOperator)

Runs 19 Soda Core checks across staging and mart layers in isolated container.

#### 6. `elementary_report` (DockerOperator)

Generates Elementary observability HTML report in isolated container.

#### 7. `push_metrics` (TaskFlow)

Pushes pipeline metrics to Prometheus Pushgateway and logs summary.

---

## DAG: `replay_pipeline`

**Schedule:** Manual (triggered via Airflow UI with parameters)
**Parameters:** `date_start`, `date_end` (YYYY-MM-DD)

### Task Graph

```
replay_from_datalake → dbt_deps → dbt_run → dbt_test → log_replay_result
```

Reads raw JSON files from MinIO for the specified date range, reloads them into
PostgreSQL, and re-runs dbt transformations in isolated Docker containers.

---

## DAG: `metadata_ingestion`

**Schedule:** Daily at 06:00 UTC
**Purpose:** Sync metadata catalog with OpenMetadata

### Task Graph

```
ingest_metadata → ingest_lineage → run_profiler
```

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `OPENWEATHER_API_KEY` | Yes | Free tier key from openweathermap.org |
| `WEATHER_CITIES` | No | Comma-separated city list (default: 5 French cities) |
| `POSTGRES_*` | No | Database connection (defaults provided) |
| `MINIO_ROOT_USER` | No | MinIO credentials (default: minioadmin) |
| `MINIO_ROOT_PASSWORD` | No | MinIO credentials (default: minioadmin) |
| `GRAFANA_ADMIN_PASSWORD` | No | Grafana admin password (default: admin) |

## Error Handling

| Scenario | Behavior |
|----------|----------|
| API rate limit | Exponential backoff, skip city after 3 failures |
| API key missing | `ValueError` raised at config validation |
| Contract violation | Warning logged, data still processed (soft enforcement) |
| Database down | Airflow retries task (2 retries, 5min delay) |
| MinIO unavailable | Warning logged, raw archiving disabled, pipeline continues |
| dbt model failure | Pipeline stops, downstream tasks skipped |
| Soda check failure | Logged as failure, pipeline marked failed |
| Docker container failure | Airflow retries the DockerOperator task |
| Prometheus unavailable | Warning logged, metrics not exported, pipeline continues |
