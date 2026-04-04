# Pipeline Documentation

## DAG: `weather_pipeline`

**Schedule:** `0 */6 * * *` (every 6 hours)
**Max active runs:** 1
**Retries:** 2 (5-minute delay)
**Execution timeout:** 30 minutes per task

### Task Graph

```
extract_weather → dbt_deps → dbt_run → dbt_test → soda_scan → elementary_report → push_metrics
```

### Task Details

#### 1. `extract_weather` (DockerOperator — `weather-pipeline-dlt:1.0.0`)

Runs in the dlt container. For each city in `WEATHER_CITIES`:
- Validates data against YAML contracts (`weather_current`, `weather_forecast`)
- Archives raw JSON response to MinIO data lake (`s3://data-lake/raw/...`)
- Loads structured data into `raw.weather_current` and `raw.weather_forecast`

**Mounts:** `ingestion/`, `contracts/`

#### 2. `dbt_deps` (DockerOperator — `weather-pipeline-dbt:1.0.0`)

Installs dbt packages (dbt_utils, elementary) from `packages.yml`.

**Mounts:** `transformations/dbt/`

#### 3. `dbt_run` (DockerOperator — `weather-pipeline-dbt:1.0.0`)

Executes all dbt models:
- **Staging:** `stg_weather_current` (view), `stg_weather_forecast` (view)
- **Mart:** `weather_daily_summary` (table), `city_weather_metrics` (table)
- **Elementary:** observability metadata tables

#### 4. `dbt_test` (DockerOperator — `weather-pipeline-dbt:1.0.0`)

Runs data tests:
- `not_null` on city_name, temperature, timestamps
- `unique` on city_name in metrics
- Custom `assert_temperature_range` (-90C to 60C)
- Elementary `volume_anomalies` and `schema_changes`

#### 5. `soda_scan` (DockerOperator — `weather-pipeline-soda:1.0.0`)

Runs 19 Soda Core checks across staging and mart layers.

**Mounts:** `data_quality/`

#### 6. `elementary_report` (DockerOperator — `weather-pipeline-dbt:1.0.0`)

Generates Elementary observability HTML report.

#### 7. `push_metrics` (Airflow TaskFlow)

Pushes pipeline metrics to Prometheus Pushgateway. Runs inside Airflow
(lightweight — only an HTTP POST).

---

## Re-running from any task

Airflow natively supports re-running a DAG from any failed task:

1. Open the Airflow UI (http://localhost:8080)
2. Navigate to the `weather_pipeline` DAG
3. Click on the failed task in the Grid or Graph view
4. Click **Clear** to re-run from that task onward

No separate replay DAG is needed.

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `OPENWEATHER_API_KEY` | Yes | Free tier key from openweathermap.org |
| `WEATHER_CITIES` | No | Comma-separated city list (default: Paris,Lyon,Marseille) |
| `POSTGRES_*` | No | Database connection (defaults provided) |
| `MINIO_ROOT_USER` | No | MinIO credentials (default: minioadmin) |
| `MINIO_ROOT_PASSWORD` | No | MinIO credentials (default: minioadmin) |
| `PROJECT_ROOT` | Yes | Absolute host path to project root (for DockerOperator mounts) |

## Error Handling

| Scenario | Behavior |
|----------|----------|
| API rate limit | Exponential backoff, skip city after 3 failures |
| API key missing | `ValueError` raised at config validation |
| Contract violation | Warning logged, data still processed (soft enforcement) |
| Database down | Airflow retries task (2 retries, 5min delay) |
| MinIO unavailable | Warning logged, raw archiving disabled, pipeline continues |
| dbt model failure | Pipeline stops at dbt_run, clear to re-run |
| Docker container failure | Airflow retries the DockerOperator task |
| Prometheus unavailable | Warning logged, metrics not exported, pipeline continues |
