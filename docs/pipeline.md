# Pipeline Documentation

## DAG: `weather_pipeline`

**Schedule:** `0 */6 * * *` (every 6 hours)
**Max active runs:** 1
**Retries:** 2 (5-minute delay)

### Task Graph

```
extract_weather → dbt_deps → dbt_run → dbt_test → soda_scan → log_pipeline_metrics
```

### Task Details

#### 1. `extract_weather` (TaskFlow)

Runs the dlt ingestion pipeline. For each city in `WEATHER_CITIES`:
- Calls `/data/2.5/weather` (current weather)
- Calls `/data/2.5/forecast` (5-day forecast)
- Writes to `raw.weather_current` and `raw.weather_forecast`

**Retry logic:** 3 attempts with exponential backoff (2s, 4s, 8s).
**Metrics returned:** duration, city count, status.

#### 2. `dbt_deps` (BashOperator)

Installs dbt packages (dbt_utils) from `packages.yml`.

#### 3. `dbt_run` (BashOperator)

Executes all dbt models:
- **Staging:** `stg_weather_current` (view), `stg_weather_forecast` (view)
- **Mart:** `weather_daily_summary` (table), `city_weather_metrics` (table)

#### 4. `dbt_test` (BashOperator)

Runs 11 data tests:
- `not_null` on city_name, temperature, timestamps
- `unique` on city_name in metrics
- Custom `assert_temperature_range` (-90°C to 60°C)

#### 5. `soda_scan` (BashOperator)

Runs 19 Soda Core checks across staging and mart layers:
- Row count, missing values, invalid ranges, duplicates

#### 6. `log_pipeline_metrics` (TaskFlow)

Logs a summary of the pipeline run with extraction metrics.

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `OPENWEATHER_API_KEY` | Yes | Free tier key from openweathermap.org |
| `WEATHER_CITIES` | No | Comma-separated city list (default: 5 French cities) |
| `POSTGRES_*` | No | Database connection (defaults provided) |

## Error Handling

| Scenario | Behavior |
|----------|----------|
| API rate limit | Exponential backoff, skip city after 3 failures |
| API key missing | `ValueError` raised at config validation |
| Database down | Airflow retries task (2 retries, 5min delay) |
| dbt model failure | Pipeline stops, downstream tasks skipped |
| Soda check failure | Logged as failure, pipeline marked failed |
