# Replay

Historical data replay and backfill modules for reprocessing raw data from the
MinIO data lake.

## Purpose

Enables replaying historical raw API responses stored in MinIO back through
the pipeline. This is necessary when:

- Fixing transformation logic and re-running on historical data
- Updating schema contracts and revalidating past data
- Correcting historical errors by reloading raw JSON into the warehouse

## Key Files

| File | Description |
|------|-------------|
| `reprocess_pipeline.py` | CLI script to replay data for a date range |

## Usage

### Via Airflow (recommended)

Trigger the `replay_pipeline` DAG from the Airflow UI with parameters:
- `date_start`: Start date (YYYY-MM-DD)
- `date_end`: End date (YYYY-MM-DD)

### Via CLI

```bash
python -m replay.reprocess_pipeline --start 2025-06-01 --end 2025-06-15
```

## How It Works

1. Lists raw JSON files in `s3://weather-data-lake/raw/openweather/year=YYYY/month=MM/day=DD/`
2. Filters by the specified date range
3. Loads each JSON into PostgreSQL (raw schema) via dlt
4. Runs dbt transformations and tests
