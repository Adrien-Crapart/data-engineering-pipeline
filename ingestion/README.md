# Ingestion

Data ingestion pipelines and storage clients for extracting weather data
from external APIs and archiving raw responses.

## Purpose

Extracts current weather and forecast data from the OpenWeather API using dlt
(Data Load Tool), validates against data contracts, archives raw JSON in MinIO
(S3-compatible data lake), and loads structured data into PostgreSQL.

## Structure

```
ingestion/
├── config.py                   — PipelineConfig dataclass (API keys, DB, MinIO settings)
├── pipelines/
│   └── openweather_pipeline.py — dlt pipeline: fetch → validate → archive → load
├── storage/
│   └── minio_client.py         — DataLakeClient for writing/reading raw JSON to MinIO
├── schemas/                    — Reserved for future JSON schema definitions
└── sources/                    — Reserved for additional data source connectors
```

## Data Flow

```
OpenWeather API
    ↓ fetch with retry
Contract Validation
    ↓ validate_contract()
MinIO Raw Archive
    ↓ store raw JSON (immutable, partitioned by date)
PostgreSQL (raw schema)
    ↓ dlt pipeline.run()
```

## Configuration

All settings are loaded from environment variables via `PipelineConfig.from_env()`.
See `.env.example` for required variables.
