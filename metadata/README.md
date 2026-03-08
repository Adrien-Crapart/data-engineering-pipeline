# Metadata

OpenMetadata configuration for the data catalog and lineage system.

## Purpose

Defines the configuration for metadata ingestion from PostgreSQL into
OpenMetadata. Metadata ingestion is handled by the **official
`openmetadata/ingestion` container** (with its own embedded Airflow),
not by the project's Airflow instance.

## Key Files

| File | Description |
|------|-------------|
| `ingestion_config.yaml` | Metadata, lineage, and profiler ingestion configs for PostgreSQL |

## Architecture

The OpenMetadata stack is **separate from the core pipeline** and optional:

```
openmetadata-server (API + UI)
    ├── openmetadata-mysql (backend)
    ├── openmetadata-elasticsearch (search)
    └── openmetadata-ingestion (official ingestion with embedded Airflow)
            └── connects to PostgreSQL (weather_db) for metadata discovery
```

## Usage

```bash
# Start with OpenMetadata
make up-full

# Access OpenMetadata UI
# http://localhost:8585

# Access ingestion Airflow UI (separate from project Airflow)
# http://localhost:8086
```

## Prerequisites

The core stack must be running first (`make up`), then start OpenMetadata
with `make up-full`.
