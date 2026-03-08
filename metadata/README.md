# Metadata

OpenMetadata ingestion configuration for the data catalog and lineage system.

## Purpose

Defines the configuration for automated metadata ingestion from PostgreSQL into
OpenMetadata. This enables data discovery, lineage tracking, profiling, and
governance across all pipeline layers.

## Key Files

| File | Description |
|------|-------------|
| `ingestion_config.yaml` | Metadata, lineage, and profiler ingestion configs for PostgreSQL |

## Ingestion Types

1. **Database Metadata** — Catalogs all tables and schemas (raw, staging, mart, elementary)
2. **Database Lineage** — Reconstructs lineage from SQL query logs
3. **Profiler** — Generates data profiles and sample data for staging and mart schemas

## Prerequisites

OpenMetadata must be running (`make up-full`). The metadata ingestion is
orchestrated by the `metadata_ingestion` Airflow DAG.
