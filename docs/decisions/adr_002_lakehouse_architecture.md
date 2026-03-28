# ADR-002: Lakehouse Architecture with DuckDB and Parquet

**Status**: Accepted
**Date**: 2026-03-08
**Decision makers**: Adrien

## Context

The initial architecture loaded raw API data directly into PostgreSQL using dlt,
with JSON archives on MinIO for backup. This approach had several limitations:

- Raw data locked into PostgreSQL (vendor lock-in)
- JSON format on S3 is inefficient for analytical queries
- No multi-engine access to raw data
- Storage cost higher than necessary for row-oriented format

## Decision

Adopt a **medallion lakehouse architecture**:

1. **Bronze (raw)**: dlt writes Parquet files to MinIO S3 via the `filesystem` destination
2. **Silver (staging)**: dbt-duckdb reads Parquet from S3, creates cleaned views
3. **Gold (mart)**: dbt-duckdb materializes analytical tables to PostgreSQL

## Rationale

| Criterion | PostgreSQL Raw | Parquet Lakehouse |
|-----------|---------------|-------------------|
| Storage efficiency | ~1x (row format) | ~5-10x compression (Parquet ZSTD) |
| Query flexibility | SQL only (PostgreSQL) | DuckDB, Polars, PyArrow, Spark |
| Schema evolution | ALTER TABLE (manual) | Automatic Parquet schema merge |
| Data immutability | Append-only tables | Immutable files |
| Cost | Higher (PostgreSQL storage) | Lower (S3 object storage) |
| Portability | PostgreSQL only | Any Parquet-compatible engine |

### Why DuckDB over PostgreSQL for transformations?

- DuckDB reads Parquet natively from S3 (zero data movement for raw layer)
- In-memory analytical engine with vectorized execution
- Can attach PostgreSQL for writing mart tables
- Growing industry adoption for lakehouse architectures
- Showcases modern Data Engineering patterns

### Why Parquet over JSON?

- Columnar format: only reads columns needed for a query
- Built-in compression (ZSTD): 5-10x smaller than JSON
- Type-safe: preserves column types across loads
- PyArrow integration: zero-copy reads into Polars and DuckDB

## Consequences

### Positive

- Lower storage costs on MinIO
- Faster analytical queries on raw data
- Multi-engine access (DuckDB, Polars, PyArrow)
- Better schema evolution handling
- Portfolio demonstrates modern lakehouse patterns

### Negative

- Added complexity (DuckDB + PostgreSQL dual engine)
- dbt-duckdb is less mature than dbt-postgres
- Elementary observability compatibility may require attention
- Debugging cross-engine issues is harder

## Alternatives Considered

1. **Keep PostgreSQL raw + dbt-postgres**: simpler but no lakehouse benefits
2. **Apache Iceberg on MinIO**: too heavy for this scale, requires Spark/Trino
3. **Delta Lake on MinIO**: similar to Iceberg, overkill for single-node
4. **DuckDB only (no PostgreSQL)**: loses Grafana and Soda Core compatibility
