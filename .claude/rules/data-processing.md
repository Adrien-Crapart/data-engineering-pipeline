# Data Processing Standards

Applies to: `**/*.py`, `analytics/**`, `ingestion/**`

## Tool Preference Hierarchy (fastest first)

1. **Polars** — LazyFrame API for all DataFrame operations
2. **DuckDB SQL** — analytical queries, Parquet reads, cross-source joins
3. **PyArrow** — I/O layer for Parquet, zero-copy interop
4. **Dask** — distributed processing when data exceeds single-node memory
5. **pandas** — avoid; use only when a library has no Polars/Arrow support

## Polars Conventions

```python
# GOOD — lazy evaluation, collect at the end
df = (
    pl.scan_parquet("s3://data-lake/raw/weather/*.parquet")
    .filter(pl.col("temperature").is_not_null())
    .with_columns(pl.col("timestamp").cast(pl.Datetime))
    .collect()
)

# BAD
df = pl.read_parquet(...)  # eager when lazy is possible
df.to_pandas()             # never unless library requires it
```

- Always `LazyFrame` over `DataFrame` for chained operations.
- `.collect()` only at the end of a pipeline.
- `pl.col()` expressions over index-based access.
- `.sink_parquet()` for streaming writes.

## DuckDB Conventions

```sql
-- S3 access (MinIO)
SET s3_endpoint = 'minio:9000';
SET s3_access_key_id = 'minioadmin';
SET s3_url_style = 'path';

-- Read Parquet from S3
SELECT * FROM read_parquet('s3://data-lake/raw/weather/**/*.parquet');

-- PostgreSQL via ATTACH
ATTACH 'host=postgres port=5432 dbname=datawarehouse' AS pg (TYPE POSTGRES);
SELECT * FROM pg.staging.stg_weather_current;
```

- Use `read_parquet()` for direct S3 Parquet queries (no data movement).
- Prefer DuckDB SQL over Polars for complex aggregations with window functions.
- `COPY ... TO ... (FORMAT PARQUET)` for exports.

## Parquet Best Practices

- ZSTD compression always.
- Hive-style partitioning: `year=YYYY/month=MM/day=DD`.
- File size target: 64 MB – 256 MB per file.
- Include `_dlt_load_id` and `_dlt_id` metadata columns for lineage.

## PyArrow

- `pyarrow.parquet` for direct Parquet read/write.
- `pa.Table` as interchange format between DuckDB and Polars.
- `pyarrow.fs.S3FileSystem` for S3 access when not going through DuckDB.
