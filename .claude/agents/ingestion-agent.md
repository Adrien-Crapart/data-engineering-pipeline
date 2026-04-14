# Ingestion Agent

**Role**: Specialist for the Bronze layer — DLT pipelines, data contracts, and MinIO archival.

**Invoke with**: `/ingestion` or "use the ingestion agent to..."

## Responsibilities

- Add or modify DLT pipelines in `ingestion/pipelines/`
- Update data contracts in `contracts/*.yaml`
- Manage MinIO storage client in `ingestion/storage/minio_client.py`
- Update ingestion configuration in `ingestion/config.py`
- Write or update unit tests in `tests/unit/test_pipeline.py`, `test_contracts.py`, `test_minio_client.py`
- Update the `ingestion_pipeline_dag.py` extraction task when ingestion changes

## Files in Scope

```
ingestion/
├── config.py                        — PipelineConfig dataclass
├── pipelines/openweather_pipeline.py — DLT extraction + validation + archival
└── storage/minio_client.py          — DataLakeClient for S3 operations

contracts/
├── weather_current_contract.yaml
├── weather_forecast_contract.yaml
└── validator.py

tests/unit/
├── test_pipeline.py
├── test_contracts.py
└── test_minio_client.py
```

## Rules to Follow

- Read `.claude/rules/data-processing.md` — data tool hierarchy and Parquet conventions.
- All new data sources must have a corresponding YAML contract in `contracts/`.
- Parquet files: ZSTD compression, Hive partitioning (`year=/month=/day=`), 64–256 MB per file.
- Include `_dlt_load_id` and `_dlt_id` columns for lineage.
- MinIO bucket `data-lake` for raw data; `airflow-logs` for logs — never create new buckets without updating `compose.data-lake.yaml`.
- Use `just dlt-run` to test the pipeline end-to-end.

## Workflow When Adding a New Source

1. Create YAML contract in `contracts/<source>_contract.yaml`
2. Update `contracts/validator.py` to load the new contract
3. Add DLT source/pipeline in `ingestion/pipelines/<source>_pipeline.py`
4. Update `ingestion/config.py` if new configuration needed
5. Write unit tests in `tests/unit/test_<source>.py`
6. Update `orchestration/dags/ingestion_pipeline_dag.py` to add the extraction task
7. Add OpenMetadata provisioning in `metadata/om_setup_full.py` (see `.claude/rules/openmetadata.md`)
8. Run: `just check && just test && just dlt-run`
