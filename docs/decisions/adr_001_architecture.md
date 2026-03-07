# ADR-001: Pipeline Architecture Decisions

## Status
Accepted

## Context
We need to build a production-grade weather data pipeline that demonstrates modern Data Engineering best practices for a technical portfolio. The pipeline must be reproducible, well-documented, and showcase the full data lifecycle.

## Decisions

### 1. Orchestration: Apache Airflow 3
**Choice:** Airflow 3.1.7 with TaskFlow API  
**Alternatives considered:** Prefect, Dagster, Mage  
**Rationale:** Airflow is the industry standard for data orchestration. Version 3 brings a modern SDK, improved API, and FAB auth manager. Most Data Engineering teams use Airflow, making it the most relevant skill to demonstrate.

### 2. Ingestion: dlt (Data Load Tool)
**Choice:** dlt with PostgreSQL destination  
**Alternatives considered:** Custom Python scripts, Airbyte, Singer taps  
**Rationale:** dlt provides declarative pipeline definition with built-in schema management, automatic typing, and retry logic. It reduces boilerplate while maintaining full control over the extraction logic.

### 3. Storage: PostgreSQL with layered schemas
**Choice:** PostgreSQL 16 with raw/staging/analytics schemas  
**Alternatives considered:** Snowflake, BigQuery, DuckDB  
**Rationale:** PostgreSQL is free, widely available, and sufficient for this scale. The three-schema architecture (raw → staging → analytics) follows the medallion pattern used in modern data platforms.

### 4. Transformation: dbt-core
**Choice:** dbt-core with dbt-postgres adapter  
**Alternatives considered:** SQLMesh, plain SQL scripts  
**Rationale:** dbt is the standard for SQL-based transformations. It provides testing, documentation, lineage tracking, and a proven development workflow that data teams expect.

### 5. Data Quality: Soda Core + Great Expectations
**Choice:** Dual data quality framework  
**Alternatives considered:** dbt tests only, custom Python checks  
**Rationale:** Soda Core provides lightweight YAML-based checks integrated into the pipeline. Great Expectations adds comprehensive expectation suites for deeper validation. Together, they demonstrate proficiency with both approaches.

### 6. Analytics: DuckDB
**Choice:** DuckDB for local analytics  
**Alternatives considered:** Pandas, Spark  
**Rationale:** DuckDB provides a fast, embedded analytical database that can query PostgreSQL data directly. It demonstrates modern analytical tooling without infrastructure overhead.

### 7. Infrastructure: Docker Compose
**Choice:** Docker Compose with custom Airflow image  
**Alternatives considered:** Kubernetes, Terraform  
**Rationale:** Docker Compose provides full reproducibility with minimal complexity. A single `make up` command starts the entire pipeline, making it easy for reviewers to test.

### 8. Package Management: uv
**Choice:** uv for Python dependency management  
**Alternatives considered:** pip, poetry, conda  
**Rationale:** uv is the fastest Python package installer (10-100x faster than pip). It demonstrates awareness of modern Python tooling while maintaining pip compatibility.

## Consequences
- The entire stack can run on a single machine with Docker
- All tools are open-source and free to use
- The architecture mirrors real production data platforms at smaller scale
- Reviewers can reproduce the entire pipeline with `make up`
