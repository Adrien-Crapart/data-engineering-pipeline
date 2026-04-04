# Metadata

OpenMetadata configuration, governance automation, and **data quality catalog** for the data platform.

## ⚠️ OpenMetadata 1.12.x Limitations

**Data Quality Test Execution**: OpenMetadata 1.12.x cannot execute tests programmatically or via UI. Tests serve as **documentation/catalog only**. Use Great Expectations and Soda Core directly via Airflow DAGs for actual test execution.

See [DATA_QUALITY.md](../DATA_QUALITY.md) for details.

## Key Files

| File | Description |
|------|-------------|
| `om_bootstrap.py` | Provisions classifications, glossary, teams, tiers, domains, custom properties via OM REST API |
| `om_setup_full.py` | Full setup: tests (catalog), profiler, Airflow service, lineage, column descriptions |
| `om_create_alerts.py` | Creates observability alerts (Slack, Email) for test failures, schema changes, etc. |
| `om_trigger_agents.py` | Manually triggers metadata/profiler/dbt agents |
| `list_all_agents.py` | Lists all ingestion agents for the datawarehouse service |
| `list_test_suites.py` | Lists all test suites and their status |
| `ingestion_config.yaml` | Legacy metadata/lineage/profiler ingestion configs (agents are now configured via OM UI) |
| `om_config_export.json` | Auto-generated export of current OM configuration (git-ignored) |

## Quick Start

### 1. Initial Setup (once)

```bash
# Step 1: Bootstrap governance structure (classifications, glossary, teams, etc.)
just om-bootstrap

# Step 2: Full setup (test catalog, profiler, services, lineage)
just om-setup
```

### 2. Daily Usage

```bash
# Trigger metadata refresh
python metadata/om_trigger_agents.py

# List all agents
python metadata/list_all_agents.py

# List test suites
python metadata/list_test_suites.py
```

## Data Quality

### ⚠️ Test Execution (OM 1.12.x Limitation)

OpenMetadata 1.12.x **cannot execute tests**:
- ❌ No API endpoint for test execution
- ❌ No "Run" button in UI
- ❌ TestSuite agent fails

**Solution**: Use Great Expectations and Soda Core directly via Airflow:

```bash
# Run quality checks on staging data
docker exec weather-pipeline-soda python -m data_quality.expectations.run_validations --layer staging
docker exec weather-pipeline-soda python -m data_quality.soda.run_scan --config-file configuration_staging.yml

# Or via Airflow DAGs (automated)
# - quality_gate_pipeline: validates staging after ingestion
# - transformation_pipeline: validates core/mart after transformation
```

Reports are stored on S3:
- `s3://data-lake/_reports/great_expectations/`
- `s3://data-lake/_reports/soda/`

### Test Catalog (View Only)

Tests are defined in OpenMetadata as **documentation**:

```
http://localhost:8585/table/datawarehouse.datawarehouse.staging.stg_weather_current/profiler/data-quality
```

You can see:
- 68 test cases configured across all tables
- Test definitions (column checks, value ranges, etc.)
- Test metadata (owner, description, tags)

But **execution must happen outside OM** in version 1.12.x.

## Architecture

The OpenMetadata stack is **separate from the core pipeline** and optional:

```
openmetadata-server (API + UI)     → http://localhost:8585
    ├── postgres (shared PG)       → openmetadata_db database
    ├── elasticsearch (search)     → ES 9.0.2
    └── openmetadata-ingestion     → embedded Airflow 3.x (agents)
```

## Prerequisites

- Core stack must be running: `just up` or `just up-full`
- Metadata Agent must have run at least once (tables visible in OM)
- Python with `requests` package installed locally for the bootstrap script

## Detailed Documentation

- [DATA_QUALITY.md](../DATA_QUALITY.md) — Data quality setup and OM 1.12.x limitations
- [GUIDE_OPENMETADATA.md](../GUIDE_OPENMETADATA.md) — Complete OpenMetadata setup guide
