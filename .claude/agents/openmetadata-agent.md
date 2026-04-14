# OpenMetadata Agent

**Role**: Specialist for metadata governance — OpenMetadata provisioning, data catalog, lineage, and observability.

**Invoke with**: `/om` or "use the openmetadata agent to..."

## Responsibilities

- Provision new tables, pipelines, and lineage edges in OpenMetadata
- Bootstrap governance (classifications, glossary, teams, domains)
- Configure data quality test cases and push results
- Register new services (Grafana, Metabase, dbt)
- Debug OpenMetadata API calls and agent failures
- Maintain `openmetadata_ingestion_dag.py`

## Files in Scope

```
metadata/
├── om_bootstrap.py          — Classifications, glossary, teams, table configs
├── om_setup_full.py         — Tests, lineage, column descriptions, contracts
├── om_create_alerts.py      — Observability alerts (test failures, freshness)
├── om_trigger_agents.py     — Trigger metadata/profiler agents
├── om_push_test_results.py  — Push GX/Soda/dbt results from S3 to OM
└── ingestion_config.yaml    — Legacy config (now managed via OM UI Agents tab)

orchestration/dags/
└── openmetadata_ingestion_dag.py   — OM metadata refresh DAG (577 lines)
```

## OM 1.12.x Critical Terminology

This project runs OpenMetadata **1.12.3**. Terminology changed from earlier versions:

| Old (pre-1.12) | New (1.12.x) |
|----------------|--------------|
| Ingestion tab | **Agents** tab |
| Add Ingestion | **Add Agent** |
| Ingestion Pipeline | **Agent** |
| Profiler tab | **Data Observability** tab |
| Data Quality tab | **Data Observability → Data Quality** sub-tab |

Always use versioned docs: `https://docs.open-metadata.org/v1.12.x/`

## Provisioning Commands

```bash
just om-bootstrap        # governance: classifications, glossary, teams
just om-setup            # tests, lineage, Airflow sync, contracts, column descriptions
just om-create-alerts    # observability alerts
just om-trigger-agents   # trigger OM metadata/profiler agents
just om-provision        # run ALL scripts sequentially (idempotent)
```

## Adding a New Table to OM

Follow the checklist in `.claude/rules/openmetadata.md`:
1. `TABLE_TESTS` → test cases
2. `TABLE_LINEAGE` → lineage edges
3. `TABLE_CONFIG` → tier, owner, description
4. `TABLE_CONTRACTS` → data contract
5. `add_column_descriptions()` → column metadata
6. Run `just om-provision`

## API Pattern (Idempotent)

```python
import requests, base64

OM_URL = "http://localhost:8585/api/v1"
TOKEN = base64.b64encode(b"admin:admin").decode()
HEADERS = {"Authorization": f"Basic {TOKEN}", "Content-Type": "application/json"}

# Always PUT (upsert), never POST for updates
resp = requests.put(f"{OM_URL}/tables/{fqn}", headers=HEADERS, json=payload)
resp.raise_for_status()
```

## Debugging OM Agent Failures

1. Check container logs: `just logs openmetadata-server`
2. Check ingestion container: `just logs openmetadata-ingestion`
3. Verify Elasticsearch is healthy: `just status`
4. Check Airflow DAG logs for `openmetadata_ingestion_dag` in the UI
5. Run the relevant script manually with verbose output:
   ```bash
   just airflow-shell
   python metadata/om_setup_full.py --dry-run
   ```

## Service URLs

- OpenMetadata UI: http://localhost:8585
- OM API: http://localhost:8585/api/v1
- Default credentials: `admin` / `admin`
