# OpenMetadata Provisioning Rules

Applies to: `metadata/**`, `orchestration/dags/openmetadata_*`, `transformations/models/**`

## Core Principle

**All OpenMetadata configuration MUST be automated via scripts.** Manual UI configuration is forbidden for anything that can be scripted. Run `just om-provision` after any structural change.

## When Adding a New dbt Model / Table

Every new dbt model that materializes in PostgreSQL needs all of the following:

1. **Test cases** → `metadata/om_setup_full.py`, `TABLE_TESTS` dict
2. **Lineage edges** → `metadata/om_setup_full.py`, `TABLE_LINEAGE` list
3. **Table config** (tier, owner, domain, description) → `metadata/om_bootstrap.py`, `TABLE_CONFIG`
4. **Data contract** → `metadata/om_setup_full.py`, `TABLE_CONTRACTS`
5. **Column descriptions** → `metadata/om_setup_full.py`, `add_column_descriptions()`
6. **GX expectations** → `data_quality/expectations/run_validations.py`
7. **Soda checks** → `data_quality/soda/checks/{layer}_checks.yml`

Then run: `just om-provision`

## When Adding a New Airflow DAG

1. Add task descriptions → `metadata/om_setup_full.py`, `TASK_DESCRIPTIONS`
2. Add pipeline ownership → `metadata/om_setup_full.py`, `PIPELINE_OWNERS`
3. Add pipeline-to-table lineage → `_create_pipeline_lineage()`, `dags` dict
4. Run: `just om-provision`

## Script Reference

| Script | Purpose | Command |
|--------|---------|---------|
| `om_bootstrap.py` | Governance: classifications, glossary, teams | `just om-bootstrap` |
| `om_setup_full.py` | Tests, lineage, Airflow sync, contracts, column descriptions | `just om-setup` |
| `om_create_alerts.py` | Observability alerts | `just om-create-alerts` |
| `om_trigger_agents.py` | Trigger OM metadata/profiler agents | `just om-trigger-agents` |

**Unified**: `just om-provision` runs all scripts sequentially.

## Idempotence Rules

- All OM API calls must use `PUT` (upsert) not `POST` where possible.
- Check existence before creating: `GET /api/v1/{resource}/name/{fqn}`.
- Scripts must be safe to re-run without side effects.

## OM 1.12.x Terminology (Breaking Changes from Earlier Versions)

| Old Term | New Term (1.12.x) |
|----------|-------------------|
| Ingestion tab | **Agents** tab |
| Add Ingestion | **Add Agent** |
| Ingestion Pipeline | **Agent** |
| Profiler tab | **Data Observability** tab |
| Data Quality tab | **Data Observability → Data Quality** sub-tab |

## Forbidden

- Never configure tests, tags, descriptions, or lineage manually in the OM UI if scriptable.
- Never use `POST` when `PUT` (idempotent upsert) is available.
- Never hardcode OM credentials.
- Never skip updating `om_setup_full.py` when adding tables or pipelines.
