# AGENTS.md

This file describes the specialized agents available in this repository and how to invoke them from the Claude Code chat.

## Available Agents

| Agent | Purpose | Invoke with |
|-------|---------|-------------|
| [Ingestion Agent](.claude/agents/ingestion-agent.md) | DLT pipelines, data contracts, MinIO (Bronze layer) | `/ingestion` or "use the ingestion agent" |
| [dbt Agent](.claude/agents/dbt-agent.md) | dbt models, DuckDB ATTACH pattern (Silver/Gold layers) | `/dbt` or "use the dbt agent" |
| [Quality Agent](.claude/agents/quality-agent.md) | Soda Core, Great Expectations, quarantine pattern | `/quality` or "use the quality agent" |
| [Infrastructure Agent](.claude/agents/infrastructure-agent.md) | Docker Compose, Dockerfiles, Prometheus/Grafana | `/infra` or "use the infrastructure agent" |
| [OpenMetadata Agent](.claude/agents/openmetadata-agent.md) | OM provisioning, data catalog, lineage, governance | `/om` or "use the openmetadata agent" |

## Available Slash Commands

| Command | What it does |
|---------|-------------|
| `/review` | Full pre-push self-review: automated checks + diff analysis + commit hygiene + verdict |
| `/review-quick` | Fast check: secrets scan, obvious issues, commit format, lines changed |
| `/prepare-pr` | Generate a complete PR description following project standards |
| `/new-dag` | Scaffold a new Airflow DAG with all required fields and conventions |
| `/new-model` | Scaffold a new dbt staging + mart model pair |
| `/quality-check` | Run all quality gates (Soda, GX, dbt test) and report results |
| `/om-provision` | Provision/re-sync all OpenMetadata configuration from code |

## Skill Playbooks

Detailed step-by-step guides for complex recurring tasks:

| Skill | Description |
|-------|-------------|
| [Add a Data Source](.claude/skills/add-data-source.md) | End-to-end: contract → ingestion → staging → mart → tests → OM |
| [Debug a Failing DAG](.claude/skills/debug-dag.md) | Systematic diagnosis by failure type: parse error, worker, DockerOperator, assets |
| [Add a Pipeline Layer](.claude/skills/new-pipeline-layer.md) | Add Silver/Gold models or a new quality gate to an existing pipeline |

## Project Rules

Standards that apply to all work in this repository:

| Rule | Scope |
|------|-------|
| [Airflow Standards](.claude/rules/airflow.md) | DAG conventions, DockerOperator pattern, Assets |
| [Docker Standards](.claude/rules/docker.md) | Image versioning, health checks, Compose structure |
| [Testing Standards](.claude/rules/testing.md) | pytest conventions, test locations, coverage |
| [Git Workflow](.claude/rules/git-workflow.md) | Branching, conventional commits, commit layering |
| [Data Processing](.claude/rules/data-processing.md) | Tool hierarchy: Polars, DuckDB, PyArrow, Parquet conventions |
| [Infrastructure Reuse](.claude/rules/infrastructure-reuse.md) | Reuse existing PostgreSQL, Redis, MinIO — never duplicate |
| [OpenMetadata Rules](.claude/rules/openmetadata.md) | OM 1.12.x provisioning, idempotence, forbidden practices |
| [Self-Review Checklist](.claude/rules/self-review.md) | Pre-push gates: Python, SQL, DAG, Docker, data correctness |
| [Version Verification](.claude/rules/version-verification.md) | Pinned versions table, versioned docs URLs, update protocol |

## Hooks

Automatic actions triggered by Claude's tool use:

| Hook | Trigger | Action |
|------|---------|--------|
| `post-edit.sh` | After editing/writing a `.py` file | Runs `ruff check --fix` + `ruff format` |
| `post-edit.sh` | After editing/writing a `.sql` file in `transformations/` | Runs `sqlfluff fix` |

Hook configuration: [`.claude/settings.json`](.claude/settings.json)

## Architecture Quick Reference

```
OpenWeather API
  → DLT + contract validation
  → MinIO (raw Parquet, Bronze)
  → PostgreSQL staging schema  (dbt via DuckDB ATTACH, Silver)
  → GX + Soda quality gates
  → PostgreSQL core/mart schemas (dbt, Gold)
  → OpenMetadata catalog
  → Prometheus / Grafana
```

**Event chain**: `ingestion_pipeline_dag` → `[staging_weather]` → `quality_gate_pipeline_dag` → `[staging_validated]` → `transformation_pipeline_dag` → `[mart_weather]`

**Key rule**: Airflow orchestrates only. All processing (dbt, Soda, GX, DLT) runs in DockerOperator containers.
