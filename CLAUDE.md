# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

This project uses `just` as the task runner (cross-platform, reads `.env` automatically). Always prefer `just` over running Docker Compose commands directly.

### Development workflow

```bash
just doctor          # Check environment health (Docker, Python, uv, just)
just check           # Lint + format-check (run before committing)
just test            # Run all unit tests
just test-unit       # Unit tests only
just test-integration # Integration tests (requires running services)
just test-cov        # Tests with coverage report
just lint            # Ruff linter only
just lint-sql        # SQLFluff on dbt models
just format          # Auto-format Python code
just pre-commit      # Run all pre-commit hooks
just review          # Full self-review gate: diff, secret scan, debug artifacts, lint, tests
just review-quick    # Quick review: diff stats + secret scan only
```

### Running the stack

```bash
just start           # Build all images + start core services (first launch or after changes)
just up              # Start core services without rebuilding (fast resume)
just start-full      # Build + start everything including OpenMetadata
just down            # Stop core services (volumes kept)
just stop            # Stop core services and remove volumes (DESTRUCTIVE)
just urls            # Print all service URLs
just status          # Show container status
just logs [SERVICE]  # Follow logs for a service
```

### Data pipeline operations

```bash
just dbt-run         # Run dbt transformations via Docker
just dbt-test        # Run dbt tests
just dbt-docs        # Generate and serve dbt docs
just dlt-run         # Run DLT ingestion pipeline
just soda-check      # Run Soda Core data quality checks
just gx-check        # Run Great Expectations validations
just psql            # Open PostgreSQL shell
just airflow-shell   # Open bash in Airflow worker
```

### Dependencies

Uses `uv` for Python dependency management. Each module (`ingestion/`, `orchestration/`, `transformations/`, `data_quality/`) has its own `pyproject.toml`. The root `pyproject.toml` is authoritative for dev/test dependencies. `uv.lock` is git-tracked.

## Architecture

### Data flow (Medallion architecture)

```
OpenWeather API
  → DLT pipeline (extraction + contract validation)
  → MinIO data lake (raw Parquet, immutable, partitioned by date)  [Bronze]
  → PostgreSQL raw schema (auto-loaded by DLT)
  → PostgreSQL staging schema (dbt views via dbt-duckdb ATTACH)    [Silver]
  → Data quality gates (Great Expectations + Soda Core)
  → PostgreSQL core/mart schemas (dbt tables)                       [Gold]
  → OpenMetadata catalog (lineage, profiling, governance)
  → Prometheus/Grafana (metrics, dashboards, alerts)
```

### DAG orchestration (event-driven)

Four production DAGs, three of which chain via Airflow Assets:

1. **`ingestion_pipeline_dag`** (every 6h, Europe/Paris) — DLT extraction → dbt staging → emits `staging_weather` Asset
2. **`quality_gate_pipeline_dag`** (triggered by `staging_weather`) — GX + Soda validation → quarantine failed rows → emits `staging_validated` Asset
3. **`transformation_pipeline_dag`** (triggered by `staging_validated`) — dbt core/mart → quality gates → dbt tests → push Prometheus metrics → emits `mart_weather` Asset
4. **`monitoring_dag`** (every 5min) — Airflow health + container status → Prometheus metrics → Alertmanager

Plus **`openmetadata_ingestion_dag`** (manual or scheduled) for metadata catalog refresh.

### Key architectural decisions

- **DockerOperator for all processing** — Airflow orchestrates only; DLT, dbt, Soda each run in their own Docker container (separate `Dockerfile` per tool in `infrastructure/docker/` and `orchestration/`).
- **dbt-duckdb with ATTACH** — DuckDB reads PostgreSQL staging via ATTACH and writes to core/mart schemas. This is the transformation execution model, not standard dbt-postgres.
- **Event-driven chaining** — Downstream DAGs trigger automatically via Airflow Assets; no manual scheduling for quality gate or transformation DAGs.
- **MinIO for raw archival** — Immutable raw data enables replay from any point; use `just dlt-run` to re-ingest; use Airflow's native Clear to re-run from any task.
- **Data contracts** (`contracts/*.yaml`) — Schema validation at ingestion time before any data hits storage.

### Module responsibilities

| Directory | Responsibility |
|-----------|---------------|
| `orchestration/dags/` | Airflow DAG definitions; plugin callbacks/notifications in `orchestration/plugins/` |
| `ingestion/` | DLT pipeline (`openweather_pipeline.py`), MinIO client, contract validation |
| `transformations/models/staging/` | dbt views — clean/type/rename raw columns |
| `transformations/models/marts/` | dbt tables — daily aggregations, city-level metrics |
| `data_quality/soda/checks/` | 19 Soda Core checks (staging + mart) |
| `data_quality/expectations/` | Great Expectations suites |
| `monitoring/` | Prometheus config, Grafana dashboards, Loki, Alertmanager, custom Docker exporter |
| `metadata/` | OpenMetadata bootstrap scripts (classifications, glossary, teams, alerts) |
| `contracts/` | Data contract YAML definitions + `validator.py` |
| `infrastructure/docker/` | All Docker Compose files (modular, one per service group) |
| `tests/unit/` | Pytest unit tests (mock external dependencies) |
| `tests/integration/` | Tests requiring live Docker services |

### Shared constants

Task images, pool names, and Airflow Asset names are centralized in `orchestration/plugins/constants.py`. Always import from there rather than hardcoding strings in DAGs.

### Service ports (local)

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | airflow / airflow |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| OpenMetadata | http://localhost:8585 | — |
| MailHog | http://localhost:8025 | — |

## Testing conventions

- pytest markers: `unit`, `integration`, `slow` — defined in root `pyproject.toml`
- DAG convention tests in `tests/unit/test_dags.py` validate all DAGs parse, have owners, retries, and correct Asset naming
- Run a single test file: `uv run pytest tests/unit/test_contracts.py -v`
- Run by marker: `uv run pytest -m unit`
- Integration tests require services running (`just up` first)

## Git workflow

Conventional commits required: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`, `perf:`. Run `just review` before pushing.

Commit layering order (bottom-up): infrastructure → ingestion → staging → core/mart → orchestration → quality checks → tests → docs.

## Claude Code configuration (`.claude/`)

Project-specific configuration for Claude Code lives in `.claude/`:

| Folder | Contents |
| ------ | -------- |
| [`rules/`](.claude/rules/) | 9 convention files (airflow, docker, testing, git, data-processing, infra-reuse, openmetadata, self-review, version-verification) |
| [`agents/`](.claude/agents/) | 5 specialist agents: ingestion, dbt, quality, infrastructure, openmetadata |
| [`commands/`](.claude/commands/) | 7 slash commands: `/review`, `/review-quick`, `/prepare-pr`, `/new-dag`, `/new-model`, `/quality-check`, `/om-provision` |
| [`skills/`](.claude/skills/) | 3 playbooks: add-data-source, debug-dag, new-pipeline-layer |
| [`hooks/`](.claude/hooks/) | `post-edit.sh` — auto-runs ruff/sqlfluff after file edits |

See [AGENTS.md](AGENTS.md) for the full index of agents, commands, and skills with invocation syntax.
