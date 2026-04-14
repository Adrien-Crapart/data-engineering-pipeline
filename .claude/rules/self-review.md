# Self-Review Checklist

Run `just review` to automate the technical checks. Complete every item before pushing.

## A. Functional Cohesion
- [ ] PR addresses a single objective (one feature / one fix / one refactor)
- [ ] No unintended side effects on other pipelines or models
- [ ] Edge cases handled: NULL values, empty datasets, API failures

## B. Diff Hygiene
- [ ] No unrelated files modified
- [ ] No commented-out code
- [ ] No `TODO`/`FIXME` without a linked issue
- [ ] No debug code (`print()`, hardcoded test values)
- [ ] Commits follow layering order (infra → sources → staging → core → DAG → tests → docs)

## C. Security & Secrets
- [ ] No credentials, API keys, or passwords in the diff
- [ ] No `.env` file committed (only `.env.example`)
- [ ] No hardcoded connection strings (use Airflow Connections/Variables)

## D. Python Code Quality
- [ ] `just check` passes (ruff lint + format)
- [ ] Naming is explicit — no `data`, `tmp`, `x`, `df2`
- [ ] No duplication — extract to shared utilities if repeated 3+ times

## E. SQL / dbt Models
- [ ] No `SELECT *` — all columns explicitly named
- [ ] JOIN conditions verified (no cartesian products)
- [ ] NULL handling explicit (`COALESCE`, `NULLIF`, or documented)
- [ ] `ref()` and `source()` used — no hardcoded table names
- [ ] Schema tests defined in `_models.yml` (`not_null`, `unique`, `accepted_values`)
- [ ] `+database: pg` set for core/mart models in `dbt_project.yml`

## F. Airflow DAGs
- [ ] DAG ID is stable and descriptive
- [ ] No heavy logic in DAG file (orchestration only, processing in Docker)
- [ ] `retries`, `retry_delay`, `execution_timeout` set in `default_args`
- [ ] `max_active_runs=1` for data pipelines
- [ ] `catchup=False` unless backfill intended
- [ ] No `Variable.get()` at module level
- [ ] Asset outlets (`outlets`) match the event chain
- [ ] Pool assignments correct (`docker_pool`, `database_pool`, `api_pool`)

## G. Data Correctness
- [ ] Transformations verified on sample data (row counts, sums, distributions)
- [ ] Pipeline is idempotent — re-run without side effects
- [ ] Before/after comparison if modifying existing models

## H. Docker & Infrastructure
- [ ] Images build successfully: `just build-all`
- [ ] No `:latest` tags — all images pinned
- [ ] `mem_limit` set for DockerOperator tasks
- [ ] Health checks pass

## I. Tests & Quality Gates
- [ ] `just test` passes
- [ ] New code has corresponding unit tests
- [ ] `just dbt-test` passes if models changed
- [ ] GX/Soda checks target the correct schema/tables

## J. Documentation
- [ ] `docs/pipeline.md` or `docs/lakehouse.md` updated if architecture changed
- [ ] dbt model descriptions updated in `_models.yml`
- [ ] DAG `doc_md` set for new/modified DAGs

## K. Final Gate
- [ ] I can explain this PR in under 2 minutes without looking at the code
- [ ] A reviewer can understand the PR by reading commits in order
