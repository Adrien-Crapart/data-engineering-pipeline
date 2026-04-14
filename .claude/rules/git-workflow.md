# Git Workflow Standards

## Branching

- **Never** commit directly to `master`.
- `feat/<component>` — new feature or pipeline
- `fix/<description>` — bug fix
- `refactor/<scope>` — restructuring, no behavior change
- Each branch = **one functional unit**.

## Conventional Commits

Format: `type(scope): description` — imperative mood, lowercase, max 72 chars, no trailing period.

| Type | Usage |
|------|-------|
| `feat` | New feature or pipeline |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `refactor` | Code restructuring |
| `test` | Adding/updating tests |
| `chore` | Maintenance, deps, CI config |

Scope = affected layer: `dbt`, `airflow`, `soda`, `ingestion`, `infra`, `metadata`.

## Commit Layering (Data Engineering Order)

Structure commits **bottom-up through the pipeline**:

```
1. infrastructure / config
2. ingestion / sources       (Bronze)
3. staging models            (Silver)
4. quarantine models
5. core / mart models        (Gold)
6. orchestration DAGs
7. quality checks (GX/Soda)
8. tests
9. documentation
```

Each commit must be atomic, buildable, and independently understandable.

**Example for a new pipeline:**
```
feat(infra): add docker network for forecast service
feat(ingestion): add dlt source for weather_forecast
feat(dbt): add stg_weather_forecast staging model
feat(dbt): add fct_weather_forecast core model
feat(airflow): add forecast ingestion task to DAG
test(dbt): add schema tests for forecast models
docs(pipeline): update pipeline.md with forecast flow
```

## Pre-Merge Requirements

1. All tests pass: `just test`
2. Lint passes: `just check`
3. Docs updated if architecture/pipeline flow changed
4. Docker images build if infra modified: `just build-all`
5. Branch rebased on `master` — no upstream merge commits
6. Self-review complete: `just review`

## Merge Strategy

- **Merge commit** (not squash) to preserve commit layering history.
- Merge message: `Merge <branch>: <short description>`.
- Squash only for single-commit trivial branches (typos, config tweaks).
