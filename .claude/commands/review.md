Perform a complete self-review of the current branch before pushing to remote.
Act as a strict external reviewer. Use the base branch `master` unless the user specified another.

## Step 1 — Run automated checks

Run `just review` in the terminal. Capture the full output and report each step with pass/fail status.

## Step 2 — Analyze the diff manually

Run `git diff master...HEAD` and inspect every changed file against the checklist in `.claude/rules/self-review.md`.

### Python files
- No `print()` statements (must use `logging`)
- No hardcoded credentials or connection strings
- Explicit naming (no `data`, `tmp`, `x`, `df2`)
- Type hints on function signatures

### SQL / dbt files
- No `SELECT *`
- JOIN conditions verified (no cartesian products)
- NULL handling explicit (`COALESCE`, `NULLIF`)
- `ref()` and `source()` used — no hardcoded table names
- Schema tests in `_models.yml`
- `+database: pg` set for core/mart models

### Airflow DAGs
- No `Variable.get()` at module level
- Task dependencies explicit (`>>` or `chain()`)
- `retries`, `retry_delay`, `execution_timeout` in `default_args`
- Pool assignments match task type
- Asset outlets match the event chain

### Docker / Infrastructure
- No `:latest` tags
- `mem_limit` set for DockerOperator tasks
- Health checks present on all services

### Config / YAML
- No secrets in committed files
- `.example` files updated if new variables added

## Step 3 — Check commit hygiene

Run `git log master..HEAD --oneline` and verify:
- All commits follow conventional format: `type(scope): description`
- Commits are ordered by layer: infra → sources → staging → core → DAG → tests → docs
- No commits mixing unrelated changes

## Step 4 — Produce the review report

Output a structured report with:

1. **Summary** — branch name, number of commits, total lines changed
2. **Automated checks** — pass/fail for each `just review` step
3. **Diff analysis** — findings per file category (Python, SQL, DAG, Docker, Config)
4. **Commit hygiene** — conventional format compliance + layering order
5. **Issues found** — sorted by severity: CRITICAL > WARNING > INFO
6. **Verdict** — READY TO PUSH / NEEDS FIXES (with specific action items)

If issues are found, propose concrete fixes with code snippets.
