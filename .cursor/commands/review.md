# Self-Review — Pre-Push Gate

Perform a complete self-review of the current branch before pushing to remote.
Act as a strict external reviewer. Report findings in French.

## Step 1: Run automated checks

Run `just review` in the terminal and capture the full output.
Report each of the 7 steps with their pass/fail status.

## Step 2: Analyze the diff manually

Run `git diff master...HEAD` (or the appropriate base branch) and inspect every changed file.

For each file, check against the self-review checklist in `.cursor/rules/self-review.mdc`:

### Python files
- No `print()` statements (use `logging`)
- No hardcoded credentials or connection strings
- Explicit naming (no `data`, `tmp`, `x`, `df2`)
- No unnecessary complexity; DRY respected
- Type hints present on function signatures

### SQL / dbt files
- No `SELECT *`
- JOIN conditions verified (no cartesian products)
- NULL handling explicit (`COALESCE`, `NULLIF`)
- `ref()` and `source()` used (no hardcoded table names)
- Schema tests defined in `_models.yml`
- `+database: pg` set for core/mart models in `dbt_project.yml`

### Airflow DAGs
- No `Variable.get()` at module level
- Task dependencies explicit (`>>` or `chain()`)
- `retries`, `retry_delay`, `execution_timeout` in `default_args`
- Pool assignments match task type
- Asset outlets match the event chain

### Docker / Infrastructure
- No `:latest` tags
- `mem_limit` set for DockerOperator tasks
- Health checks present on services
- Entrypoint scripts handle CRLF (`sed -i 's/\r$//'`)

### Config / YAML
- No secrets in committed files
- `.example` files updated if new variables added

## Step 3: Check commit hygiene

Run `git log master..HEAD --oneline` and verify:
- All commits follow conventional format: `type(scope): description`
- Commits are ordered by layer (infra → sources → staging → core → DAG → tests → docs)
- No "fourre-tout" commits mixing unrelated changes

## Step 4: Produce the review report

Output a structured report in French with:

1. **Résumé** — branch name, number of commits, total lines changed
2. **Checks automatisés** — pass/fail for each `just review` step
3. **Analyse du diff** — findings per file category (Python, SQL, DAG, Docker, Config)
4. **Hygiène des commits** — conventional format compliance + layering order
5. **Problèmes détectés** — list of issues found, sorted by severity (CRITICAL > WARNING > INFO)
6. **Verdict** — READY TO PUSH / NEEDS FIXES (with specific action items)

If issues are found, propose concrete fixes with code snippets.
