# Skill: Debug a Failing DAG

Systematic playbook for diagnosing and fixing failing Airflow DAGs.

## Step 1 — Identify the Failure Type

Open the Airflow UI at http://localhost:8080 and check:

| Symptom | Likely cause |
|---------|-------------|
| DAG not appearing in UI | Import error — parse failure |
| Task stuck in `queued` | Worker down, pool exhausted, or no healthy worker |
| Task `failed` immediately | DockerOperator startup error |
| Task `failed` after running | Processing error in the Docker container |
| DAG not triggered | Asset not emitted by upstream DAG |

## Step 2 — Check DAG Parsing

```bash
uv run pytest tests/unit/test_dags.py -v
```

If failing, also check:
```bash
just airflow-shell
python -c "from orchestration.dags.<dag_file> import dag; print(dag)"
```

Common parse errors:
- Import at module level that fails (missing env var, connection)
- `Variable.get()` called at module level (not inside a task)
- Circular import in `plugins/`

## Step 3 — Check Task Logs

In Airflow UI: click failed task → **Logs**. Or from the CLI:
```bash
just logs airflow-worker
just logs airflow-scheduler
```

For DockerOperator failures, the container logs are embedded in the task log. Look for:
```
docker: Error response from daemon: ...   ← Docker startup error
Error: command failed with exit code 1    ← Processing error inside container
```

## Step 4 — Diagnose by Operator Type

### DockerOperator failures

```bash
# Check the image exists and is healthy
docker images | grep <image_name>

# Run the container manually with the same command
docker run --rm --network data-engineering-pipeline_pipeline-network \
  -e POSTGRES_HOST=postgres \
  <image_name> <command>
```

Common causes:
- Image not built: run `just build-<image>`
- Missing environment variable: check `docker_default_vars` in the DAG
- Volume mount path wrong (Windows path vs container path)
- Network not found: run `just ensure-network`

### dbt (Cosmos DbtTaskGroup) failures

```bash
just dbt-run   # run dbt directly outside Airflow
```

Check `profiles.yml` S3/PG connection settings. Check `dbt_project.yml` for `+database: pg` on core/mart models.

### Asset not triggering downstream DAG

1. Confirm the producer task has `outlets=[MY_ASSET]` decorator.
2. Confirm the consumer DAG has `schedule=[MY_ASSET]`.
3. In Airflow UI → **Datasets** (or **Assets** in 3.x) — check if the asset was updated.
4. Check `orchestration/plugins/constants.py` — asset names must match exactly.

```python
# CORRECT — name is first positional arg, uri is keyword
MY_ASSET = Asset(name="staging_weather", uri="s3://...")

# WRONG — causes "multiple values for argument 'name'"
MY_ASSET = Asset("s3://...", name="staging_weather")
```

### Pool exhaustion

```bash
# Check pool status in Airflow UI: Admin → Pools
# Or via CLI:
just airflow-shell
airflow pools list
```

Increase pool size or check for stuck tasks holding slots.

## Step 5 — Re-run After Fix

- **Re-run a single task**: Airflow UI → click task → **Clear** → select "Current" only.
- **Re-run from a specific task**: Click task → **Clear** → "Downstream" checked.
- **Re-run the full DAG**: DAG page → **Trigger DAG** (manual) or wait for the asset to be re-emitted.
- **Force re-emit an Asset**: Clear the producer task and re-run it.

## Step 6 — Prevent Recurrence

- Add or improve the unit test in `tests/unit/test_dags.py`.
- If the fix involved a Docker image change, update `version-verification.md`.
- If it was a known Airflow 3.x gotcha, document it in `.claude/rules/airflow.md`.
