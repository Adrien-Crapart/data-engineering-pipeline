# Scripts

Utility shell scripts for environment setup and manual pipeline operations.

## Purpose

Provides convenience scripts for common operations that are not part of the
automated pipeline. These are helpers for local development and manual debugging.

## Key Files

| File | Description |
|------|-------------|
| `setup_env.sh` | Checks prerequisites (Docker, git), creates `.env` from `.env.example` |
| `run_pipeline.sh` | Manually triggers the full pipeline via `docker compose exec` |

## Usage

```bash
# Initial setup
bash scripts/setup_env.sh

# Manual pipeline run
bash scripts/run_pipeline.sh
```

## Note

For production use, pipelines should be triggered via Airflow DAGs, not these
scripts. These are intended for local development and debugging only.
