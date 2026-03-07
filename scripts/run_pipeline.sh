#!/usr/bin/env bash
# ============================================================
# Run the full weather data pipeline manually
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
COMPOSE="docker compose --env-file $PROJECT_ROOT/.env -f $PROJECT_ROOT/infrastructure/docker/docker-compose.yml"

echo "=== Weather Data Pipeline - Manual Run ==="
echo ""

echo "[1/5] Running ingestion..."
$COMPOSE exec airflow-scheduler bash -c \
  "PYTHONPATH=/opt/airflow /usr/python/bin/python -c 'from ingestion.pipelines.openweather_pipeline import run_pipeline; print(run_pipeline())'"

echo ""
echo "[2/5] Running dbt deps..."
$COMPOSE exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt && /usr/python/bin/dbt deps --profiles-dir ."

echo ""
echo "[3/5] Running dbt transformations..."
$COMPOSE exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt && /usr/python/bin/dbt run --profiles-dir ."

echo ""
echo "[4/5] Running dbt tests..."
$COMPOSE exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt && /usr/python/bin/dbt test --profiles-dir ."

echo ""
echo "[5/5] Running Soda data quality checks..."
$COMPOSE exec airflow-scheduler bash -c \
  "/usr/python/bin/soda scan -d weather_db -c /opt/airflow/data_quality/soda/configuration.yml /opt/airflow/data_quality/soda/checks/"

echo ""
echo "=== Pipeline run complete ==="
