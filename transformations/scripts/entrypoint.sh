#!/usr/bin/env bash
set -euo pipefail

# Handle both "run --select staging" (direct) and "dbt run --select staging" (Cosmos)
DBT_CMD="${1:-run}"
shift || true

if [ "$DBT_CMD" = "dbt" ]; then
    DBT_CMD="${1:-run}"
    shift || true
fi

DBT_ARGS="${*}"
DBT_PROJECT_DIR="${DBT_PROJECT_DIR:-/app}"
EXIT_CODE=0

cd "$DBT_PROJECT_DIR"

# DuckDB httpfs needs the host:port without scheme
MINIO_RAW="${MINIO_ENDPOINT:-http://minio:9000}"
export MINIO_ENDPOINT_HOST="${MINIO_RAW#http://}"
MINIO_ENDPOINT_HOST="${MINIO_ENDPOINT_HOST#https://}"
export MINIO_ENDPOINT_HOST

# Use dbt-ol wrapper when OpenLineage transport is configured
DBT_BIN="dbt"
if [ -n "${OPENLINEAGE_URL:-}" ] || [ -n "${OPENLINEAGE_CONFIG:-}" ]; then
    if command -v dbt-ol &>/dev/null; then
        DBT_BIN="dbt-ol"
        echo "OpenLineage enabled — using dbt-ol wrapper"
    fi
fi

echo "=== dbt entrypoint ==="
echo "  command:       $DBT_CMD"
echo "  args:          $DBT_ARGS"
echo "  project dir:   $DBT_PROJECT_DIR"
echo "  profiles:      $DBT_PROJECT_DIR/profiles.yml"
echo "  minio host:    $MINIO_ENDPOINT_HOST"
echo "  dbt binary:    $DBT_BIN"
echo ""

wait_for_postgres() {
    local host="${POSTGRES_HOST:-postgres}"
    local port="${POSTGRES_PORT:-5432}"
    local max_retries=15
    local attempt=0
    echo "Waiting for PostgreSQL at $host:$port ..."
    while [ $attempt -lt $max_retries ]; do
        if pg_isready -h "$host" -p "$port" -q 2>/dev/null; then
            echo "  PostgreSQL ready"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 2
    done
    echo "  WARNING: PostgreSQL not reachable after ${max_retries} attempts"
    return 1
}

wait_for_postgres || true

has_profiles_dir() {
    echo "$DBT_ARGS" | grep -q "\-\-profiles-dir"
}

PROFILES_FLAG=""
if ! has_profiles_dir; then
    PROFILES_FLAG="--profiles-dir ."
fi

case "$DBT_CMD" in
    deps)
        echo "--- dbt deps ---"
        dbt deps $PROFILES_FLAG $DBT_ARGS || EXIT_CODE=$?
        ;;
    run)
        echo "--- dbt deps ---"
        dbt deps $PROFILES_FLAG
        echo "--- dbt run ---"
        $DBT_BIN run $PROFILES_FLAG $DBT_ARGS || EXIT_CODE=$?
        ;;
    test)
        echo "--- dbt deps ---"
        dbt deps $PROFILES_FLAG
        echo "--- dbt test ---"
        $DBT_BIN test $PROFILES_FLAG $DBT_ARGS || EXIT_CODE=$?
        ;;
    docs)
        echo "--- dbt deps ---"
        dbt deps $PROFILES_FLAG
        echo "--- dbt run (populate catalog for dbt-duckdb :memory:) ---"
        $DBT_BIN run $PROFILES_FLAG --select staging core marts analytic || true
        echo "--- dbt docs generate ---"
        dbt docs generate $PROFILES_FLAG $DBT_ARGS || EXIT_CODE=$?
        if [ $EXIT_CODE -eq 0 ]; then
            echo "--- uploading docs to S3 ---"
            python /app/scripts/upload_docs.py || echo "  WARNING: docs upload failed"
        fi
        ;;
    build)
        echo "--- dbt build (deps + run + test) ---"
        dbt deps $PROFILES_FLAG
        $DBT_BIN run $PROFILES_FLAG $DBT_ARGS || EXIT_CODE=$?
        if [ $EXIT_CODE -eq 0 ]; then
            $DBT_BIN test $PROFILES_FLAG $DBT_ARGS || EXIT_CODE=$?
        fi
        ;;
    *)
        echo "--- dbt $DBT_CMD ---"
        $DBT_BIN "$DBT_CMD" $PROFILES_FLAG $DBT_ARGS || EXIT_CODE=$?
        ;;
esac

echo ""
echo "=== dbt $DBT_CMD finished with exit code $EXIT_CODE ==="
exit $EXIT_CODE
