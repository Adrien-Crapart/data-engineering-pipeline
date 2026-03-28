#!/usr/bin/env bash
set -euo pipefail

DBT_CMD="${1:-run}"
shift || true
DBT_ARGS="${*:---profiles-dir .}"
DBT_PROJECT_DIR="${DBT_PROJECT_DIR:-/app}"
EXIT_CODE=0

cd "$DBT_PROJECT_DIR"

echo "=== dbt entrypoint ==="
echo "  command:     $DBT_CMD"
echo "  args:        $DBT_ARGS"
echo "  project dir: $DBT_PROJECT_DIR"
echo "  profiles:    $DBT_PROJECT_DIR/profiles.yml"
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

case "$DBT_CMD" in
    deps)
        echo "--- dbt deps ---"
        dbt deps --profiles-dir . $DBT_ARGS || EXIT_CODE=$?
        ;;
    run)
        echo "--- dbt run ---"
        dbt run --profiles-dir . $DBT_ARGS || EXIT_CODE=$?
        ;;
    test)
        echo "--- dbt test ---"
        dbt test --profiles-dir . $DBT_ARGS || EXIT_CODE=$?
        ;;
    docs)
        echo "--- dbt docs generate ---"
        dbt docs generate --profiles-dir . $DBT_ARGS || EXIT_CODE=$?
        if [ $EXIT_CODE -eq 0 ]; then
            echo "--- uploading docs to S3 ---"
            python /app/scripts/upload_docs.py || echo "  WARNING: docs upload failed"
        fi
        ;;
    build)
        echo "--- dbt build (deps + run + test) ---"
        dbt deps --profiles-dir .
        dbt run --profiles-dir . $DBT_ARGS || EXIT_CODE=$?
        if [ $EXIT_CODE -eq 0 ]; then
            dbt test --profiles-dir . $DBT_ARGS || EXIT_CODE=$?
        fi
        ;;
    *)
        echo "--- dbt $DBT_CMD ---"
        dbt "$DBT_CMD" --profiles-dir . $DBT_ARGS || EXIT_CODE=$?
        ;;
esac

echo ""
echo "=== dbt $DBT_CMD finished with exit code $EXIT_CODE ==="
exit $EXIT_CODE
