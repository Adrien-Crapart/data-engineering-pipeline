#!/usr/bin/env bash
set -euo pipefail

echo "=== DLT Ingestion Entrypoint ==="
echo "  args: $*"
echo "  PYTHONPATH: ${PYTHONPATH:-not set}"
echo ""

wait_for_service() {
    local name="$1"
    local host="$2"
    local port="$3"
    local max_retries="${4:-15}"
    local attempt=0
    echo "Waiting for $name at $host:$port ..."
    while [ $attempt -lt $max_retries ]; do
        if python -u -c "import socket; s=socket.socket(); s.settimeout(2); s.connect(('$host',int('$port'))); s.close(); print('  $name ready')" 2>/dev/null; then
            return 0
        fi
        attempt=$((attempt + 1))
        echo "  attempt $attempt/$max_retries ..."
        sleep 1
    done
    echo "  WARNING: $name not reachable after $max_retries attempts — continuing anyway"
    return 0
}

PG_HOST="${POSTGRES_HOST:-postgres}"
PG_PORT="${POSTGRES_PORT:-5432}"
MINIO_RAW="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_HOST=$(echo "$MINIO_RAW" | sed 's|https\?://||' | cut -d: -f1)
MINIO_PORT=$(echo "$MINIO_RAW" | sed 's|https\?://||' | cut -d: -f2)
MINIO_PORT="${MINIO_PORT:-9000}"

wait_for_service "PostgreSQL" "$PG_HOST" "$PG_PORT" 15
wait_for_service "MinIO" "$MINIO_HOST" "$MINIO_PORT" 10

echo ""
echo "--- Starting ingestion pipeline ---"
exec python -u -m ingestion.cli "$@"
