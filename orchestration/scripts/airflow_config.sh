#!/usr/bin/env bash
set -euo pipefail

export PATH="/home/airflow/.local/bin:${PATH}"
export PYTHONPATH="/home/airflow/.local/lib/python3.12/site-packages:${PYTHONPATH:-}"

# ============================================================
# Airflow Configuration Manager
# Handles import/export of connections, variables, and pools.
#
# Usage:
#   ./airflow_config.sh import   # Load config into Airflow
#   ./airflow_config.sh export   # Save current config to files
# ============================================================

CONFIG_DIR="${AIRFLOW_CONFIG_DIR:-/opt/airflow/config}"
CONNECTIONS_FILE="${CONFIG_DIR}/connections.yaml"
VARIABLES_FILE="${CONFIG_DIR}/variables.json"
POOLS_FILE="${CONFIG_DIR}/pools.json"
CONNECTIONS_EXAMPLE="${CONFIG_DIR}/connections.yaml.example"
VARIABLES_EXAMPLE="${CONFIG_DIR}/variables.json.example"

log() { echo "[airflow_config] $(date '+%H:%M:%S') $*"; }

wait_for_db() {
    local host="${POSTGRES_HOST:-postgres}"
    local port="${POSTGRES_PORT:-5432}"
    local retries=15
    local attempt=0
    log "Waiting for PostgreSQL at $host:$port ..."
    while [ $attempt -lt $retries ]; do
        if python3 -c "import socket; s=socket.socket(); s.settimeout(2); s.connect(('$host',${port})); s.close()" 2>/dev/null; then
            log "Database ready"
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 1
    done
    log "WARNING: DB not reachable after ${retries} attempts — proceeding anyway"
    return 0
}

import_pools() {
    if [ -f "$POOLS_FILE" ]; then
        log "Importing pools from ${POOLS_FILE}..."
        airflow pools import "$POOLS_FILE" 2>&1 || log "WARNING: Some pools may already exist"
        log "Pools imported"
    else
        log "No pools file found at ${POOLS_FILE} — skipping"
    fi
}

import_connections() {
    local file="$CONNECTIONS_FILE"
    if [ ! -f "$file" ] && [ -f "$CONNECTIONS_EXAMPLE" ]; then
        log "No connections.yaml found — copying from example"
        cp "$CONNECTIONS_EXAMPLE" "$file"
    fi
    if [ -f "$file" ]; then
        log "Importing connections from ${file}..."
        airflow connections import "$file" --overwrite 2>&1 || log "WARNING: Connection import had issues"
        log "Connections imported"
    else
        log "No connections file found — skipping"
    fi
}

import_variables() {
    local file="$VARIABLES_FILE"
    if [ ! -f "$file" ] && [ -f "$VARIABLES_EXAMPLE" ]; then
        log "No variables.json found — copying from example"
        cp "$VARIABLES_EXAMPLE" "$file"
    fi
    if [ -f "$file" ]; then
        log "Importing variables from ${file}..."
        airflow variables import "$file" 2>&1 || log "WARNING: Variable import had issues"
        log "Variables imported"
    else
        log "No variables file found — skipping"
    fi
}

export_connections() {
    log "Exporting connections to ${CONNECTIONS_FILE}..."
    airflow connections export "$CONNECTIONS_FILE" --format yaml 2>&1
    log "Connections exported to ${CONNECTIONS_FILE}"
}

export_variables() {
    log "Exporting variables to ${VARIABLES_FILE}..."
    airflow variables export "$VARIABLES_FILE" 2>&1
    log "Variables exported to ${VARIABLES_FILE}"
}

export_pools() {
    log "Exporting pools to ${POOLS_FILE}..."
    airflow pools export "$POOLS_FILE" 2>&1
    log "Pools exported to ${POOLS_FILE}"
}

do_import() {
    log "=== Importing Airflow configuration ==="
    wait_for_db
    import_pools
    import_connections
    import_variables
    log "=== Configuration import complete ==="
}

do_export() {
    log "=== Exporting Airflow configuration ==="
    wait_for_db
    export_connections
    export_variables
    export_pools
    log "=== Configuration export complete ==="
}

case "${1:-import}" in
    import)  do_import  ;;
    export)  do_export  ;;
    *)
        echo "Usage: $0 {import|export}"
        exit 1
        ;;
esac
