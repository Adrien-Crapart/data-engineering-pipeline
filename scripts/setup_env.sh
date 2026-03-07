#!/usr/bin/env bash
# ============================================================
# Environment Setup Script
# ============================================================
# Creates .env from .env.example and validates prerequisites.
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=== Weather Pipeline - Environment Setup ==="
echo ""

# Check prerequisites
for cmd in docker git; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "ERROR: $cmd is not installed. Please install it first."
    exit 1
  fi
done

echo "[OK] Docker and Git are installed."

# Check Docker is running
if ! docker info &>/dev/null; then
  echo "ERROR: Docker daemon is not running. Please start Docker."
  exit 1
fi
echo "[OK] Docker daemon is running."

# Create .env if it doesn't exist
ENV_FILE="$PROJECT_ROOT/.env"
ENV_EXAMPLE="$PROJECT_ROOT/.env.example"

if [ -f "$ENV_FILE" ]; then
  echo "[OK] .env file already exists."
else
  if [ -f "$ENV_EXAMPLE" ]; then
    cp "$ENV_EXAMPLE" "$ENV_FILE"
    echo "[OK] Created .env from .env.example"
    echo ""
    echo "IMPORTANT: Edit .env and set your OPENWEATHER_API_KEY"
    echo "  Get a free key at: https://openweathermap.org/api"
  else
    echo "ERROR: .env.example not found."
    exit 1
  fi
fi

echo ""
echo "=== Setup complete ==="
echo ""
echo "Next steps:"
echo "  1. Edit .env with your OpenWeather API key"
echo "  2. Run: make up"
echo "  3. Open: http://localhost:8080 (airflow/airflow)"
