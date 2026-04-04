#!/bin/bash
# Create Grafana Service Account Token for OpenMetadata

GRAFANA_URL="http://localhost:3000"
GRAFANA_USER="admin"
GRAFANA_PASSWORD="${GRAFANA_ADMIN_PASSWORD:-admin}"

echo "Creating Grafana Service Account for OpenMetadata..."

# Step 1: Create Service Account
SA_RESPONSE=$(curl -s -X POST "${GRAFANA_URL}/api/serviceaccounts" \
  -u "${GRAFANA_USER}:${GRAFANA_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "OpenMetadata",
    "role": "Viewer",
    "isDisabled": false
  }')

SA_ID=$(echo $SA_RESPONSE | jq -r '.id')

if [ "$SA_ID" = "null" ] || [ -z "$SA_ID" ]; then
  echo "Error creating service account:"
  echo $SA_RESPONSE | jq .
  exit 1
fi

echo "✓ Service Account created (ID: $SA_ID)"

# Step 2: Create Token
TOKEN_RESPONSE=$(curl -s -X POST "${GRAFANA_URL}/api/serviceaccounts/${SA_ID}/tokens" \
  -u "${GRAFANA_USER}:${GRAFANA_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "OpenMetadata-Token"
  }')

TOKEN=$(echo $TOKEN_RESPONSE | jq -r '.key')

if [ "$TOKEN" = "null" ] || [ -z "$TOKEN" ]; then
  echo "Error creating token:"
  echo $TOKEN_RESPONSE | jq .
  exit 1
fi

echo "✓ Token created successfully"
echo ""
echo "=========================================="
echo "GRAFANA SERVICE ACCOUNT TOKEN"
echo "=========================================="
echo "$TOKEN"
echo "=========================================="
echo ""
echo "Copy this token and use it in OpenMetadata"
echo "Host: grafana_monitoring:3000"
