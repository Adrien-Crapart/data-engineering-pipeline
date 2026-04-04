"""Create Grafana Service Account Token for OpenMetadata integration."""
import os
import sys

import requests

GRAFANA_URL = "http://localhost:3000"
GRAFANA_USER = "admin"
GRAFANA_PASSWORD = os.getenv("GRAFANA_ADMIN_PASSWORD", "admin")


def create_service_account():
    """Create a service account in Grafana."""
    print("Creating Grafana Service Account for OpenMetadata...")

    # Step 1: Check if service account already exists
    resp = requests.get(
        f"{GRAFANA_URL}/api/serviceaccounts/search?query=OpenMetadata",
        auth=(GRAFANA_USER, GRAFANA_PASSWORD),
        timeout=30,
    )

    sa_id = None
    if resp.status_code == 200:
        accounts = resp.json().get("serviceAccounts", [])
        if accounts:
            sa_id = accounts[0].get("id")
            print(f"[INFO] Service Account already exists (ID: {sa_id})")

    # Step 2: Create Service Account if it doesn't exist
    if not sa_id:
        resp = requests.post(
            f"{GRAFANA_URL}/api/serviceaccounts",
            auth=(GRAFANA_USER, GRAFANA_PASSWORD),
            json={
                "name": "OpenMetadata",
                "role": "Viewer",
                "isDisabled": False,
            },
            timeout=30,
        )

        if resp.status_code not in (200, 201):
            print(f"[ERROR] Error creating service account ({resp.status_code}):")
            print(resp.text)
            sys.exit(1)

        sa_data = resp.json()
        sa_id = sa_data.get("id")
        print(f"[OK] Service Account created (ID: {sa_id})")

    # Step 3: Create Token
    resp = requests.post(
        f"{GRAFANA_URL}/api/serviceaccounts/{sa_id}/tokens",
        auth=(GRAFANA_USER, GRAFANA_PASSWORD),
        json={"name": "OpenMetadata-Token"},
        timeout=30,
    )

    if resp.status_code not in (200, 201):
        print(f"[ERROR] Error creating token ({resp.status_code}):")
        print(resp.text)
        sys.exit(1)

    token_data = resp.json()
    token = token_data.get("key")

    print("[OK] Token created successfully\n")
    print("=" * 60)
    print("GRAFANA SERVICE ACCOUNT TOKEN")
    print("=" * 60)
    print(token)
    print("=" * 60)
    print("\nCopy this token and use it in OpenMetadata")
    print("Host: grafana_monitoring:3000")
    print("\nNote: Save this token securely - it won't be shown again!")


if __name__ == "__main__":
    create_service_account()
