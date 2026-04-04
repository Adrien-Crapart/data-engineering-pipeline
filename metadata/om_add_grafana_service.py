"""Add Grafana service to OpenMetadata with proper configuration.

This script creates a Grafana dashboard service in OpenMetadata 1.12.x
with the correct API format to avoid LinkedHashMap conversion errors.

Usage:
    python metadata/om_add_grafana_service.py --token glsa_xxxxx
"""
from __future__ import annotations

import argparse
import base64
import logging
import sys

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

OM_URL = "http://localhost:8585"
EMAIL = "admin@open-metadata.org"
PASSWORD = "admin"


class OMGrafanaServiceCreator:
    def __init__(self, base_url: str, grafana_token: str):
        self.base_url = base_url.rstrip("/")
        self.api = f"{self.base_url}/api/v1"
        self.token = None
        self.headers = {"Content-Type": "application/json"}
        self.grafana_token = grafana_token

    def authenticate(self) -> None:
        """Authenticate and get JWT token."""
        logger.info("Authenticating to OpenMetadata...")
        encoded_password = base64.b64encode(PASSWORD.encode()).decode()
        resp = requests.post(
            f"{self.base_url}/api/v1/users/login",
            json={"email": EMAIL, "password": encoded_password},
            timeout=30,
        )
        if resp.status_code == 200:
            self.token = resp.json()["accessToken"]
            self.headers["Authorization"] = f"Bearer {self.token}"
            logger.info("  ✓ Authentication successful")
        else:
            logger.error("  ✗ Authentication failed: %s", resp.text[:200])
            sys.exit(1)

    def _get(self, endpoint: str) -> requests.Response:
        """GET request wrapper."""
        return requests.get(f"{self.api}{endpoint}", headers=self.headers, timeout=30)

    def _put(self, endpoint: str, payload: dict) -> requests.Response:
        """PUT request wrapper."""
        return requests.put(
            f"{self.api}{endpoint}", headers=self.headers, json=payload, timeout=30
        )

    def _delete(self, endpoint: str) -> requests.Response:
        """DELETE request wrapper."""
        return requests.delete(f"{self.api}{endpoint}", headers=self.headers, timeout=30)

    def check_existing_service(self) -> str | None:
        """Check if Grafana service already exists."""
        logger.info("Checking for existing Grafana service...")
        resp = self._get("/services/dashboardServices/name/grafana")
        if resp.status_code == 200:
            service_id = resp.json()["id"]
            logger.info("  ⚠ Grafana service already exists (ID: %s)", service_id[:8])
            return service_id
        logger.info("  ✓ No existing Grafana service found")
        return None

    def delete_existing_service(self, service_id: str) -> None:
        """Delete existing Grafana service."""
        logger.info("Deleting existing Grafana service...")
        resp = self._delete(f"/services/dashboardServices/{service_id}?hardDelete=true")
        if resp.status_code in (200, 204):
            logger.info("  ✓ Existing service deleted")
        else:
            logger.warning("  ⚠ Could not delete service: %s", resp.text[:200])

    def create_grafana_service(self) -> str | None:
        """Create Grafana dashboard service with OM 1.12.x compatible format."""
        logger.info("Creating Grafana service...")

        # OM 1.12.x expects a specific structure for Grafana
        payload = {
            "name": "grafana",
            "displayName": "Grafana Monitoring",
            "description": "Grafana dashboards for monitoring the data pipeline (Airflow, Prometheus, PostgreSQL)",
            "serviceType": "Grafana",
            "connection": {
                "config": {
                    "type": "Grafana",
                    "hostPort": "http://grafana_monitoring:3000",
                    "apiKey": self.grafana_token,
                }
            },
        }

        resp = self._put("/services/dashboardServices", payload)

        if resp.status_code in (200, 201):
            service_data = resp.json()
            service_id = service_data.get("id")
            logger.info("  ✓ Grafana service created: %s", service_id[:8])
            return service_id
        else:
            logger.error("  ✗ Failed to create service (%s):", resp.status_code)
            logger.error("    %s", resp.text[:500])
            return None

    def test_connection(self, service_id: str) -> bool:
        """Test the Grafana connection."""
        logger.info("Testing Grafana connection...")

        # OM 1.12.x test connection endpoint
        resp = requests.post(
            f"{self.api}/services/dashboardServices/{service_id}/testConnection",
            headers=self.headers,
            timeout=30,
        )

        if resp.status_code == 200:
            result = resp.json()
            status = result.get("status", "unknown")
            if status == "successful":
                logger.info("  ✓ Connection test successful")
                return True
            else:
                logger.error("  ✗ Connection test failed: %s", result)
                return False
        else:
            logger.error("  ✗ Connection test failed (%s): %s", resp.status_code, resp.text[:200])
            return False

    def run(self, recreate: bool = False) -> None:
        """Main execution flow."""
        self.authenticate()

        # Check for existing service
        existing_id = self.check_existing_service()
        if existing_id:
            if recreate:
                self.delete_existing_service(existing_id)
            else:
                logger.info("Service already exists. Use --recreate to replace it.")
                return

        # Create service
        service_id = self.create_grafana_service()
        if not service_id:
            logger.error("Failed to create Grafana service")
            sys.exit(1)

        # Test connection
        if self.test_connection(service_id):
            logger.info("")
            logger.info("=" * 60)
            logger.info("GRAFANA SERVICE READY")
            logger.info("=" * 60)
            logger.info("Service ID: %s", service_id)
            logger.info("View in OM: %s/settings/services/dashboards/grafana", self.base_url)
            logger.info("")
            logger.info("Next steps:")
            logger.info("  1. Create a Metadata Agent to ingest dashboards")
            logger.info("  2. OM UI → Services → grafana → Agents → Add Agent")
            logger.info("  3. Type: Metadata, Schedule: 0 0 * * *")
        else:
            logger.warning("")
            logger.warning("Service created but connection test failed.")
            logger.warning("Check:")
            logger.warning("  - Grafana is running: docker ps | grep grafana")
            logger.warning("  - Token is valid: curl -H 'Authorization: Bearer %s' http://localhost:3000/api/org", self.grafana_token[:20] + "...")
            logger.warning("  - Network connectivity: docker exec openmetadata-server curl http://grafana_monitoring:3000/api/health")


def main() -> None:
    parser = argparse.ArgumentParser(description="Add Grafana service to OpenMetadata")
    parser.add_argument(
        "--token",
        required=True,
        help="Grafana Service Account Token (starts with glsa_)",
    )
    parser.add_argument(
        "--url",
        default=OM_URL,
        help="OpenMetadata URL (default: http://localhost:8585)",
    )
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Delete existing service and recreate",
    )
    args = parser.parse_args()

    if not args.token.startswith("glsa_"):
        logger.error("Invalid token format. Grafana Service Account Tokens start with 'glsa_'")
        sys.exit(1)

    creator = OMGrafanaServiceCreator(args.url, args.token)
    creator.run(recreate=args.recreate)


if __name__ == "__main__":
    main()
