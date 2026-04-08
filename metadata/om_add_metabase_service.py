"""Add Metabase service to OpenMetadata with proper configuration.

Registers Metabase as a dashboard service in OM 1.12.x so that
questions and dashboards are ingested into the data catalog.

Usage:
    python metadata/om_add_metabase_service.py [--url http://localhost:8585] [--recreate]
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

METABASE_HOST = "http://metabase:3000"
METABASE_USER = "admin@weather-corp.com"
METABASE_PASSWORD = "admin"


class OMMetabaseServiceCreator:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.api = f"{self.base_url}/api/v1"
        self.token: str | None = None
        self.headers: dict[str, str] = {"Content-Type": "application/json"}

    def authenticate(self) -> None:
        logger.info("Authenticating to OpenMetadata...")
        encoded_password = base64.b64encode(PASSWORD.encode()).decode()
        resp = requests.post(
            f"{self.api}/users/login",
            json={"email": EMAIL, "password": encoded_password},
            timeout=30,
        )
        if resp.status_code == 200:
            self.token = resp.json()["accessToken"]
            self.headers["Authorization"] = f"Bearer {self.token}"
            logger.info("  Authenticated")
        else:
            logger.error("  Authentication failed: %s", resp.text[:200])
            sys.exit(1)

    def check_existing(self) -> str | None:
        logger.info("Checking for existing Metabase service...")
        resp = requests.get(
            f"{self.api}/services/dashboardServices/name/metabase",
            headers=self.headers,
            timeout=15,
        )
        if resp.status_code == 200:
            sid = resp.json()["id"]
            logger.info("  Metabase service already exists (id=%s)", sid[:8])
            return sid
        return None

    def delete_existing(self, service_id: str) -> None:
        logger.info("Deleting existing Metabase service...")
        requests.delete(
            f"{self.api}/services/dashboardServices/{service_id}?hardDelete=true",
            headers=self.headers,
            timeout=15,
        )

    def create_service(self) -> str | None:
        logger.info("Creating Metabase service...")
        payload = {
            "name": "metabase",
            "displayName": "Metabase BI",
            "description": "Metabase dashboards and questions for weather data analysis",
            "serviceType": "Metabase",
            "connection": {
                "config": {
                    "type": "Metabase",
                    "hostPort": METABASE_HOST,
                    "username": METABASE_USER,
                    "password": METABASE_PASSWORD,
                }
            },
        }

        resp = requests.put(
            f"{self.api}/services/dashboardServices",
            headers=self.headers,
            json=payload,
            timeout=30,
        )

        if resp.status_code in (200, 201):
            sid = resp.json()["id"]
            logger.info("  Metabase service created (id=%s)", sid[:8])
            return sid

        logger.error("  Failed (%s): %s", resp.status_code, resp.text[:300])
        return None

    def run(self, recreate: bool = False) -> None:
        self.authenticate()

        existing = self.check_existing()
        if existing and not recreate:
            logger.info("Service exists. Use --recreate to replace.")
            return
        if existing and recreate:
            self.delete_existing(existing)

        service_id = self.create_service()
        if not service_id:
            sys.exit(1)

        logger.info("")
        logger.info("=" * 60)
        logger.info("METABASE SERVICE READY")
        logger.info("=" * 60)
        logger.info("Service ID: %s", service_id[:8])
        logger.info("View in OM: %s/settings/services/dashboards/metabase", self.base_url)
        logger.info("")
        logger.info("Next steps:")
        logger.info("  1. OM UI → Services → metabase → Agents → Add Agent")
        logger.info("  2. Type: Metadata, Schedule: 0 0 * * *")


def main() -> None:
    parser = argparse.ArgumentParser(description="Add Metabase to OpenMetadata")
    parser.add_argument("--url", default=OM_URL)
    parser.add_argument("--recreate", action="store_true")
    args = parser.parse_args()

    creator = OMMetabaseServiceCreator(args.url)
    creator.run(recreate=args.recreate)


if __name__ == "__main__":
    main()
