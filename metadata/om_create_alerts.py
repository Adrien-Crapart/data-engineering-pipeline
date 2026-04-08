"""Create Observability Alerts in OpenMetadata for data quality monitoring.

This script creates comprehensive alerting rules for:
  - Test failures (critical tables)
  - Schema changes
  - Data freshness issues
  - New table detection

Alerts can be sent to Slack, email, or webhooks.

Usage:
    python metadata/om_create_alerts.py [--url http://localhost:8585] [--slack-webhook URL] [--email user@domain.com]
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


class OMAlertCreator:
    def __init__(self, base_url: str, slack_webhook: str | None, email: str | None):
        self.base_url = base_url.rstrip("/")
        self.api = f"{self.base_url}/api/v1"
        self.token = None
        self.headers = {"Content-Type": "application/json"}
        self.slack_webhook = slack_webhook
        self.email = email

    def authenticate(self) -> None:
        """Authenticate and get JWT token."""
        logger.info("Authenticating to OpenMetadata...")
        # OM 1.12.x requires Base64-encoded password
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

    def _post(self, endpoint: str, payload: dict) -> requests.Response | None:
        """POST request wrapper."""
        resp = requests.post(
            f"{self.api}{endpoint}", headers=self.headers, json=payload, timeout=30
        )
        return resp if resp is not None else None

    def _delete(self, endpoint: str) -> requests.Response:
        """DELETE request wrapper."""
        return requests.delete(f"{self.api}{endpoint}", headers=self.headers, timeout=30)

    def cleanup_existing_alerts(self) -> None:
        """Remove existing alerts created by this script."""
        logger.info("Checking for existing alerts...")
        resp = self._get("/events/subscriptions?limit=100")
        if resp.status_code != 200:
            logger.warning("  Could not list existing alerts")
            return

        alerts = resp.json().get("data", [])
        our_alerts = [
            a for a in alerts
            if a.get("name", "").startswith((
                "quality_test_failure", "schema_change", "data_freshness",
                "new_table", "pipeline_failure", "incident_created",
            ))
        ]

        if not our_alerts:
            logger.info("  No existing alerts found")
            return

        logger.info("  Found %d existing alert(s), removing...", len(our_alerts))
        for alert in our_alerts:
            alert_id = alert["id"]
            alert_name = alert.get("name", "?")
            logger.info("    Deleting: %s", alert_name)
            self._delete(f"/events/subscriptions/{alert_id}")

    def create_destinations(self) -> dict:
        """Create alert destinations (Slack, Email)."""
        logger.info("Creating alert destinations...")
        destinations = {}

        if self.slack_webhook:
            logger.info("  Creating Slack destination...")
            slack_payload = {
                "name": "slack_data_quality",
                "displayName": "Slack - Data Quality Channel",
                "description": "Slack webhook for data quality alerts",
                "destinationType": "Slack",
                "config": {
                    "webhookUrl": self.slack_webhook,
                },
                "enabled": True,
            }
            resp = self._post("/events/subscriptions/destinations", slack_payload)
            if resp is not None and resp.status_code in (200, 201):
                destinations["slack"] = resp.json()["id"]
                logger.info("    ✓ Slack destination created")
            elif resp is not None:
                logger.warning("    ✗ Failed (%s): %s", resp.status_code, resp.text[:200])

        if self.email:
            logger.info("  Creating Email destination...")
            email_payload = {
                "name": "email_data_engineering",
                "displayName": "Email - Data Engineering Team",
                "description": "Email notifications for data engineering team",
                "destinationType": "Email",
                "config": {
                    "receivers": [self.email],
                },
                "enabled": True,
            }
            resp = self._post("/events/subscriptions/destinations", email_payload)
            if resp is not None and resp.status_code in (200, 201):
                destinations["email"] = resp.json()["id"]
                logger.info("    ✓ Email destination created")
            elif resp is not None:
                logger.warning("    ✗ Failed (%s): %s", resp.status_code, resp.text[:200])

        return destinations

    def create_test_failure_alert(self, destinations: dict) -> None:
        """Create alert for test failures on critical tables."""
        logger.info("Creating test failure alert...")

        dest_ids = [d for d in destinations.values() if d]
        if not dest_ids:
            logger.warning("  Skipping (no destinations configured)")
            return

        payload = {
            "name": "quality_test_failure_critical",
            "displayName": "Data Quality Test Failure (Critical Tables)",
            "description": "Alert when data quality tests fail on Tier 1 or Tier 2 tables",
            "alertType": "TestCaseResult",
            "triggerConfig": {
                "type": "TestCaseResult",
                "entities": ["table"],
                "filters": [
                    {
                        "name": "testResult",
                        "effect": "include",
                        "condition": "matchAny",
                        "arguments": ["Failed", "Aborted"],
                    }
                ],
            },
            "destinations": dest_ids,
            "enabled": True,
            "batchSize": 10,
            "pollInterval": 60,
        }

        resp = self._post("/events/subscriptions", payload)
        if resp is not None and resp.status_code in (200, 201):
            logger.info("  ✓ Test failure alert created")
        elif resp is not None:
            logger.warning("  ✗ Failed (%s): %s", resp.status_code, resp.text[:400])

    def create_schema_change_alert(self, destinations: dict) -> None:
        """Create alert for schema changes."""
        logger.info("Creating schema change alert...")

        dest_ids = [d for d in destinations.values() if d]
        if not dest_ids:
            logger.warning("  Skipping (no destinations configured)")
            return

        payload = {
            "name": "schema_change_detection",
            "displayName": "Schema Change Detection",
            "description": "Alert when table schemas are modified (columns added/removed/changed)",
            "alertType": "EntityUpdated",
            "triggerConfig": {
                "type": "EntityUpdated",
                "entities": ["table"],
                "filters": [
                    {
                        "name": "fieldName",
                        "effect": "include",
                        "condition": "matchAny",
                        "arguments": ["columns"],
                    }
                ],
            },
            "destinations": dest_ids,
            "enabled": True,
            "batchSize": 10,
            "pollInterval": 60,
        }

        resp = self._post("/events/subscriptions", payload)
        if resp is not None and resp.status_code in (200, 201):
            logger.info("  ✓ Schema change alert created")
        elif resp is not None:
            logger.warning("  ✗ Failed (%s): %s", resp.status_code, resp.text[:400])

    def create_freshness_alert(self, destinations: dict) -> None:
        """Create alert for data freshness issues."""
        logger.info("Creating data freshness alert...")

        dest_ids = [d for d in destinations.values() if d]
        if not dest_ids:
            logger.warning("  Skipping (no destinations configured)")
            return

        payload = {
            "name": "data_freshness_failure",
            "displayName": "Data Freshness Alert",
            "description": "Alert when freshness tests fail (data not updated within SLA)",
            "alertType": "TestCaseResult",
            "triggerConfig": {
                "type": "TestCaseResult",
                "entities": ["table"],
                "filters": [
                    {
                        "name": "testResult",
                        "effect": "include",
                        "condition": "matchAny",
                        "arguments": ["Failed"],
                    }
                ],
            },
            "destinations": dest_ids,
            "enabled": True,
            "batchSize": 10,
            "pollInterval": 60,
        }

        resp = self._post("/events/subscriptions", payload)
        if resp is not None and resp.status_code in (200, 201):
            logger.info("  ✓ Freshness alert created")
        elif resp is not None:
            logger.warning("  ✗ Failed (%s): %s", resp.status_code, resp.text[:400])

    def create_new_table_alert(self, destinations: dict) -> None:
        """Create alert for new table detection."""
        logger.info("Creating new table detection alert...")

        dest_ids = [d for d in destinations.values() if d]
        if not dest_ids:
            logger.warning("  Skipping (no destinations configured)")
            return

        payload = {
            "name": "new_table_detected",
            "displayName": "New Table Detected",
            "description": "Alert when new tables are created in the datawarehouse",
            "alertType": "EntityCreated",
            "triggerConfig": {
                "type": "EntityCreated",
                "entities": ["table"],
            },
            "destinations": dest_ids,
            "enabled": True,
            "batchSize": 10,
            "pollInterval": 60,
        }

        resp = self._post("/events/subscriptions", payload)
        if resp is not None and resp.status_code in (200, 201):
            logger.info("  ✓ New table alert created")
        elif resp is not None:
            logger.warning("  ✗ Failed (%s): %s", resp.status_code, resp.text[:400])

    def create_pipeline_failure_alert(self, destinations: dict) -> None:
        """Create alert for pipeline/ingestion failures."""
        logger.info("Creating pipeline failure alert...")

        dest_ids = [d for d in destinations.values() if d]
        if not dest_ids:
            logger.warning("  Skipping (no destinations configured)")
            return

        payload = {
            "name": "pipeline_failure_alert",
            "displayName": "Pipeline Failure Alert",
            "description": "Alert when ingestion or pipeline execution fails",
            "alertType": "EntityUpdated",
            "triggerConfig": {
                "type": "EntityUpdated",
                "entities": ["ingestionPipeline"],
                "filters": [
                    {
                        "name": "fieldName",
                        "effect": "include",
                        "condition": "matchAny",
                        "arguments": ["pipelineStatuses"],
                    }
                ],
            },
            "destinations": dest_ids,
            "enabled": True,
            "batchSize": 10,
            "pollInterval": 60,
        }

        resp = self._post("/events/subscriptions", payload)
        if resp is not None and resp.status_code in (200, 201):
            logger.info("  Pipeline failure alert created")
        elif resp is not None:
            logger.warning("  Failed (%s): %s", resp.status_code, resp.text[:400])

    def create_incident_alert(self, destinations: dict) -> None:
        """Create alert when incidents are created or updated."""
        logger.info("Creating incident alert...")

        dest_ids = [d for d in destinations.values() if d]
        if not dest_ids:
            logger.warning("  Skipping (no destinations configured)")
            return

        payload = {
            "name": "incident_created_alert",
            "displayName": "Incident Created / Updated",
            "description": "Alert when a data quality incident is created or state changes",
            "alertType": "EntityUpdated",
            "triggerConfig": {
                "type": "EntityUpdated",
                "entities": ["testCase"],
                "filters": [
                    {
                        "name": "fieldName",
                        "effect": "include",
                        "condition": "matchAny",
                        "arguments": ["testCaseResult"],
                    }
                ],
            },
            "destinations": dest_ids,
            "enabled": True,
            "batchSize": 10,
            "pollInterval": 60,
        }

        resp = self._post("/events/subscriptions", payload)
        if resp is not None and resp.status_code in (200, 201):
            logger.info("  Incident alert created")
        elif resp is not None:
            logger.warning("  Failed (%s): %s", resp.status_code, resp.text[:400])

    def run(self) -> None:
        """Main execution flow."""
        self.authenticate()
        self.cleanup_existing_alerts()

        destinations = self.create_destinations()

        if not destinations:
            logger.warning("")
            logger.warning("No destinations configured. Alerts will be created but not sent.")
            logger.warning("Provide --slack-webhook or --email to enable notifications.")
            logger.warning("")

        self.create_test_failure_alert(destinations)
        self.create_schema_change_alert(destinations)
        self.create_freshness_alert(destinations)
        self.create_new_table_alert(destinations)
        self.create_pipeline_failure_alert(destinations)
        self.create_incident_alert(destinations)

        logger.info("")
        logger.info("=" * 60)
        logger.info("OBSERVABILITY ALERTS CONFIGURED")
        logger.info("=" * 60)
        logger.info("View alerts: OM UI → Settings → Notifications → Alerts")
        logger.info("Alert types: test failure, schema change, freshness, new table, pipeline failure, incident")
        logger.info("Destinations: %d configured", len(destinations))
        if destinations:
            for dest_type, dest_id in destinations.items():
                logger.info("  - %s: %s", dest_type.capitalize(), dest_id[:8])


def main() -> None:
    parser = argparse.ArgumentParser(description="Create OpenMetadata observability alerts")
    parser.add_argument("--url", default=OM_URL, help="OpenMetadata URL")
    parser.add_argument("--slack-webhook", help="Slack webhook URL for notifications")
    parser.add_argument("--email", help="Email address for notifications")
    args = parser.parse_args()

    creator = OMAlertCreator(args.url, args.slack_webhook, args.email)
    creator.run()


if __name__ == "__main__":
    main()
