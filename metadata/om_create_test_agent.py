"""Create and deploy a TestSuite agent in OpenMetadata for scheduled test execution.

The TestSuite agent runs OM-native tests (created via API or UI) on a schedule.
This enables users to define custom tests in the OM UI and have them executed
automatically, populating the Data Observability > Data Quality tab.

Usage:
    python metadata/om_create_test_agent.py [--url http://localhost:8585] [--schedule "0 */6 * * *"]
"""
from __future__ import annotations

import argparse
import logging
import sys

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

OM_URL = "http://localhost:8585"
EMAIL = "admin@open-metadata.org"
PASSWORD = "YWRtaW4="

DB_SERVICE = "datawarehouse"
DB_FQN = "datawarehouse.datawarehouse"


class OMTestAgentCreator:
    def __init__(self, base_url: str, schedule: str):
        self.base_url = base_url.rstrip("/")
        self.api = f"{self.base_url}/api/v1"
        self.token: str | None = None
        self.schedule = schedule

    def authenticate(self) -> None:
        resp = requests.post(
            f"{self.api}/users/login",
            json={"email": EMAIL, "password": PASSWORD},
            timeout=15,
        )
        resp.raise_for_status()
        self.token = resp.json()["accessToken"]
        logger.info("Authenticated to %s", self.base_url)

    @property
    def headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _get_service_id(self) -> str | None:
        resp = requests.get(
            f"{self.api}/services/databaseServices/name/{DB_SERVICE}",
            headers=self.headers,
            timeout=15,
        )
        if resp.status_code == 200:
            return resp.json()["id"]
        logger.error("Database service '%s' not found", DB_SERVICE)
        return None

    def _cleanup_existing_agents(self) -> None:
        logger.info("Checking for existing TestSuite agents...")
        resp = requests.get(
            f"{self.api}/services/ingestionPipelines?limit=100",
            headers=self.headers,
            timeout=15,
        )
        if resp.status_code != 200:
            return

        for pipeline in resp.json().get("data", []):
            if pipeline.get("pipelineType") == "TestSuite":
                pid = pipeline["id"]
                pname = pipeline.get("name", "?")
                logger.info("  Deleting existing TestSuite agent: %s", pname)
                requests.delete(
                    f"{self.api}/services/ingestionPipelines/{pid}?hardDelete=true",
                    headers=self.headers,
                    timeout=15,
                )

    def create_agent(self, service_id: str) -> str | None:
        logger.info("Creating TestSuite agent (schedule: %s)...", self.schedule)

        payload = {
            "name": f"test_suite_agent_{DB_SERVICE}",
            "displayName": f"TestSuite Agent — {DB_SERVICE}",
            "pipelineType": "TestSuite",
            "service": {"id": service_id, "type": "databaseService"},
            "sourceConfig": {
                "config": {
                    "type": "TestSuite",
                    "entityFullyQualifiedName": DB_FQN,
                }
            },
            "airflowConfig": {
                "pausePipeline": False,
                "concurrency": 1,
                "scheduleInterval": self.schedule,
                "startDate": "2026-04-01",
                "retries": 1,
            },
        }

        resp = requests.post(
            f"{self.api}/services/ingestionPipelines",
            headers=self.headers,
            json=payload,
            timeout=30,
        )

        if resp.status_code in (200, 201):
            agent_id = resp.json()["id"]
            logger.info("  TestSuite agent created (id=%s)", agent_id[:8])
            return agent_id

        if resp.status_code in (400, 409):
            logger.info("  Agent already exists or config conflict — trying PUT")
            resp2 = requests.put(
                f"{self.api}/services/ingestionPipelines",
                headers=self.headers,
                json=payload,
                timeout=30,
            )
            if resp2.status_code in (200, 201):
                agent_id = resp2.json()["id"]
                logger.info("  TestSuite agent created via PUT (id=%s)", agent_id[:8])
                return agent_id
            logger.warning("  PUT also failed (%s): %s", resp2.status_code, resp2.text[:300])
            return None

        logger.error("  Failed (%s): %s", resp.status_code, resp.text[:300])
        return None

    def deploy_agent(self, agent_id: str) -> bool:
        logger.info("Deploying TestSuite agent...")
        resp = requests.post(
            f"{self.api}/services/ingestionPipelines/deploy/{agent_id}",
            headers=self.headers,
            timeout=60,
        )
        if resp.status_code in (200, 201):
            logger.info("  Agent deployed successfully")
            return True

        logger.warning("  Deploy failed (%s): %s", resp.status_code, resp.text[:300])
        logger.info("  This is expected if OM's internal Airflow DB is not initialized.")
        logger.info("  Fix: docker exec openmetadata_ingestion_metadata airflow db migrate")
        return False

    def trigger_agent(self, agent_id: str) -> bool:
        logger.info("Triggering TestSuite agent...")
        resp = requests.post(
            f"{self.api}/services/ingestionPipelines/trigger/{agent_id}",
            headers=self.headers,
            json={},
            timeout=30,
        )
        if resp.status_code in (200, 201):
            logger.info("  Agent triggered successfully")
            return True

        logger.warning("  Trigger failed (%s): %s", resp.status_code, resp.text[:300])
        return False

    def run(self) -> None:
        self.authenticate()

        service_id = self._get_service_id()
        if not service_id:
            sys.exit(1)

        self._cleanup_existing_agents()

        agent_id = self.create_agent(service_id)
        if not agent_id:
            logger.error("Failed to create TestSuite agent")
            sys.exit(1)

        deployed = self.deploy_agent(agent_id)
        if deployed:
            self.trigger_agent(agent_id)

        logger.info("")
        logger.info("=" * 60)
        logger.info("TESTSUITE AGENT CONFIGURED")
        logger.info("=" * 60)
        logger.info("Schedule: %s", self.schedule)
        logger.info("Agent ID: %s", agent_id[:8])
        logger.info("")
        logger.info("To run tests on-demand:")
        logger.info("  OM UI → table → Data Observability → Data Quality → Run All")
        logger.info("  OR: OM UI → Settings → Services → %s → Agents", DB_SERVICE)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create OM TestSuite agent")
    parser.add_argument("--url", default=OM_URL)
    parser.add_argument("--schedule", default="0 */6 * * *",
                        help="Cron schedule for test execution")
    args = parser.parse_args()

    creator = OMTestAgentCreator(args.url, args.schedule)
    creator.run()


if __name__ == "__main__":
    main()
