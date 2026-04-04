"""OpenMetadata bootstrap script.

Provisions classifications, glossary, teams, users, tiers, custom properties,
descriptions and ownership for all weather pipeline tables via the OM REST API.

Usage:
    python metadata/om_bootstrap.py [--url http://localhost:8585] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Any

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_OM_URL = "http://localhost:8585"
DEFAULT_EMAIL = "admin@open-metadata.org"
DEFAULT_PASSWORD = "YWRtaW4="  # base64("admin")

FQN_PREFIX = "datawarehouse.datawarehouse"
EXPORT_DIR = Path(__file__).parent

# ---------------------------------------------------------------------------
# Data definitions
# ---------------------------------------------------------------------------

CLASSIFICATIONS = [
    {
        "name": "DataLayer",
        "description": "Classification by medallion architecture layer.",
        "tags": [
            {"name": "Bronze", "description": "Raw, immutable data from source systems (S3 Parquet)."},
            {"name": "Silver", "description": "Cleaned, validated, and typed data (staging)."},
            {"name": "Gold", "description": "Business-domain and aggregated data (core/mart/analytic)."},
        ],
    },
    {
        "name": "DataDomain",
        "description": "Business domain classification.",
        "tags": [
            {"name": "Meteorology", "description": "Weather observations, forecasts, and conditions."},
            {"name": "Geography", "description": "City, country, and location reference data."},
            {"name": "Analytics", "description": "Derived aggregations and trend analysis."},
        ],
    },
    {
        "name": "DataSensitivity",
        "description": "Data sensitivity classification for governance.",
        "tags": [
            {"name": "Public", "description": "Open data, no access restrictions."},
            {"name": "Internal", "description": "Internal use only, not shared externally."},
        ],
    },
    {
        "name": "UpdateFrequency",
        "description": "How often the data asset is refreshed.",
        "tags": [
            {"name": "Hourly", "description": "Updated every 1-3 hours."},
            {"name": "Daily", "description": "Updated once per day."},
            {"name": "Weekly", "description": "Updated once per week."},
        ],
    },
]

GLOSSARY = {
    "name": "Weather Data Glossary",
    "description": "Standard terminology for the weather data platform.",
    "terms": [
        {"name": "Temperature", "description": "Air temperature measured in degrees Celsius.", "synonyms": ["temp", "température"]},
        {"name": "Humidity", "description": "Relative humidity expressed as percentage (0-100).", "synonyms": ["humidité"]},
        {"name": "Pressure", "description": "Atmospheric pressure at sea level in hectopascals (hPa).", "synonyms": ["pression"]},
        {"name": "Wind Speed", "description": "Speed of wind in meters per second (m/s).", "synonyms": ["vitesse du vent"]},
        {"name": "Weather Condition", "description": "Primary weather group (Clear, Rain, Clouds, etc.).", "synonyms": ["condition météo"]},
        {"name": "Observation", "description": "A single weather reading at a specific time and location.", "synonyms": ["mesure"]},
        {"name": "Forecast", "description": "Predicted weather values for a future time slot.", "synonyms": ["prévision"]},
        {"name": "Moving Average", "description": "Rolling average over a defined window (e.g. 7 days).", "synonyms": ["moyenne glissante"]},
    ],
}

TEAMS = [
    {"name": "data-engineering", "displayName": "Data Engineering", "description": "Builds and maintains data pipelines.", "teamType": "Group"},
    {"name": "data-analytics", "displayName": "Data Analytics", "description": "Analyses data and creates dashboards.", "teamType": "Group"},
    {"name": "data-governance", "displayName": "Data Governance", "description": "Ensures data quality and compliance.", "teamType": "Group"},
    {"name": "weather-operations", "displayName": "Weather Operations", "description": "Domain experts for weather data.", "teamType": "Group"},
]

USERS = [
    {"name": "marie_dupont", "email": "marie.dupont@weather-corp.com", "displayName": "Marie Dupont", "teams": ["data-engineering"]},
    {"name": "pierre_martin", "email": "pierre.martin@weather-corp.com", "displayName": "Pierre Martin", "teams": ["data-analytics"]},
    {"name": "sophie_leroy", "email": "sophie.leroy@weather-corp.com", "displayName": "Sophie Leroy", "teams": ["data-governance"]},
    {"name": "jp_meteo", "email": "jp.meteo@weather-corp.com", "displayName": "Jean-Paul Météo", "teams": ["weather-operations"]},
]

TABLE_CONFIG: dict[str, dict[str, Any]] = {
    "fct_weather_observation": {
        "schema": "core",
        "tier": "Tier1",
        "owner_team": "data-engineering",
        "domain_tags": ["DataLayer.Gold", "DataDomain.Meteorology", "DataSensitivity.Public", "UpdateFrequency.Hourly"],
        "description": "Canonical fact table for individual weather observations. One row per city per extraction.",
        "custom_properties": {"data_source": "OpenWeather API v2.5", "refresh_frequency": "Every 3 hours", "sla_hours": "4", "data_retention_days": "730"},
    },
    "dim_city": {
        "schema": "core",
        "tier": "Tier1",
        "owner_team": "data-engineering",
        "domain_tags": ["DataLayer.Gold", "DataDomain.Geography", "DataSensitivity.Public", "UpdateFrequency.Weekly"],
        "description": "Conformed city dimension. Deduplicates city metadata into a single authoritative reference.",
        "custom_properties": {"data_source": "OpenWeather API v2.5", "refresh_frequency": "Weekly", "sla_hours": "24"},
    },
    "weather_daily_summary": {
        "schema": "mart",
        "tier": "Tier2",
        "owner_team": "data-analytics",
        "domain_tags": ["DataLayer.Gold", "DataDomain.Meteorology", "DataSensitivity.Public", "UpdateFrequency.Daily"],
        "description": "Daily weather summary per city. Aggregates observations and forecasts into daily min/max/avg.",
        "custom_properties": {"data_source": "Derived (dbt)", "refresh_frequency": "Daily at 03:00", "sla_hours": "6", "data_retention_days": "365"},
    },
    "city_weather_metrics": {
        "schema": "mart",
        "tier": "Tier2",
        "owner_team": "data-analytics",
        "domain_tags": ["DataLayer.Gold", "DataDomain.Analytics", "DataSensitivity.Public", "UpdateFrequency.Daily"],
        "description": "Aggregated weather metrics per city across all available data for city comparison dashboards.",
        "custom_properties": {"data_source": "Derived (dbt)", "refresh_frequency": "Daily at 03:00", "sla_hours": "6", "data_retention_days": "365"},
    },
    "weather_trend_analysis": {
        "schema": "analytic",
        "tier": "Tier3",
        "owner_team": "data-analytics",
        "domain_tags": ["DataLayer.Gold", "DataDomain.Analytics", "DataSensitivity.Internal", "UpdateFrequency.Daily"],
        "description": "Rolling 7-day weather trend analysis per city with moving averages for temperature, humidity, and wind.",
        "custom_properties": {"data_source": "Derived (dbt)", "refresh_frequency": "Daily at 03:00", "sla_hours": "12", "data_retention_days": "180"},
    },
    "city_comparison_ranking": {
        "schema": "analytic",
        "tier": "Tier3",
        "owner_team": "data-analytics",
        "domain_tags": ["DataLayer.Gold", "DataDomain.Analytics", "DataSensitivity.Internal", "UpdateFrequency.Daily"],
        "description": "Cross-city ranking on multiple weather dimensions for comparative analysis.",
        "custom_properties": {"data_source": "Derived (dbt)", "refresh_frequency": "Daily at 03:00", "sla_hours": "12", "data_retention_days": "180"},
    },
}

DOMAINS = [
    {
        "name": "weather-observations",
        "displayName": "Weather Observations",
        "description": "Domain covering raw and processed weather observation data.",
        "domainType": "Source-aligned",
        "tables": ["fct_weather_observation", "dim_city"],
    },
    {
        "name": "weather-analytics",
        "displayName": "Weather Analytics",
        "description": "Domain covering derived analytics, trends, and rankings.",
        "domainType": "Consumer-aligned",
        "tables": ["weather_daily_summary", "city_weather_metrics", "weather_trend_analysis", "city_comparison_ranking"],
    },
]

CUSTOM_PROPERTY_DEFS = [
    {"name": "data_source", "propertyType": "string", "description": "API or system that produces this data."},
    {"name": "refresh_frequency", "propertyType": "string", "description": "How often the data is refreshed."},
    {"name": "sla_hours", "propertyType": "string", "description": "SLA for data availability in hours."},
    {"name": "data_retention_days", "propertyType": "string", "description": "Data retention period in days."},
]


# ---------------------------------------------------------------------------
# API Client
# ---------------------------------------------------------------------------

class OMClient:
    def __init__(self, base_url: str, dry_run: bool = False):
        self.base_url = base_url.rstrip("/")
        self.api = f"{self.base_url}/api/v1"
        self.token: str | None = None
        self.dry_run = dry_run
        self._table_cache: dict[str, dict] = {}
        self._team_cache: dict[str, str] = {}

    def authenticate(self) -> None:
        resp = requests.post(
            f"{self.api}/users/login",
            json={"email": DEFAULT_EMAIL, "password": DEFAULT_PASSWORD},
            timeout=15,
        )
        resp.raise_for_status()
        self.token = resp.json()["accessToken"]
        logger.info("Authenticated to %s", self.base_url)

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    @property
    def patch_headers(self) -> dict[str, str]:
        return {**self.headers, "Content-Type": "application/json-patch+json"}

    @property
    def json_headers(self) -> dict[str, str]:
        return {**self.headers, "Content-Type": "application/json"}

    def _get(self, path: str, **kwargs: Any) -> requests.Response:
        return requests.get(f"{self.api}{path}", headers=self.headers, timeout=15, **kwargs)

    def _post(self, path: str, payload: dict) -> requests.Response | None:
        if self.dry_run:
            logger.info("[DRY-RUN] POST %s → %s", path, json.dumps(payload, indent=2)[:200])
            return None
        resp = requests.post(f"{self.api}{path}", headers=self.json_headers, json=payload, timeout=15)
        return resp

    def _put(self, path: str, payload: dict) -> requests.Response | None:
        if self.dry_run:
            logger.info("[DRY-RUN] PUT %s", path)
            return None
        resp = requests.put(f"{self.api}{path}", headers=self.json_headers, json=payload, timeout=15)
        return resp

    def _patch(self, path: str, payload: list[dict]) -> requests.Response | None:
        if self.dry_run:
            logger.info("[DRY-RUN] PATCH %s → %s", path, json.dumps(payload)[:200])
            return None
        resp = requests.patch(f"{self.api}{path}", headers=self.patch_headers, json=payload, timeout=15)
        return resp

    # ------ Classifications / Tags ------
    def create_classifications(self) -> None:
        logger.info("=== Creating classifications ===")
        for clf in CLASSIFICATIONS:
            resp = self._put("/classifications", {"name": clf["name"], "description": clf["description"]})
            if resp is not None and resp.status_code in (200, 201):
                logger.info("  Classification '%s' created/updated", clf["name"])
                clf_data = resp.json()
                for tag in clf["tags"]:
                    tag_payload = {
                        "classification": clf["name"],
                        "name": tag["name"],
                        "description": tag["description"],
                    }
                    tag_resp = self._post("/tags", tag_payload)
                    if tag_resp is not None and tag_resp.status_code in (200, 201):
                        logger.info("    Tag '%s.%s' → created", clf["name"], tag["name"])
                    elif tag_resp is not None and tag_resp.status_code == 409:
                        logger.info("    Tag '%s.%s' → already exists", clf["name"], tag["name"])
                    elif tag_resp is not None:
                        logger.warning("    Tag '%s.%s' → FAILED (%s): %s", clf["name"], tag["name"], tag_resp.status_code, tag_resp.text[:200])
                    else:
                        logger.info("    Tag '%s.%s' → DRY-RUN", clf["name"], tag["name"])
            elif resp is not None:
                logger.warning("  Classification '%s' → %s: %s", clf["name"], resp.status_code, resp.text[:200])

    # ------ Glossary ------
    def create_glossary(self) -> None:
        logger.info("=== Creating glossary ===")
        resp = self._put(
            "/glossaries",
            {"name": GLOSSARY["name"], "description": GLOSSARY["description"]},
        )
        if not resp or resp.status_code not in (200, 201):
            logger.warning("Glossary creation failed: %s", resp.status_code if resp else "dry-run")
            return
        glossary_data = resp.json()
        glossary_id = glossary_data["id"]
        for term in GLOSSARY["terms"]:
            term_payload = {
                "glossary": {"id": glossary_id, "type": "glossary"},
                "name": term["name"],
                "description": term["description"],
                "synonyms": term.get("synonyms", []),
            }
            t_resp = self._put("/glossaryTerms", term_payload)
            status = "OK" if t_resp is not None and t_resp.status_code in (200, 201) else "SKIP"
            logger.info("  Term '%s' → %s", term["name"], status)

    # ------ Teams ------
    def create_teams(self) -> None:
        logger.info("=== Creating teams ===")
        for team in TEAMS:
            resp = self._put("/teams", team)
            if resp is not None and resp.status_code in (200, 201):
                self._team_cache[team["name"]] = resp.json()["id"]
                logger.info("  Team '%s' → OK", team["name"])
            elif resp is not None:
                existing = self._get(f"/teams/name/{team['name']}")
                if existing.status_code == 200:
                    self._team_cache[team["name"]] = existing.json()["id"]
                logger.info("  Team '%s' → already exists", team["name"])

    # ------ Users ------
    def create_users(self) -> None:
        logger.info("=== Creating users ===")
        for user in USERS:
            teams_refs = []
            for t_name in user.get("teams", []):
                if t_name in self._team_cache:
                    teams_refs.append({"id": self._team_cache[t_name], "type": "team"})
            payload = {
                "name": user["name"],
                "email": user["email"],
                "displayName": user["displayName"],
                "teams": teams_refs,
                "isBot": False,
            }
            resp = self._put("/users", payload)
            status = "OK" if resp is not None and resp.status_code in (200, 201) else "exists"
            logger.info("  User '%s' → %s", user["name"], status)

    # ------ Table operations ------
    def _get_table(self, table_name: str, schema: str) -> dict | None:
        fqn = f"{FQN_PREFIX}.{schema}.{table_name}"
        if fqn in self._table_cache:
            return self._table_cache[fqn]
        resp = self._get(f"/tables/name/{fqn}", params={"fields": "tags,owners"})
        if resp.status_code == 200:
            self._table_cache[fqn] = resp.json()
            return resp.json()
        logger.warning("  Table '%s' not found in OM (fqn=%s, status=%s)", table_name, fqn, resp.status_code)
        return None

    def apply_table_configs(self) -> None:
        logger.info("=== Applying table configurations ===")
        for table_name, cfg in TABLE_CONFIG.items():
            table = self._get_table(table_name, cfg["schema"])
            if not table:
                continue
            table_id = table["id"]
            logger.info("  Processing '%s' (id=%s)", table_name, table_id[:8])

            patches: list[dict] = []

            if cfg.get("description"):
                patches.append({"op": "add", "path": "/description", "value": cfg["description"]})

            if cfg.get("tier"):
                patches.append({
                    "op": "add",
                    "path": "/tags/-",
                    "value": {"tagFQN": f"Tier.{cfg['tier']}", "source": "Classification"},
                })

            for tag_fqn in cfg.get("domain_tags", []):
                patches.append({
                    "op": "add",
                    "path": "/tags/-",
                    "value": {"tagFQN": tag_fqn, "source": "Classification"},
                })

            if cfg.get("owner_team") and cfg["owner_team"] in self._team_cache:
                patches.append({
                    "op": "add",
                    "path": "/owners",
                    "value": [{"id": self._team_cache[cfg["owner_team"]], "type": "team"}],
                })

            if patches:
                logger.info("    Sending %d patches...", len(patches))
                resp = self._patch(f"/tables/{table_id}", patches)
                if resp is None:
                    logger.info("    [DRY-RUN] skipped")
                elif resp is not None and resp.status_code in (200, 201):
                    logger.info("    Patched OK: description, tier, tags, owner")
                elif resp is not None:
                    logger.warning("    Patch failed (%s): %s", resp.status_code, resp.text[:400])

    # ------ Domains ------
    def create_domains(self) -> None:
        logger.info("=== Creating domains ===")
        for domain in DOMAINS:
            payload = {
                "name": domain["name"],
                "displayName": domain["displayName"],
                "description": domain["description"],
                "domainType": domain["domainType"],
            }
            resp = self._put("/domains", payload)
            domain_id = None
            if resp is not None and resp.status_code in (200, 201):
                domain_id = resp.json()["id"]
                logger.info("  Domain '%s' → OK (id=%s)", domain["name"], domain_id[:8])
            elif resp is not None:
                existing = self._get(f"/domains/name/{domain['name']}")
                if existing.status_code == 200:
                    domain_id = existing.json()["id"]
                logger.info("  Domain '%s' → already exists", domain["name"])

            if not domain_id:
                continue

            table_names = [t for t in domain.get("tables", [])]
            logger.info("    Tables to assign via UI: %s", ", ".join(table_names))
            logger.info("    (OM 1.12.x API does not support bulk domain asset assignment — use UI: table → Domains → Edit)")

    # ------ Custom Properties ------
    def create_custom_properties(self) -> None:
        logger.info("=== Creating custom properties for tables ===")
        type_resp = self._get("/metadata/types/name/table")
        if type_resp.status_code != 200:
            logger.warning("  Cannot fetch table type metadata, skipping custom properties")
            return
        table_type_id = type_resp.json()["id"]

        string_type = self._get("/metadata/types/name/string")
        if string_type.status_code != 200:
            logger.warning("  Cannot fetch string property type")
            return
        string_type_id = string_type.json()["id"]

        for prop in CUSTOM_PROPERTY_DEFS:
            payload = {
                "name": prop["name"],
                "description": prop["description"],
                "propertyType": {"id": string_type_id, "type": "type"},
            }
            resp = self._put(f"/metadata/types/{table_type_id}", payload)
            status = "OK" if resp is not None and resp.status_code in (200, 201) else "exists"
            logger.info("  Property '%s' → %s", prop["name"], status)

    # ------ Export ------
    def export_config(self, output_path: str | None = None) -> None:
        if output_path is None:
            output_path = str(EXPORT_DIR / "om_config_export.json")
        logger.info("=== Exporting current OM configuration ===")
        export: dict[str, Any] = {"exported_at": time.strftime("%Y-%m-%dT%H:%M:%SZ"), "tables": {}}

        for table_name, cfg in TABLE_CONFIG.items():
            table = self._get_table(table_name, cfg["schema"])
            if not table:
                continue
            export["tables"][table_name] = {
                "id": table["id"],
                "fqn": table.get("fullyQualifiedName"),
                "description": table.get("description"),
                "tags": [t.get("tagFQN") for t in table.get("tags", [])],
                "owners": [o.get("name") for o in table.get("owners", [])],
                "domain": table.get("domain", {}).get("name") if table.get("domain") else None,
            }

        with open(output_path, "w") as f:
            json.dump(export, f, indent=2)
        logger.info("  Exported to %s", output_path)

    # ------ Full bootstrap ------
    def bootstrap(self) -> None:
        self.authenticate()
        self.create_classifications()
        self.create_glossary()
        self.create_teams()
        self.create_users()
        self.create_custom_properties()
        self.apply_table_configs()
        self.create_domains()
        self.export_config()
        logger.info("=== Bootstrap complete ===")


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap OpenMetadata configuration")
    parser.add_argument("--url", default=DEFAULT_OM_URL, help="OM server URL")
    parser.add_argument("--dry-run", action="store_true", help="Print actions without executing")
    parser.add_argument("--export-only", action="store_true", help="Only export current config")
    args = parser.parse_args()

    client = OMClient(args.url, dry_run=args.dry_run)

    if args.export_only:
        client.authenticate()
        client.export_config()
    else:
        client.bootstrap()


if __name__ == "__main__":
    main()
