"""Trigger all OM ingestion agents in order: metadata → dbt → profiler."""
import requests
import time
import json

OM = "http://localhost:8585/api/v1"
r = requests.post(f"{OM}/users/login", json={"email": "admin@open-metadata.org", "password": "YWRtaW4="})
TOKEN = r.json()["accessToken"]
H = {"Authorization": f"Bearer {TOKEN}"}
JH = {**H, "Content-Type": "application/json"}

pipelines = requests.get(f"{OM}/services/ingestionPipelines?limit=50", headers=H, timeout=15).json()
agents = {}
for p in pipelines.get("data", []):
    agents[p.get("pipelineType", "unknown")] = {"id": p["id"], "name": p["name"]}

print("Available agents:")
for t, info in agents.items():
    print(f"  {t}: {info['name']} (id={info['id'][:8]})")

trigger_order = ["metadata", "dbt", "profiler"]
for agent_type in trigger_order:
    if agent_type not in agents:
        print(f"\n  SKIP: no {agent_type} agent")
        continue

    pid = agents[agent_type]["id"]
    print(f"\nTriggering {agent_type} agent (id={pid[:8]})...")
    resp = requests.post(f"{OM}/services/ingestionPipelines/trigger/{pid}", headers=JH, json={}, timeout=30)
    if resp.status_code in (200, 201):
        print(f"  {agent_type} triggered OK")
    else:
        print(f"  FAILED ({resp.status_code}): {resp.text[:300]}")

    print(f"  Waiting 10s before next agent...")
    time.sleep(10)

    status_resp = requests.get(f"{OM}/services/ingestionPipelines/{pid}/pipelineStatus?startTs=0&endTs=9999999999999", headers=H, timeout=15)
    if status_resp.status_code == 200:
        statuses = status_resp.json().get("data", [])
        if statuses:
            latest = statuses[0]
            print(f"  Latest status: {latest.get('pipelineState', '?')} (started: {latest.get('startDate', '?')})")

print("\nAll agents triggered. Check OM UI in 2-5 minutes for:")
print("  - Lineage: table → Lineage tab")
print("  - Sample Data: table → Data Observability → Table Profile")
print("  - Test Quality: table → Data Observability → Data Quality")
