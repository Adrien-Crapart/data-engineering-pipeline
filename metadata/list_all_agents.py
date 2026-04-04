"""List all agents for datawarehouse service."""
import base64

import requests

token = requests.post(
    "http://localhost:8585/api/v1/users/login",
    json={
        "email": "admin@open-metadata.org",
        "password": base64.b64encode("admin".encode()).decode(),
    },
    timeout=30,
).json()["accessToken"]

# Get ALL agents (no pipelineType filter)
resp = requests.get(
    "http://localhost:8585/api/v1/services/ingestionPipelines?service=datawarehouse&limit=50",
    headers={"Authorization": f"Bearer {token}"},
    timeout=30,
)

agents = resp.json().get("data", [])
print(f"\n{'='*80}")
print(f"Total agents for datawarehouse: {len(agents)}")
print(f"{'='*80}\n")

# Group by type
by_type = {}
for p in agents:
    ptype = p["pipelineType"]
    if ptype not in by_type:
        by_type[ptype] = []
    by_type[ptype].append(p)

for ptype, agents_list in sorted(by_type.items()):
    print(f"\n{ptype} ({len(agents_list)}):")
    print("-" * 80)
    for p in agents_list:
        deployed = "OK" if p.get("deployed", False) else "NO"
        schedule = p.get("airflowConfig", {}).get("scheduleInterval", "N/A")
        print(f"  [{deployed}] {p['name']:45} | Schedule: {schedule}")
print()
