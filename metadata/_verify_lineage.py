"""Verify lineage was created correctly."""
import requests

API = "http://localhost:8585/api/v1"
tok = requests.post(
    f"{API}/users/login",
    json={"email": "admin@open-metadata.org", "password": "YWRtaW4="},
    timeout=10,
).json()["accessToken"]
h = {"Authorization": f"Bearer {tok}"}

tables_to_check = [
    "datawarehouse.datawarehouse.core.fct_weather_observation",
    "datawarehouse.datawarehouse.staging.stg_weather_current",
    "datawarehouse.datawarehouse.mart.weather_daily_summary",
    "datawarehouse.datawarehouse.analytic.city_comparison_ranking",
]

for fqn in tables_to_check:
    short = fqn.split(".")[-1]
    r = requests.get(
        f"{API}/lineage/table/name/{fqn}?upstreamDepth=3&downstreamDepth=3",
        headers=h, timeout=10,
    )
    if r.status_code == 200:
        data = r.json()
        nodes = data.get("nodes", [])
        up = data.get("upstreamEdges", [])
        down = data.get("downstreamEdges", [])
        print(f"\n{short}: {len(nodes)} nodes, {len(up)} upstream, {len(down)} downstream")
        for n in nodes:
            nfqn = n.get("fullyQualifiedName", "?")
            ntype = n.get("entityType", "?")
            print(f"  node: {nfqn} ({ntype})")
    else:
        print(f"\n{short}: ERROR {r.status_code}")

print("\n=== PIPELINE ENTITIES ===")
r = requests.get(f"{API}/pipelines?limit=50", headers=h, timeout=10)
if r.status_code == 200:
    for p in r.json().get("data", []):
        print(f"  {p.get('fullyQualifiedName', '?')} (id={p['id'][:8]})")
