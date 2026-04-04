import urllib.request
import json
import sys

base = "http://airflow-api-server:8080"
auth_data = json.dumps({"username": "airflow", "password": "airflow"}).encode()
req = urllib.request.Request(
    f"{base}/auth/token",
    data=auth_data,
    headers={"Content-Type": "application/json"},
    method="POST",
)
resp = urllib.request.urlopen(req)
token = json.loads(resp.read())["access_token"]
headers = {"Authorization": f"Bearer {token}"}

# Check import errors
req2 = urllib.request.Request(f"{base}/api/v2/importErrors", headers=headers)
resp2 = urllib.request.urlopen(req2)
errors = json.loads(resp2.read())
import_errors = errors.get("import_errors", [])
if import_errors:
    print(f"=== {len(import_errors)} IMPORT ERROR(S) ===")
    for e in import_errors:
        print(f"\nFile: {e.get('filename', '?')}")
        print(f"Error: {e.get('stack_trace', '?')[:500]}")
else:
    print("=== No import errors ===")

# List all DAGs
req3 = urllib.request.Request(f"{base}/api/v2/dags?limit=50", headers=headers)
resp3 = urllib.request.urlopen(req3)
dags = json.loads(resp3.read())
print(f"\n=== {len(dags.get('dags', []))} DAG(s) found ===")
for d in dags.get("dags", []):
    print(f"  {d['dag_id']} | paused={d.get('is_paused')} | active={d.get('is_active')}")
