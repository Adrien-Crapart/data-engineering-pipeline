"""List test suites."""
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

resp = requests.get(
    "http://localhost:8585/api/v1/dataQuality/testSuites?limit=50",
    headers={"Authorization": f"Bearer {token}"},
    timeout=30,
)

suites = resp.json().get("data", [])
print(f"\nTest Suites ({len(suites)}):")
print("=" * 80)
for s in suites:
    executable = "YES" if s.get("executable", False) else "NO"
    tests_count = len(s.get("tests", []))
    print(f"  [{executable}] {s['name']:50} | Tests: {tests_count}")
print()
