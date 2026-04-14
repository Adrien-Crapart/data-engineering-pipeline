Provision or re-sync all OpenMetadata configuration from code. Requires OpenMetadata to be running (`just up-full`).

## Pre-flight checks

1. Verify OpenMetadata is running:
   ```bash
   just status
   ```
   Look for `openmetadata-server` showing "Up (healthy)".

2. Check OpenMetadata is reachable:
   ```bash
   curl -s http://localhost:8585/api/v1/system/status | python3 -m json.tool
   ```

## Provisioning steps

Run scripts in order (or use the unified command):

```bash
# Option A — unified (recommended)
just om-provision

# Option B — step by step
just om-bootstrap        # 1. Governance: classifications, glossary, teams, domains
just om-setup            # 2. Tests, lineage, Airflow sync, contracts, column descriptions
just om-create-alerts    # 3. Observability alerts
just om-trigger-agents   # 4. Trigger OM metadata/profiler agents
```

## Validation

After provisioning, verify in the OM UI (http://localhost:8585):

- [ ] Tables visible under the `weather_db` service
- [ ] Lineage edges connect Bronze → Silver → Gold layers
- [ ] Data quality tests appear under **Data Observability → Data Quality** tab
- [ ] Column descriptions populated
- [ ] Teams and owners assigned to tables
- [ ] Classifications (Tier, PII) applied

## If a script fails

1. Check the error message — most failures are API 404/409 (resource already exists or wrong FQN).
2. All scripts are idempotent — safe to re-run after fixing the issue.
3. Use `--dry-run` flag if available to preview without writing:
   ```bash
   python metadata/om_setup_full.py --dry-run
   ```
4. Consult `.claude/rules/openmetadata.md` for OM 1.12.x API patterns.
5. Check OM 1.12.x docs: https://docs.open-metadata.org/v1.12.x/
