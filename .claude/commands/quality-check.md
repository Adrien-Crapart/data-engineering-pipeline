Run all data quality gates and report a summary of results. Requires the stack to be running (`just up`).

## Steps

1. Verify the stack is running:
   ```bash
   just status
   ```
   If services are not up, run `just up` first and wait for health checks to pass.

2. Run Soda Core checks on staging:
   ```bash
   just soda-check
   ```

3. Run Great Expectations validations:
   ```bash
   just gx-check
   ```

4. Run dbt tests:
   ```bash
   just dbt-test
   ```

5. Check quarantine table for rejected rows:
   ```bash
   just psql
   ```
   Then in psql:
   ```sql
   SELECT rejection_reason, count(*) 
   FROM staging_quarantine.stg_weather_current_quarantine 
   GROUP BY 1 
   ORDER BY 2 DESC;
   ```

6. Report a summary table:

| Gate | Tool | Status | Checks Passed | Issues |
|------|------|--------|--------------|--------|
| Gate 2 — Staging | Soda | ... | ... | ... |
| Gate 2 — Staging | GX | ... | ... | ... |
| Gate 3 — Mart | Soda | ... | ... | ... |
| Gate 3 — Mart | GX | ... | ... | ... |
| dbt tests | dbt test | ... | ... | ... |

7. If any checks failed, show the specific failing checks, the affected tables, and suggest root cause + fix.
