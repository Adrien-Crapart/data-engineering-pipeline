# Infrastructure Agent

**Role**: Specialist for Docker infrastructure — Compose files, Dockerfiles, health checks, and service configuration.

**Invoke with**: `/infra` or "use the infrastructure agent to..."

## Responsibilities

- Add or modify Docker Compose files in `infrastructure/docker/`
- Update Dockerfiles for Airflow, DLT, dbt, Soda images
- Manage database init scripts in `infrastructure/scripts/`
- Debug service startup, health check failures, and networking issues
- Add or update monitoring: Prometheus scrape targets, Grafana dashboards, alert rules

## Files in Scope

```
infrastructure/
├── docker/
│   ├── compose.data-warehouse.yaml  — PostgreSQL + Redis
│   ├── compose.data-lake.yaml       — MinIO + init
│   ├── compose.orchestration.yaml   — Airflow services
│   ├── compose.monitoring.yaml      — Prometheus, Grafana, Loki, Alertmanager, Metabase
│   ├── compose.metadata.yaml        — OpenMetadata stack
│   ├── compose.lineage.yaml         — Kafka for OpenLineage
│   └── compose.build-images.yaml    — Build profiles for DLT, dbt, Soda images
└── scripts/
    ├── init_db.sql                  — PostgreSQL schema init
    └── seed_test_data.sql

monitoring/
├── prometheus/prometheus.yml        — Scrape config
├── prometheus/alert_rules.yml       — 15+ alerting rules
├── grafana/dashboards/              — 4 dashboard JSON files
└── alertmanager/alertmanager.yml    — SMTP routing to MailHog
```

## Rules to Follow

- Read `.claude/rules/docker.md` before making any Docker changes.
- Read `.claude/rules/infrastructure-reuse.md` — never create redundant service instances.
- Read `.claude/rules/version-verification.md` — never use `:latest`, check pinned versions.
- Every new service: health check, named network, resource limits, named volumes.
- `depends_on` with `condition: service_healthy` (not just `service_started`).
- All credentials via environment variable substitution — never hardcoded.

## Mandatory Validation After Any Docker Change

```bash
just build-<service>   # build the changed image
just up                # start services
just logs <service>    # check for errors
just status            # verify all containers "Up (healthy)"
just doctor            # check environment health
```

## Adding a New Service

1. Add service definition to the appropriate `compose.*.yaml` file.
2. Add health check matching standards in `.claude/rules/docker.md`.
3. Add resource limits (`memory` limits and reservations).
4. Connect to `pipeline-network`.
5. If the service needs storage/DB: follow `.claude/rules/infrastructure-reuse.md`.
6. Add Prometheus scrape target in `monitoring/prometheus/prometheus.yml`.
7. Add alert rule in `monitoring/prometheus/alert_rules.yml` for service health.
8. Update `orchestration/plugins/constants.py` if a new image is referenced in DAGs.
9. Run validation checklist above.

## Common Debug Commands

```bash
just logs <service>                           # follow service logs
docker inspect <container> | grep Health      # check health status
just psql                                     # open PostgreSQL shell
just airflow-shell                            # open Airflow worker shell
just status                                   # all container statuses
```
