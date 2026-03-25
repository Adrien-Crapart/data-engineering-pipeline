# Monitoring Stack

Observabilite complete de la stack Weather Data Pipeline : metriques, dashboards, alertes, et logs centralises.

## Architecture

```
                  ┌──────────────┐
                  │   Airflow    │──StatsD──▶ statsd-exporter ──▶ Prometheus
                  │  (scheduler, │                                    │
                  │   worker...) │                                    ▼
                  └──────────────┘                              Alertmanager ──▶ MailHog
                         │                                          │
                         │ logs                                     ▼
                         ▼                                     Email alerts
     Docker socket ──▶ Promtail ──▶ Loki ──▶ Grafana
                                               ▲
     Docker socket ──▶ docker-exporter ────────┘
     /proc, /sys   ──▶ node-exporter  ─────────┘
     Pipeline code ──▶ Pushgateway ─────────────┘
```

## Services

| Service | Port | Description |
|---------|------|-------------|
| Prometheus | 9090 | Collecte et stockage de metriques (retention 90j) |
| Pushgateway | 9091 | Metriques pipeline poussees par le code Python |
| Grafana | 3000 | Dashboards et alertes (admin/admin) |
| Alertmanager | 9093 | Routage et notification des alertes |
| StatsD Exporter | 9102 | Conversion metriques StatsD Airflow → Prometheus |
| Docker Exporter | 9417 | Metriques CPU/RAM/reseau des conteneurs Docker |
| Node Exporter | 9100 | Metriques machine hote (CPU, RAM, disque) |
| Loki | 3100 | Agregation et stockage de logs |
| Promtail | 9080 | Collecte des logs Docker → Loki |
| MailHog | 8025 | Interface web pour visualiser les emails d'alerte |

## Dashboards Grafana

| Dashboard | Description |
|-----------|-------------|
| **Airflow - Pipelines & DAGs** | DAGs actives, erreurs d'import, DAG runs, task instances, taux de succes, timeline Gantt, slots/pools, parsing time, logs d'erreurs |
| **Airflow - Infrastructure & Containers** | Statut UP/DOWN des services, healthchecks, CPU/RAM par conteneur, timeline de disponibilite, redemarrages, reseau, metriques machine hote, logs temps reel |
| **Weather Pipeline Overview** | Duree pipeline, statut derniere execution, echecs, temps de reponse API, latence d'ingestion, freshness |
| **Pipeline SLA** | Temps depuis derniere execution, alertes actives, utilisation memoire/disque, sante des services |

## Alertes Prometheus

### Airflow
- **AirflowSchedulerDown** — Le scheduler ne repond plus (CRITICAL)
- **AirflowHighTaskFailureRate** — Taux d'echec eleve (WARNING)
- **AirflowDAGImportErrors** — DAGs avec erreurs d'import (WARNING)
- **AirflowExecutorSlotsExhausted** — Slots presque epuises (WARNING)
- **AirflowZombiesDetected** — Taches zombies (WARNING)

### Conteneurs Docker
- **AirflowContainerDown** — Conteneur Airflow critique arrete (CRITICAL)
- **InfrastructureServiceDown** — Redis/Postgres/MinIO arrete (CRITICAL)
- **ContainerRestarted** — Conteneur redemarre (WARNING)
- **ContainerHighMemory** — Memoire > 85% (WARNING)
- **ContainerHighCPU** — CPU > 90% (WARNING)
- **ContainerUnhealthy** — Healthcheck echoue (WARNING)

### Machine hote
- **HostDown** — Machine inaccessible (CRITICAL)
- **HostHighCPU** — CPU > 85% (WARNING)
- **HostHighMemory** — RAM > 85% (WARNING)
- **HostDiskSpaceLow** — Disque > 85% (WARNING)
- **HostNetworkDown** — Trafic reseau nul (CRITICAL)

### Stack monitoring
- **PrometheusTargetDown** — Cible inaccessible (WARNING)
- **LokiDown** — Loki inaccessible (WARNING)

## Structure des fichiers

```
monitoring/
├── alertmanager/
│   ├── alertmanager.yml              — Config Alertmanager (SMTP MailHog)
│   └── templates/
│       └── email.tmpl                — Template HTML des emails d'alerte
├── docker-exporter/
│   ├── Dockerfile                    — Image Python pour metriques Docker
│   └── exporter.py                   — Exporteur Prometheus custom
├── grafana/
│   ├── dashboards/
│   │   ├── airflow-containers.json   — Dashboard Infrastructure & Containers
│   │   ├── airflow-overview.json     — Dashboard Pipelines & DAGs
│   │   ├── pipeline_overview.json    — Dashboard Weather Pipeline
│   │   └── pipeline_sla.json         — Dashboard SLA
│   └── provisioning/
│       ├── alerting/
│       │   └── alerts.yml            — Contact points et policies Grafana
│       ├── dashboards/
│       │   └── dashboard.yml         — Provider auto-load dashboards
│       └── datasources/
│           └── prometheus.yml        — Datasources (Prometheus, Loki, PostgreSQL)
├── loki/
│   └── loki-config.yml              — Config Loki (stockage local)
├── metrics/
│   └── exporter.py                  — Metriques pipeline Python → Pushgateway
├── prometheus/
│   ├── alert_rules.yml              — Regles d'alertes Prometheus
│   └── prometheus.yml               — Config scrape Prometheus
├── promtail/
│   └── promtail-config.yml          — Config collecte logs Docker
├── statsd-exporter/
│   └── statsd_mapping.yml           — Mapping StatsD Airflow → Prometheus
└── README.md
```

## Acces

- **Grafana**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Alertmanager**: http://localhost:9093
- **MailHog** (emails): http://localhost:8025
- **Loki** (logs API): http://localhost:3100
