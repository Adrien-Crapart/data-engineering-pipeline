# Monitoring

Prometheus metrics collection, Grafana dashboards, and alerting configuration.

## Purpose

Provides full pipeline observability through metrics collection, visualization,
and alerting. Tracks pipeline duration, ingestion latency, API response times,
record counts, failures, and data freshness.

## Structure

```
monitoring/
├── metrics/
│   └── exporter.py              — Prometheus metrics definitions and push_to_gateway helper
├── prometheus/
│   └── prometheus.yml           — Prometheus scrape configuration
└── grafana/
    ├── dashboards/
    │   └── pipeline_overview.json — Pre-configured dashboard (7 panels)
    └── provisioning/
        ├── datasources/prometheus.yml — Auto-provisioned Prometheus datasource
        ├── dashboards/dashboard.yml   — Dashboard file provider
        └── alerting/alerts.yml        — Alert rules (pipeline failure, freshness SLA)
```

## Metrics Exposed

| Metric | Type | Description |
|--------|------|-------------|
| `pipeline_duration_seconds` | Histogram | Total pipeline execution duration |
| `ingestion_latency_seconds` | Histogram | Per-city ingestion latency |
| `api_response_time_seconds` | Histogram | OpenWeather API response time |
| `records_ingested_total` | Counter | Total records ingested |
| `pipeline_failures_total` | Counter | Pipeline failure count |
| `data_freshness_seconds` | Gauge | Age of most recent data |

## Access

- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)
