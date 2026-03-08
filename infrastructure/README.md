# Infrastructure

Docker infrastructure, database initialization scripts, and deployment configuration.

## Purpose

Contains all infrastructure-as-code for the data platform: Dockerfiles,
docker-compose configurations, and database initialization scripts.

## Structure

```
infrastructure/
├── docker/
│   ├── Dockerfile                      — Airflow image with all Python dependencies
│   ├── docker-compose.yml              — Core stack (Airflow, PostgreSQL, MinIO, Prometheus, Grafana)
│   └── docker-compose.openmetadata.yml — OpenMetadata stack (MySQL, Elasticsearch, OM server)
└── scripts/
    ├── init_db.sql                     — PostgreSQL schema initialization (raw, staging, mart, elementary)
    └── seed_test_data.sql              — Test data seeding script
```

## Quick Start

```bash
# Core stack
make up

# Full stack (including OpenMetadata)
make up-full

# Tear down
make down
```

## Service Ports

| Service | Port | URL |
|---------|------|-----|
| Airflow UI | 8080 | http://localhost:8080 |
| PostgreSQL | 5432 | — |
| MinIO Console | 9001 | http://localhost:9001 |
| MinIO API | 9000 | — |
| Prometheus | 9090 | http://localhost:9090 |
| Grafana | 3000 | http://localhost:3000 |
| OpenMetadata | 8585 | http://localhost:8585 |
