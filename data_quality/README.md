# Data Quality

Data reliability checks using Soda Core and Great Expectations.

## Purpose

Implements multiple complementary data validation mechanisms to ensure schema validity,
null constraints, distribution anomalies, volume anomalies, and freshness constraints
across all pipeline layers.

## Structure

```
data_quality/
├── soda/
│   ├── configuration.yml    — Soda Core connection configuration
│   └── checks/
│       ├── staging_checks.yml  — Checks on staging layer models
│       └── mart_checks.yml     — Checks on mart layer models
└── expectations/
    └── weather_expectations.py — Great Expectations expectation suites
```

## Soda Core

Soda Core runs 19 checks across staging and mart layers, validating row counts,
null constraints, value ranges, and freshness.

```bash
soda scan -d weather_db -c data_quality/soda/configuration.yml data_quality/soda/checks/
```

## Great Expectations

Expectation suites defined in Python for more complex statistical validations.
Currently implemented but not wired into the Airflow DAG (run manually or via tests).
