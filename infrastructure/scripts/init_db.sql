-- Initialize database schemas for the data pipeline layers.
-- raw:     raw API responses loaded by dlt
-- staging: cleaned and typed models built by dbt
-- mart:    analytical models built by dbt

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;
