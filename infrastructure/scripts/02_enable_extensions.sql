-- Enable pg_stat_statements on all databases that OpenMetadata may query.

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

\c datawarehouse
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

\c airflow
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
