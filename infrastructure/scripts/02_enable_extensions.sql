-- Enable pg_stat_statements on every application database.
-- The shared library is loaded via postgres command args in compose.

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

\c datawarehouse
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

\c airflow
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

\c openmetadata
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

\c openmetadata_airflow
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

\c metabase
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
