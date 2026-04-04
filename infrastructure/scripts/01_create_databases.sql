-- Runs as the PostgreSQL superuser on first container start.

-- OpenMetadata database
SELECT 'CREATE DATABASE openmetadata_db OWNER openmetadata_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'openmetadata_db')\gexec

-- OpenMetadata Airflow database
SELECT 'CREATE DATABASE openmetadata_airflow OWNER openmetadata_airflow_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'openmetadata_airflow')\gexec

-- Airflow database
SELECT 'CREATE DATABASE airflow OWNER airflow_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

-- Datawarehouse database
SELECT 'CREATE DATABASE datawarehouse OWNER datawarehouse_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'datawarehouse')\gexec

-- Metabase database
SELECT 'CREATE DATABASE metabase OWNER metabase_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec

-- Create schemas inside datawarehouse for medallion architecture
\c datawarehouse
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION datawarehouse_user;
CREATE SCHEMA IF NOT EXISTS staging_quarantine AUTHORIZATION datawarehouse_user;
CREATE SCHEMA IF NOT EXISTS core AUTHORIZATION datawarehouse_user;
CREATE SCHEMA IF NOT EXISTS mart AUTHORIZATION datawarehouse_user;
CREATE SCHEMA IF NOT EXISTS analytic AUTHORIZATION datawarehouse_user;
