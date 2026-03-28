-- Runs as the PostgreSQL superuser on first container start.

-- OpenMetadata database
SELECT 'CREATE DATABASE openmetadata OWNER openmetadata_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'openmetadata')\gexec

-- OpenMetadata Airflow database
SELECT 'CREATE DATABASE openmetadata_airflow OWNER openmetadata_airflow_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'openmetadata_airflow')\gexec

-- Airflow database
SELECT 'CREATE DATABASE airflow OWNER airflow_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

-- Datawarehouse database
SELECT 'CREATE DATABASE datawarehouse OWNER datawarehouse_user'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'datawarehouse')\gexec
