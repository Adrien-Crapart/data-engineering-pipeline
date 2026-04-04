-- Application database owner

-- OpenMetadata database owner
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'openmetadata_user') THEN
    CREATE ROLE openmetadata_user WITH LOGIN PASSWORD 'openmetadata_password';
  END IF;
END
$$;

-- Airflow (OM ingestion) database owner
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'openmetadata_airflow_user') THEN
    CREATE ROLE openmetadata_airflow_user WITH LOGIN PASSWORD 'openmetadata_airflow_password';
  END IF;
END
$$;

-- Airflow database owner
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow_user') THEN
    CREATE ROLE airflow_user WITH LOGIN PASSWORD 'airflow_password';
  END IF;
END
$$;

-- Datawarehouse database owner
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'datawarehouse_user') THEN
    CREATE ROLE datawarehouse_user WITH LOGIN PASSWORD 'datawarehouse_password';
  END IF;
END
$$;

-- Metabase database owner
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'metabase_user') THEN
    CREATE ROLE metabase_user WITH LOGIN PASSWORD 'metabase_password';
  END IF;
END
$$;