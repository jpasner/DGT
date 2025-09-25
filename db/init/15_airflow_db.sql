DO $$
BEGIN
  IF NOT EXISTS (
      SELECT 1 FROM pg_roles WHERE rolname = 'airflow'
  ) THEN
    CREATE ROLE airflow LOGIN PASSWORD 'airflow';
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
      SELECT 1 FROM pg_database WHERE datname = 'airflow_db'
  ) THEN
    CREATE DATABASE airflow_db OWNER airflow;
  END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow;
