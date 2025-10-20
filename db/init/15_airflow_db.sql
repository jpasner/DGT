DO $$
BEGIN
  IF NOT EXISTS (
      SELECT 1 FROM pg_roles WHERE rolname = 'airflow'
  ) THEN
    CREATE ROLE airflow LOGIN PASSWORD 'airflow';
  END IF;
END
$$;

SELECT 'CREATE DATABASE airflow_db OWNER airflow'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_database WHERE datname = 'airflow_db'
)
\gexec

ALTER DATABASE airflow_db OWNER TO airflow;

GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow;
