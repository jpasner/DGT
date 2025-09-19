DO $$
BEGIN
  IF NOT EXISTS (
      SELECT 1 FROM pg_roles WHERE rolname = 'openmetadata_user'
  ) THEN
    CREATE ROLE openmetadata_user LOGIN PASSWORD 'openmetadata_password';
  END IF;
END
$$;

DO $$
BEGIN
  IF NOT EXISTS (
      SELECT 1 FROM pg_database WHERE datname = 'openmetadata_db'
  ) THEN
    CREATE DATABASE openmetadata_db OWNER openmetadata_user;
  ELSE
    ALTER DATABASE openmetadata_db OWNER TO openmetadata_user;
  END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE openmetadata_db TO openmetadata_user;

ALTER DEFAULT PRIVILEGES FOR ROLE openmetadata_user IN SCHEMA public
GRANT ALL ON TABLES TO openmetadata_user;

ALTER DEFAULT PRIVILEGES FOR ROLE openmetadata_user IN SCHEMA public
GRANT ALL ON SEQUENCES TO openmetadata_user;
