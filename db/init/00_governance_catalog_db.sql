-- Create governance_catalog database and user
DO $$
BEGIN
  IF NOT EXISTS (
      SELECT 1 FROM pg_roles WHERE rolname = 'metadata_admin'
  ) THEN
    CREATE ROLE metadata_admin LOGIN PASSWORD 'metadata_admin';
  END IF;
END
$$;

SELECT 'CREATE DATABASE governance_catalog OWNER metadata_admin'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_database WHERE datname = 'governance_catalog'
)
\gexec

GRANT ALL PRIVILEGES ON DATABASE governance_catalog TO metadata_admin;

-- Connect to governance_catalog and enable extensions
\c governance_catalog

-- Extensions needed for governance_catalog
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
