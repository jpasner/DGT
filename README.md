# Data Governance Transformation Demo

This repository wires up a local demonstration environment that supports the Data Governance Transformation (DGT) Policy Framework. It provisions:

- A PostgreSQL warehouse that stores harvested Data.gov metadata enriched with governance policies, roles, and security controls.
- An OpenMetadata catalog configured to use PostgreSQL as its metadata backend and to surface curated governance views from the warehouse.
- Seed data that captures core governance roles, baseline federal policies, and security markings to jump-start scenario walkthroughs.

Use this stack to iterate on governance playbooks, validate policy controls, and showcase a modernized metadata experience for agency stakeholders.

## Repository Layout

- `docker-compose.yml` — orchestrates PostgreSQL, OpenMetadata, Kafka, and Elasticsearch containers.
- `db/init/` — SQL bootstrap scripts that create the governance schema, views, and seed data.
- `openmetadata/openmetadata.yaml` — OpenMetadata server configuration pointing to PostgreSQL.
- `openmetadata/ingestion.yaml` — Ingestion workflow that registers governance views in the catalog.
- `openmetadata/base_ingestion.yaml` — Ingestion workflow that captures the base governance tables.
- `openmetadata/profiler.yaml` — Profiler workflow that builds sample data and column statistics for the UI.
- `docs/architecture.md` — High-level architecture and data model overview.

## Prerequisites

- Docker Engine 20.10+ and Docker Compose v2 installed locally.
- At least 4 CPU cores and 6 GB RAM allocated to Docker (OpenMetadata + Elasticsearch need resources).
- Ports `5432`, `8585`, `9092`, and `9200` available on the host.

## Quick Start

1. **Prime the Airflow metadata database (first run only)**
   ```bash
   docker compose up airflow-init
   ```
   This container runs once to migrate Airflow's schema and create an admin user (`admin` / `admin`). Subsequent starts can skip this step unless you wipe the `airflow_db` database.

2. **Start the stack**
   ```bash
   docker compose up -d
   ```
   The first run initializes the PostgreSQL cluster, creates the governance (`governance_catalog`), OpenMetadata (`openmetadata_db`), and Airflow (`airflow_db`) databases, seeds governance reference data, and brings up Airflow + OpenMetadata. Allow a few minutes for all health checks to turn green.

3. **Run OpenMetadata's bootstrap migrations (first run only)**
   ```bash
   docker compose run --rm --entrypoint /bin/bash openmetadata_server -lc './bootstrap/bootstrap_storage.sh migrate-all'
   ```
   This seeds the OpenMetadata application schema inside the `openmetadata_db` database and initializes Elasticsearch indexes. Re-run only if you reset the metadata database.

4. **Trigger Airflow ingestion & profiling (on demand)**
   - Browse to http://localhost:8080 and log in (`admin` / `admin`).
   - Open the **governance_metadata_ingestion** DAG and click **Trigger DAG** to run both metadata ingestion and the profiler in sequence.
   - Task `ingest_openmetadata_views` executes the `metadata ingest` CLI (config in `openmetadata/ingestion.yaml`) to synchronize curated views and generate sample rows.
   - Task `profile_openmetadata_views` runs `metadata profile` (config in `openmetadata/profiler.yaml`) to compute column statistics and persist sample data for the Profiler & Data Quality tabs.
   - If you ever regenerate the `ingestion-bot` token, update both YAML files so the DAG continues to authenticate.
   - To publish base governance tables, trigger the **governance_base_table_ingestion** DAG; it uses `openmetadata/base_ingestion.yaml` to ingest core relational tables.
   - Scheduling is set to `0 6 * * *` (daily at 06:00). Adjust the DAG to fit your cadence.

5. **Explore the catalog**
   - Open http://localhost:8585 in a browser.
   - Log in with the default `no_auth` (no credentials required) setup.
   - Search for the `governance-metadata` service to explore the registered views.

6. **Inspect the warehouse (optional)**
   ```bash
   docker compose exec postgres psql -U metadata_admin -d governance_catalog
   ```
   Useful commands inside `psql`:
   ```sql
   \dt
   SELECT * FROM governance_role;
   SELECT * FROM vw_catalog_dataset;
   ```

## Data Model Highlights

The warehouse schema captures both harvested metadata and governance overlays:

- `dataset`, `distribution`, `tag`, `agency` — core Data.gov entities.
- `policy`, `permissible_use`, `permissible_use_condition` — link datasets to authoritative policy clauses and approved use cases.
- `governance_role`, `role_assignment`, `person` — map responsibilities to people and datasets.
- `security_marking`, `dataset_security_marking` — classify datasets for cybersecurity oversight.
- Views (`vw_*`) aggregate policy, security, and stewardship context for catalog consumption.

Details and relationship diagrams live in [`docs/architecture.md`](docs/architecture.md).

## Configuration Details

- **PostgreSQL**
- Warehouse admin credentials: `metadata_admin` / `metadata_admin`.
- OpenMetadata application schema user: `openmetadata_user` / `openmetadata_password` (limited to the `openmetadata_db` database).
  - Databases:
    - `governance_catalog`: holds the governance schema and views.
    - `openmetadata_db`: reserved for OpenMetadata application tables.
    - `airflow_db`: Airflow metadata database.
  - Extensions: `uuid-ossp`, `pgcrypto`, and `pg_stat_statements` are installed automatically; the latter underpins query-usage reporting in OpenMetadata.
  - Initialization scripts run only on first bootstrap (they are mounted read-only under `db/init/`).

- **OpenMetadata**
  - Uses `openmetadata/openmetadata.yaml` to target PostgreSQL, Kafka, and Elasticsearch.
  - Authentication provider is `no_auth` for local demos; swap for SSO or JWT in production pilots.
  - The ingestion container is configured for ad-hoc runs; integrate with Airflow or the OpenMetadata workflow UI for scheduled jobs.

- **Airflow**
  - Web UI: http://localhost:8080 (`admin` / `admin`).
  - Runs the `governance_metadata_ingestion` DAG (views + profiler) and `governance_base_table_ingestion` DAG (core tables).
  - DAGs live under `airflow/dags`; edit schedules (`0 6 * * *` for views, `0 5 * * *` for base tables) or add upstream harvest jobs as needed.

## Next Steps

1. **Harvest Data.gov metadata**
   - Use the CKAN API (`https://catalog.data.gov/api/3/action/package_search`) to pull datasets of interest.
   - Normalize fields into the `dataset`, `distribution`, and `tag` tables.
   - Record the source `identifier` in `dataset.source_identifier` for traceability.

2. **Overlay governance policies**
   - Map the roles/responsibilities from the Transformation Policy Framework into the `governance_role` table (extend or adjust the seeded entries).
   - Associate datasets with policy clauses and permissible uses via `dataset_policy` and `permissible_use_condition`.
   - Populate `dataset_security_marking` to drive cybersecurity dashboards.

3. **Automate ingestion pipelines**
   - Wrap the Data.gov harvest + enrichment logic in a repeatable ETL (e.g., Python + dbt + Airflow).
   - Expand `openmetadata/ingestion.yaml` with multiple workflows (business glossary sync, lineage, data quality).
   - Attach CI/CD hooks so changes to governance policies trigger catalog refreshes.

4. **Extend the policy controls**
   - Implement audit logging (`decision_log`, `data_access_request`) to capture governance decisions.
   - Surface privacy impact assessment outcomes and risk scores in additional views for leadership reporting.

## Troubleshooting

- **OpenMetadata server fails to start** — ensure Docker has sufficient RAM; check logs via `docker compose logs openmetadata_server`.
- **Ingestion errors** — run the ingestion service manually (see Quick Start step 2) and inspect logs for PostgreSQL connectivity or schema typos.
- **Profiler task fails** — ensure `/opt/airflow/openmetadata/profiler.yaml` mounts correctly (see docker volumes) and the JWT token inside matches the current `ingestion-bot` token; re-trigger the DAG after updating.
- **Database migrations re-run** — remove the `pgdata` volume (`docker compose down -v`) if you need a clean rebuild.

## License

MIT (or align with institutional requirements).
