# Quick Start Guide

This guide walks you through setting up the Data Governance Toolkit and ingesting CLUE data into OpenMetadata.

## Prerequisites

- Docker and Docker Compose installed
- The following CSV files in the project root:
  - `MD Clue Case Types in Redivis - Sheet1.csv`
  - `Maryland Data from CLUE_Intake Files_20220928_Allegany County Circuit Court.csv` (can be any slice of approiate schema.)

## Step 1: Start the Stack

Start all services with Docker Compose:

```bash
docker compose up -d
```

Wait for all containers to be healthy (this may take a few minutes on first run):

```bash
docker ps
```

You should see these containers running:
- `openmetadata_postgresql`
- `openmetadata_elasticsearch`
- `openmetadata_server`
- `openmetadata_ingestion`

## Step 2: Get the JWT Token from OpenMetadata

1. Open the OpenMetadata web UI at http://localhost:8585

2. Log in with default credentials:
   - Username: `admin@open-metadata.org`
   - Password: `admin`

3. Navigate to **Settings** (gear icon) > **Bots** > **ingestion-bot**

4. Click on the **ingestion-bot** and copy the JWT token

## Step 3: Update the JWT Token

Run the token update script:

```bash
./scripts/update_jwt_token.sh
```

When prompted, paste the JWT token you copied from OpenMetadata.

Restart the ingestion container to apply the new token:

```bash
docker compose restart ingestion
```

## Step 4: Load CLUE Data

Ensure the required CSV files are in the project root directory, then run:

```bash
./load_clue_data.sh
```

This script will:
1. Set up the database schema
2. Load the case types lookup table
3. Load the court data files
4. Process data into cases, defendants, and plaintiffs tables
5. Refresh data for lineage capture

Expected output:
```
========================================
CLUE Maryland Data Loader
========================================
...
✓ Schema setup complete
✓ Copied 1 court data file(s)
✓ Case types loaded
✓ All files loaded into source_data
✓ Data processing complete
...
========================================
Data loading complete!
========================================
```

## Step 5: Ingest CLUE Metadata into OpenMetadata

1. Open the Airflow web UI at http://localhost:8080

2. Log in with default credentials:
   - Username: `admin`
   - Password: `admin`

3. Find the `clue_metadata_ingestion` DAG

4. Toggle the DAG to **ON** if it's paused

5. Click the **Play** button to trigger a manual run

The DAG will execute three tasks in sequence:
1. `ingest_openmetadata_views` - Ingests table metadata
2. `profile_openmetadata_views` - Profiles tables
3. `ingest_openmetadata_lineage` - Captures data lineage

## Step 6: View Results in OpenMetadata

Once the DAG completes successfully:

1. Go to http://localhost:8585

2. Navigate to **Explore** > **Tables**

3. Search for `clue` to find the ingested tables:
   - `cases`
   - `defendants`
   - `plaintiffs`
   - `source_data`
   - `case_types`
   - `file_load_metadata`

4. Click on any table to view:
   - **Schema** - Column definitions
   - **Lineage** - Data flow showing how tables are derived

## Troubleshooting

### JWT Token Expired

If you see authentication errors, regenerate the token:
1. Go to OpenMetadata > Settings > Bots > ingestion-bot
2. Regenerate the token
3. Run `./scripts/update_jwt_token.sh` again
4. Restart the ingestion container

### Database Connection Issues

Verify PostgreSQL is running and healthy:

```bash
docker exec openmetadata_postgresql psql -U postgres -c "SELECT 1;"
```

### Lineage Not Showing

Lineage is captured from query logs. If lineage is missing:

```bash
# Re-run the processing procedure to capture lineage
docker exec openmetadata_postgresql psql -U postgres -d governance_catalog -c "
TRUNCATE clue.cases CASCADE;
UPDATE clue.file_load_metadata SET load_status = 'completed';
CALL clue.process_case_data();
"

# Then re-run the lineage DAG task
```

### Fresh Deployment

To completely reset and start fresh:

```bash
docker compose down -v
sudo rm -rf ./docker-volume/db-data-postgres
docker compose up -d
```

Then repeat from Step 2.
