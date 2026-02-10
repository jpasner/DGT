# CLUE Maryland Court Data - Extensible Ingestion System

## Overview

This system provides an extensible, production-ready solution for ingesting multiple Maryland court case CSV files into PostgreSQL. It's designed to handle data from multiple counties, supports incremental loading, and creates views for full lineage tracking in OpenMetadata.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  CSV Files (Multiple Counties)                             │
│  - Maryland Data from CLUE_Intake Files_County1.csv        │
│  - Maryland Data from CLUE_Intake Files_County2.csv        │
│  - MD Clue Case Types in Redivis - Sheet1.csv (lookup)     │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  load_clue_data.sh (Batch Loader)                          │
│  - Copies files to container                                │
│  - Tracks each file load                                    │
│  - Creates transformation views for lineage                 │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  PostgreSQL (openmetadata_postgresql container)            │
│                                                             │
│  Schema: clue                                               │
│  ├── file_load_metadata (tracks all loads)                 │
│  ├── case_types (lookup table)                             │
│  ├── source_data (raw data from all CSVs)                  │
│  │                                                          │
│  │  Transformation Views (26 views for lineage):           │
│  ├── vw_label_cases                                        │
│  ├── vw_limit_by_subtype                                   │
│  ├── vw_real_nulls                                         │
│  ├── vw_drop_extras                                        │
│  ├── vw_case_table, vw_defendant_*, vw_plaintiff_*         │
│  ├── vw_save_off_the_non_civil                             │
│  ├── vw_catch_the_remaining_civil_cases                    │
│  ├── vw_all_cases, vw_all_defendants, vw_all_plaintiffs    │
│  │                                                          │
│  │  Final Output Tables:                                   │
│  ├── final_cases (processed cases)                         │
│  ├── final_defendants (extracted from JSON)                │
│  └── final_plaintiffs (extracted from JSON)                │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  OpenMetadata (via clue_metadata_ingestion DAG)            │
│  - Ingests all tables and views                            │
│  - Captures full lineage from views                        │
│  - Shows transformation pipeline in lineage graph          │
└─────────────────────────────────────────────────────────────┘
```

## Database Schema

### `clue.file_load_metadata`
Tracks every CSV file loaded into the system:
- `load_id` - Unique identifier for each load
- `file_name` - Name of the CSV file
- `rows_loaded` - Number of rows loaded
- `load_status` - started | completed | failed
- `load_started_at`, `load_completed_at` - Timestamps

### `clue.source_data`
Raw data from all CSV files:
- Contains all columns from source CSVs
- References `load_id` to track which file each row came from
- Supports multiple counties/courts in single table

### `clue.case_types`
Lookup table for case categorization:
- Maps case types to subtypes (D=DEBT, E=EVICTION, etc.)

### Transformation Views (26 views)
These views provide full lineage tracking in OpenMetadata:

| View | Description |
|------|-------------|
| `vw_label_cases` | Join source_data with case_types lookup |
| `vw_limit_by_subtype` | Filter to civil case subtypes (D, E, PD, PE) |
| `vw_real_nulls` | Replace 'NA' strings with NULL |
| `vw_drop_extras` | Filter out criminal/family/traffic/probate cases |
| `vw_case_table` | Extract case data (Branch A) |
| `vw_defendant_json/array` | Extract defendant JSON |
| `vw_unpack_defendant_json` | Unpack defendant fields |
| `vw_plaintiff_json/array` | Extract plaintiff JSON |
| `vw_unpack_plaintiff_json` | Unpack plaintiff fields |
| `vw_save_off_the_non_civil` | Non-civil cases (Branch B) |
| `vw_fix_the_nulls_here_too` | Fix nulls for Branch B |
| `vw_catch_the_remaining_civil_cases` | Remaining civil cases (Branch B1) |
| `vw_extra_case/defendant/plaintiff` | Branch B1 extractions |
| `vw_remaining_civil_cases_2` | Case-insensitive civil match (Branch B2) |
| `vw_all_cases/defendants/plaintiffs` | Combined output views |

### `clue.final_cases`
Processed and categorized cases:
- Filtered and cleaned data
- Categories: DEBT, EVICTION, POSSIBLE DEBT, POSSIBLE EVICTION, etc.
- Excludes criminal, family, traffic, probate cases

### `clue.final_defendants` & `clue.final_plaintiffs`
Party information extracted from JSON:
- One row per party (handles JSON arrays)
- Linked to cases via `row_id`

## Quick Start

### 1. Place CSV Files in Project Directory

```bash
cd /home/kang/Documents/mdi/temp/DGT

# You should have:
# - MD Clue Case Types in Redivis - Sheet1.csv
# - Maryland Data from CLUE_Intake Files_*.csv (one or more)
# - clue_maryland_postgres.sql (transformation script)
```

### 2. Run the Loader Script

```bash
./load_clue_data.sh
```

This will:
1. Set up database schema
2. Copy CSV files to container
3. Load case types lookup table
4. Load all court data files
5. Create transformation views and final tables
6. Show summary statistics

### 3. Trigger OpenMetadata Ingestion

```bash
# Via Airflow UI, trigger the clue_metadata_ingestion DAG
# Or manually:
docker exec -it openmetadata_ingestion bash
metadata ingest -c /opt/airflow/openmetadata/clue_ingestion.yaml
metadata ingest -c /opt/airflow/openmetadata/clue_lineage.yaml
```

### 4. Query the Data

```bash
# Connect to database
docker exec -it openmetadata_postgresql psql -U postgres -d governance_catalog

# Switch to clue schema
SET search_path TO clue;

# View loaded files
SELECT * FROM file_load_metadata;

# View case statistics
SELECT category, COUNT(*) as count
FROM final_cases
WHERE category IS NOT NULL
GROUP BY category
ORDER BY count DESC;

# Sample cases with defendants
SELECT
    c.case_number,
    c.caption,
    c.category,
    c.file_date,
    d.name as defendant_name,
    d.city,
    d.state
FROM final_cases c
LEFT JOIN final_defendants d ON c.row_id = d.row_id
LIMIT 10;

# View all transformation views
SELECT viewname FROM pg_views WHERE schemaname = 'clue' ORDER BY viewname;
```

## Adding More CSV Files

The system is designed for extensibility:

### Option 1: Add Files and Re-run
```bash
# Add new CSV files to directory
cp /path/to/new/Maryland_Data_*.csv .

# Re-run loader (it will reload all data)
./load_clue_data.sh
```

### Option 2: Load Individual File
```bash
docker cp "new_file.csv" openmetadata_postgresql:/tmp/
docker exec -it openmetadata_postgresql psql -U postgres -d governance_catalog

# Then in psql:
BEGIN;

INSERT INTO clue.file_load_metadata (file_name, load_status)
VALUES ('new_file.csv', 'started')
RETURNING load_id;

-- Use the returned load_id
\COPY clue.source_data (...columns...) FROM '/tmp/new_file.csv' WITH CSV HEADER;

UPDATE clue.source_data SET load_id = <load_id> WHERE load_id IS NULL;

UPDATE clue.file_load_metadata
SET load_status = 'completed',
    load_completed_at = CURRENT_TIMESTAMP,
    rows_loaded = (SELECT COUNT(*) FROM clue.source_data WHERE load_id = <load_id>)
WHERE load_id = <load_id>;

COMMIT;

-- Recreate views and final tables
\i clue_maryland_postgres.sql
```

## Key Features

### 1. **Full Lineage Tracking**
- 26 views create a visible transformation pipeline
- OpenMetadata captures view dependencies automatically
- Lineage graph matches the Redivis data curation workflow

### 2. **Duplicate Prevention**
- `file_load_metadata.file_name` has UNIQUE constraint
- Re-running loader won't duplicate data
- Can track file hash for content-based deduplication

### 3. **Error Handling**
- Safe date parsing (returns NULL on error)
- Safe JSON parsing (handles malformed data)
- Load status tracking (started/completed/failed)

### 4. **Multi-County Support**
- Single `source_data` table for all counties
- `load_id` tracks which file each row came from
- Can query by county: `WHERE court LIKE '%County%'`

### 5. **Data Lineage Queries**
```sql
-- Track data through the pipeline
SELECT
    fm.file_name,
    fm.load_started_at,
    fm.rows_loaded as source_rows,
    (SELECT COUNT(*) FROM clue.final_cases) as processed_cases
FROM clue.file_load_metadata fm
WHERE fm.load_status = 'completed'
ORDER BY fm.load_started_at DESC;

-- View the transformation chain
SELECT
    'source_data' as step, COUNT(*) as rows FROM clue.source_data
UNION ALL SELECT 'vw_label_cases', COUNT(*) FROM clue.vw_label_cases
UNION ALL SELECT 'vw_limit_by_subtype', COUNT(*) FROM clue.vw_limit_by_subtype
UNION ALL SELECT 'vw_drop_extras', COUNT(*) FROM clue.vw_drop_extras
UNION ALL SELECT 'final_cases', COUNT(*) FROM clue.final_cases;
```

## Monitoring & Maintenance

### Check Load Status
```sql
SELECT
    file_name,
    rows_loaded,
    load_status,
    load_completed_at - load_started_at as duration,
    error_message
FROM clue.file_load_metadata
ORDER BY load_started_at DESC;
```

### View Statistics
```sql
-- Cases by category
SELECT category, COUNT(*) as count
FROM clue.final_cases
GROUP BY category
ORDER BY count DESC;

-- Cases by court
SELECT court, COUNT(*) as count
FROM clue.final_cases
GROUP BY court
ORDER BY count DESC
LIMIT 10;

-- Filing date range
SELECT
    MIN(file_date) as earliest_case,
    MAX(file_date) as latest_case,
    COUNT(*) as total_cases
FROM clue.final_cases;
```

### Rebuild Data
```sql
-- Drop and recreate views + final tables
-- Run the transformation script
\i clue_maryland_postgres.sql
```

## File Structure

```
/home/kang/Documents/mdi/temp/DGT/
├── db/
│   └── init/
│       ├── 01_create_clue_schema.sql       # Schema & base tables
│       └── 02_create_helper_functions.sql  # Safe parsing functions
├── openmetadata/
│   ├── clue_ingestion.yaml                 # Table/view ingestion config
│   ├── clue_lineage.yaml                   # Lineage ingestion config
│   └── clue_profiler.yaml                  # Data profiling config
├── airflow/dags/
│   └── clue_metadata_ingestion.py          # Airflow DAG for OpenMetadata
├── load_clue_data.sh                       # Main loader script
├── clue_maryland_postgres.sql              # View-based transformation script
├── CLUE_README.md                          # This file
├── MD Clue Case Types in Redivis - Sheet1.csv  # Lookup table
└── Maryland Data from CLUE_Intake Files_*.csv   # Court data files
```

## Troubleshooting

### "Container not running"
```bash
docker-compose up -d
```

### "Permission denied" on script
```bash
chmod +x load_clue_data.sh
```

### "Files not found"
Make sure CSV files are in the project root directory (same level as load_clue_data.sh)

### Check logs for failed loads
```sql
SELECT * FROM clue.file_load_metadata WHERE load_status = 'failed';
```

### Rebuild views and tables
```sql
-- Re-run the transformation script
\i clue_maryland_postgres.sql
```

## Deprecated Tables

The following tables are deprecated and will be removed in a future version:
- `clue.cases` - replaced by `clue.final_cases`
- `clue.defendants` - replaced by `clue.final_defendants`
- `clue.plaintiffs` - replaced by `clue.final_plaintiffs`

The stored procedure `clue.process_case_data()` is also deprecated.

## Performance Tips

- **Indexes**: Already created on key columns in source tables
- **Views**: Views are computed on-demand; final_* tables are materialized
- **Vacuum**: Run `VACUUM ANALYZE clue.source_data;` after large loads
- **Refresh**: To refresh final tables, re-run `clue_maryland_postgres.sql`

## OpenMetadata Integration

The system is designed for full integration with OpenMetadata:

1. **Metadata Ingestion**: `clue_ingestion.yaml` ingests all tables and views
2. **Lineage Capture**: `clue_lineage.yaml` captures view dependencies
3. **Data Profiling**: `clue_profiler.yaml` computes column statistics

Run the `clue_metadata_ingestion` Airflow DAG to:
1. Ingest metadata (tables + views)
2. Profile data
3. Capture lineage

The lineage graph will show the full transformation pipeline from `source_data` through all intermediate views to `final_cases`, `final_defendants`, and `final_plaintiffs`.

## Support

For issues or questions, check:
- PostgreSQL logs: `docker logs openmetadata_postgresql`
- Load metadata: `SELECT * FROM clue.file_load_metadata ORDER BY load_started_at DESC;`
- OpenMetadata UI: Check lineage graph for the clue schema tables
