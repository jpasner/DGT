# CLUE Maryland Court Data - Extensible Ingestion System

## Overview

This system provides an extensible, production-ready solution for ingesting multiple Maryland court case CSV files into PostgreSQL. It's designed to handle data from multiple counties and supports incremental loading.

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
│  - Handles errors gracefully                                │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│  PostgreSQL (governance-postgres container)                 │
│                                                             │
│  Schema: clue                                               │
│  ├── file_load_metadata (tracks all loads)                 │
│  ├── case_types (lookup table)                             │
│  ├── source_data (raw data from all CSVs)                  │
│  ├── cases (processed cases)                               │
│  ├── defendants (extracted from JSON)                      │
│  └── plaintiffs (extracted from JSON)                      │
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

### `clue.cases`
Processed and categorized cases:
- Filtered and cleaned data
- Categories: DEBT, EVICTION, POSSIBLE DEBT, POSSIBLE EVICTION, etc.
- Excludes criminal, family, traffic, probate cases

### `clue.defendants` & `clue.plaintiffs`
Party information extracted from JSON:
- One row per party (handles JSON arrays)
- Linked to cases via `row_id`

## Quick Start

### 1. Place CSV Files in Project Directory

```bash
cd /home/kang/Documents/mdi/DGT

# You should have:
# - MD Clue Case Types in Redivis - Sheet1.csv
# - Maryland Data from CLUE_Intake Files_*.csv (one or more)
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
5. Process data into final tables
6. Show summary statistics

### 3. Query the Data

```bash
# Connect to database
docker exec -it governance-postgres psql -U metadata_admin -d governance_catalog

# Switch to clue schema
SET search_path TO clue;

# View loaded files
SELECT * FROM file_load_metadata;

# View case statistics
SELECT category, COUNT(*) as count
FROM cases
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
FROM cases c
LEFT JOIN defendants d ON c.row_id = d.row_id
LIMIT 10;
```

## Adding More CSV Files

The system is designed for extensibility:

### Option 1: Add Files and Re-run
```bash
# Add new CSV files to directory
cp /path/to/new/Maryland_Data_*.csv .

# Re-run loader (it will skip already-loaded files)
./load_clue_data.sh
```

### Option 2: Load Individual File
```bash
docker cp "new_file.csv" governance-postgres:/tmp/
docker exec -it governance-postgres psql -U metadata_admin -d governance_catalog

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

-- Process the new data
CALL clue.process_case_data(<load_id>);
```

## Key Features

### 1. **Duplicate Prevention**
- `file_load_metadata.file_name` has UNIQUE constraint
- Re-running loader won't duplicate data
- Can track file hash for content-based deduplication

### 2. **Error Handling**
- Safe date parsing (returns NULL on error)
- Safe JSON parsing (handles malformed data)
- Load status tracking (started/completed/failed)

### 3. **Multi-County Support**
- Single `source_data` table for all counties
- `load_id` tracks which file each row came from
- Can query by county: `WHERE court LIKE '%County%'`

### 4. **Incremental Processing**
```sql
-- Process only new loads
CALL clue.process_case_data();

-- Reprocess specific load
CALL clue.process_case_data(123);
```

### 5. **Data Lineage**
```sql
-- Track data from source to final tables
SELECT
    fm.file_name,
    fm.load_started_at,
    COUNT(DISTINCT c.row_id) as cases_processed
FROM clue.file_load_metadata fm
LEFT JOIN clue.source_data sd ON fm.load_id = sd.load_id
LEFT JOIN clue.cases c ON sd.source_id = c.source_id
GROUP BY fm.file_name, fm.load_started_at
ORDER BY fm.load_started_at DESC;
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
FROM clue.cases
GROUP BY category
ORDER BY count DESC;

-- Cases by court
SELECT court, COUNT(*) as count
FROM clue.cases
GROUP BY court
ORDER BY count DESC
LIMIT 10;

-- Filing date range
SELECT
    MIN(file_date) as earliest_case,
    MAX(file_date) as latest_case,
    COUNT(*) as total_cases
FROM clue.cases;
```

### Cleanup Old Data
```sql
-- Delete specific load
DELETE FROM clue.source_data WHERE load_id = 123;
DELETE FROM clue.file_load_metadata WHERE load_id = 123;

-- Rebuild processed tables
TRUNCATE clue.cases, clue.defendants, clue.plaintiffs CASCADE;
CALL clue.process_case_data();
```

## File Structure

```
/home/kang/Documents/mdi/DGT/
├── db/
│   └── init/
│       ├── 01_create_clue_schema.sql      # Schema & tables
│       ├── 02_create_helper_functions.sql  # Safe parsing functions
│       └── 03_create_processing_procedure.sql # Data transformation
├── load_clue_data.sh                       # Main loader script
├── CLUE_README.md                          # This file
├── MD Clue Case Types in Redivis - Sheet1.csv  # Lookup table
└── Maryland Data from CLUE_Intake Files_*.csv   # Court data files
```

## Troubleshooting

### "Container not running"
```bash
docker-compose up -d postgres
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

### Reprocess data
```sql
-- Clear processed tables
TRUNCATE clue.cases, clue.defendants, clue.plaintiffs CASCADE;

-- Reprocess all
CALL clue.process_case_data();
```

## Performance Tips

- **Indexes**: Already created on key columns
- **Batch size**: Script processes all files in one run
- **Vacuum**: Run `VACUUM ANALYZE clue.source_data;` after large loads
- **Partitioning**: For very large datasets (millions of rows), consider partitioning by court or date

## Next Steps

1. Set up scheduled jobs for regular data updates
2. Add data quality checks and alerts
3. Create views for common query patterns
4. Integrate with OpenMetadata for data catalog
5. Set up incremental updates (CDC if source supports)

## Support

For issues or questions, check:
- PostgreSQL logs: `docker logs governance-postgres`
- Load metadata: `SELECT * FROM clue.file_load_metadata ORDER BY load_started_at DESC;`
