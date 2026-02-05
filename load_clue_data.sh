#!/bin/bash
# Extensible CLUE Data Loader
# This script loads multiple Maryland court case CSV files into PostgreSQL

set -e

# Configuration
DB_CONTAINER="openmetadata_postgresql"
DB_NAME="governance_catalog"
DB_USER="postgres"
DB_PASSWORD="postgres"
DATA_DIR="/tmp/clue_data"
CASE_TYPES_FILE="MD Clue Case Types in Redivis - Sheet1.csv"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}CLUE Maryland Data Loader${NC}"
echo -e "${GREEN}========================================${NC}"

# Check if container is running
if ! docker ps | grep -q "$DB_CONTAINER"; then
    echo -e "${RED}Error: Container $DB_CONTAINER is not running${NC}"
    exit 1
fi

echo -e "${YELLOW}Step 1: Setting up database schema...${NC}"

# Execute schema setup scripts in order
for script in db/init/01_create_clue_schema.sql \
              db/init/02_create_helper_functions.sql \
              db/init/03_create_processing_procedure.sql; do
    if [ -f "$script" ]; then
        echo "  Executing: $script"
        docker exec -i $DB_CONTAINER psql -U $DB_USER -d $DB_NAME < "$script"
    else
        echo -e "${RED}  Warning: $script not found${NC}"
    fi
done

echo -e "${GREEN}✓ Schema setup complete${NC}"

# Create temp directory in container
echo -e "${YELLOW}Step 2: Copying CSV files to container...${NC}"
docker exec $DB_CONTAINER mkdir -p $DATA_DIR

# Copy case types file
if [ -f "$CASE_TYPES_FILE" ]; then
    echo "  Copying case types lookup file..."
    docker cp "$CASE_TYPES_FILE" $DB_CONTAINER:$DATA_DIR/
    CASE_TYPES_BASENAME=$(basename "$CASE_TYPES_FILE")
else
    echo -e "${RED}Error: Case types file not found: $CASE_TYPES_FILE${NC}"
    exit 1
fi

# Copy all Maryland court data CSV files
COUNT=0
for file in "Maryland Data from"*.csv; do
    if [ -f "$file" ]; then
        echo "  Copying: $file"
        docker cp "$file" $DB_CONTAINER:$DATA_DIR/
        COUNT=$((COUNT + 1))
    fi
done

if [ $COUNT -eq 0 ]; then
    echo -e "${RED}Error: No Maryland court data CSV files found${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Copied $COUNT court data file(s)${NC}"

# Preprocess case types CSV to remove duplicate header lines and fix line endings
echo -e "${YELLOW}Step 2b: Preprocessing CSV files...${NC}"
docker exec $DB_CONTAINER bash -c "
    cd $DATA_DIR
    # Convert Windows line endings to Unix and remove duplicate headers
    sed -i 's/\r$//' '$CASE_TYPES_BASENAME'
    head -1 '$CASE_TYPES_BASENAME' > tmp_case_types.csv
    tail -n +2 '$CASE_TYPES_BASENAME' | grep -v '^case_count,' >> tmp_case_types.csv
    mv tmp_case_types.csv '$CASE_TYPES_BASENAME'
    echo \"Preprocessed case types: \$(wc -l < '$CASE_TYPES_BASENAME') lines\"
"
echo -e "${GREEN}✓ CSV preprocessing complete${NC}"

# Load case types (one-time)
echo -e "${YELLOW}Step 3: Loading case types lookup table...${NC}"
docker exec -i $DB_CONTAINER psql -U $DB_USER -d $DB_NAME <<-EOSQL
    TRUNCATE clue.case_types CASCADE;
    COPY clue.case_types (case_count, case_type, subtype, notes, col5, col6, col7)
    FROM '$DATA_DIR/$CASE_TYPES_BASENAME' WITH (FORMAT csv, HEADER true, NULL 'NA');
    SELECT COUNT(*) as case_type_count FROM clue.case_types;
EOSQL

echo -e "${GREEN}✓ Case types loaded${NC}"

# Load each court data file
echo -e "${YELLOW}Step 4: Loading court data files...${NC}"

docker exec -i $DB_CONTAINER bash <<-'EOSCRIPT'
set -e
DATA_DIR=/tmp/clue_data
DB_NAME=governance_catalog
DB_USER=postgres

cd $DATA_DIR

for file in "Maryland Data from"*.csv; do
    if [ -f "$file" ]; then
        echo "Processing: $file"

        # Get file info
        FILE_SIZE=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null)

        # Insert load metadata and get load_id
        LOAD_ID=$(psql -U $DB_USER -d $DB_NAME -t -A -q -c "
            INSERT INTO clue.file_load_metadata (
                file_name, file_path, file_size_bytes, load_status
            ) VALUES (
                '$file', '$DATA_DIR/$file', $FILE_SIZE, 'started'
            )
            ON CONFLICT (file_name) DO UPDATE
            SET load_started_at = CURRENT_TIMESTAMP,
                load_status = 'started'
            RETURNING load_id;
        " | tail -1 | tr -d '[:space:]')

        echo "  Load ID: $LOAD_ID"

        # Load data
        psql -U $DB_USER -d $DB_NAME <<-EOSQL
            COPY clue.source_data (
                row_num, id, case_number, caption, name, dob, party_type, court,
                court_id, case_type, filing_date, status, court_system, checksum,
                misc, last_scrape, not_found_at, created_at, updated_at,
                tracking_number, case_type_code, location, defendant, plaintiff,
                error, versions, parser, cjis, sentence_length
            ) FROM '$DATA_DIR/$file' WITH (FORMAT csv, HEADER true, NULL 'NA');

            -- Update source_data with load_id
            UPDATE clue.source_data
            SET load_id = $LOAD_ID
            WHERE load_id IS NULL;

            -- Update metadata
            UPDATE clue.file_load_metadata
            SET rows_loaded = (SELECT COUNT(*) FROM clue.source_data WHERE load_id = $LOAD_ID),
                load_completed_at = CURRENT_TIMESTAMP,
                load_status = 'completed'
            WHERE load_id = $LOAD_ID;
EOSQL

        if [ $? -eq 0 ]; then
            echo "  ✓ Loaded successfully"
        else
            psql -U $DB_USER -d $DB_NAME -c "
                UPDATE clue.file_load_metadata
                SET load_status = 'failed',
                    error_message = 'Copy command failed'
                WHERE load_id = $LOAD_ID;
            "
            echo "  ✗ Failed to load"
        fi
    fi
done
EOSCRIPT

echo -e "${GREEN}✓ All files loaded into source_data${NC}"

# Process the data
echo -e "${YELLOW}Step 5: Processing data into final tables...${NC}"
docker exec -i $DB_CONTAINER psql -U $DB_USER -d $DB_NAME <<-EOSQL
    CALL clue.process_case_data();
EOSQL

echo -e "${GREEN}✓ Data processing complete${NC}"

# Show summary
echo -e "${YELLOW}Step 6: Summary Statistics${NC}"
docker exec -i $DB_CONTAINER psql -U $DB_USER -d $DB_NAME <<-EOSQL
    SELECT
        'Files Loaded' as metric,
        COUNT(*)::text as count
    FROM clue.file_load_metadata
    WHERE load_status = 'completed'
    UNION ALL
    SELECT
        'Total Source Rows',
        COUNT(*)::text
    FROM clue.source_data
    UNION ALL
    SELECT
        'Processed Cases',
        COUNT(*)::text
    FROM clue.cases
    UNION ALL
    SELECT
        'Defendants',
        COUNT(*)::text
    FROM clue.defendants
    UNION ALL
    SELECT
        'Plaintiffs',
        COUNT(*)::text
    FROM clue.plaintiffs;

    \echo ''
    \echo 'Case Categories:'
    SELECT
        category,
        COUNT(*) as count,
        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM clue.cases
    WHERE category IS NOT NULL
    GROUP BY category
    ORDER BY count DESC;
EOSQL

# Cleanup
echo -e "${YELLOW}Step 7: Cleanup${NC}"
docker exec $DB_CONTAINER rm -rf $DATA_DIR
echo -e "${GREEN}✓ Temporary files cleaned up${NC}"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Data loading complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Connect to database:"
echo "  docker exec -it $DB_CONTAINER psql -U $DB_USER -d $DB_NAME"
echo ""
echo "Query examples:"
echo "  SELECT * FROM clue.cases LIMIT 10;"
echo "  SELECT * FROM clue.file_load_metadata;"
