-- PostgreSQL Setup Script for CLUE Maryland Data
-- This script creates tables and provides safer data loading

-- Drop existing tables if they exist
DROP TABLE IF EXISTS case_types CASCADE;
DROP TABLE IF EXISTS source_data CASCADE;
DROP TABLE IF EXISTS final_cases CASCADE;
DROP TABLE IF EXISTS final_defendants CASCADE;
DROP TABLE IF EXISTS final_plaintiffs CASCADE;

-- Create case types lookup table
CREATE TABLE case_types (
    case_count INTEGER,
    caseType TEXT,
    subtype TEXT,
    notes TEXT,
    col5 TEXT,
    col6 TEXT,
    col7 TEXT  -- For the extra columns in the CSV
);

-- Create source data table with all columns as TEXT initially for safe loading
CREATE TABLE source_data (
    row_num TEXT,
    id TEXT,
    caseNumber TEXT,
    caption TEXT,
    name TEXT,
    dob TEXT,
    partyType TEXT,
    court TEXT,
    courtId TEXT,
    caseType TEXT,
    filingDate TEXT,
    status TEXT,
    courtSystem TEXT,
    checksum TEXT,
    misc TEXT,
    lastScrape TEXT,
    notFoundAt TEXT,
    createdAt TEXT,
    updatedAt TEXT,
    trackingNumber TEXT,
    caseTypeCode TEXT,
    location TEXT,
    defendant TEXT,
    plaintiff TEXT,
    error TEXT,
    versions TEXT,
    parser TEXT,
    cjis TEXT,
    sentenceLength TEXT
);

-- Function to safely parse dates
CREATE OR REPLACE FUNCTION safe_to_date(date_str TEXT, format TEXT)
RETURNS DATE AS $$
BEGIN
    RETURN TO_DATE(date_str, format);
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMIT;

-- Instructions for loading data:
-- Run the following commands from psql, adjusting paths as needed:
--
-- \COPY case_types FROM '/home/kang/Documents/mdi/DGT/MD Clue Case Types in Redivis - Sheet1.csv' WITH (FORMAT csv, HEADER true, NULL 'NA');
--
-- \COPY source_data FROM '/home/kang/Documents/mdi/DGT/Maryland Data from CLUE_Intake Files_20220928_Allegany County Circuit Court.csv' WITH (FORMAT csv, HEADER true, NULL 'NA', ENCODING 'UTF8');
--
-- After loading, run: clue_maryland_postgres.sql
