-- PostgreSQL-compatible version of CLUE Maryland ingestion script
-- This script transforms Maryland court case data and categorizes cases
-- Prerequisites: Run setup_postgres.sql first and load the CSV data

-- Drop final tables if they exist (source tables should already be loaded)
DROP TABLE IF EXISTS final_cases CASCADE;
DROP TABLE IF EXISTS final_defendants CASCADE;
DROP TABLE IF EXISTS final_plaintiffs CASCADE;

-- Function to safely parse dates (will return NULL on error)
CREATE OR REPLACE FUNCTION safe_to_date(date_str TEXT, format TEXT)
RETURNS DATE AS $$
BEGIN
    IF date_str IS NULL OR date_str = '' OR date_str = 'NA' THEN
        RETURN NULL;
    END IF;
    RETURN TO_DATE(date_str, format);
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to safely parse JSON
CREATE OR REPLACE FUNCTION safe_json(text_str TEXT)
RETURNS JSONB AS $$
BEGIN
    IF text_str IS NULL OR text_str = '' OR text_str = 'NA' THEN
        RETURN NULL;
    END IF;
    RETURN text_str::jsonb;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Create indexes for better performance if they don't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_source_id') THEN
        CREATE INDEX idx_source_id ON source_data(id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_source_caseType') THEN
        CREATE INDEX idx_source_caseType ON source_data(caseType);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_casetype_lookup') THEN
        CREATE INDEX idx_casetype_lookup ON case_types(caseType);
    END IF;
END $$;

-- Main query with all transformations
WITH
label_cases AS (
    SELECT
        t0.id AS id,
        t0.caseNumber AS caseNumber,
        t0.caption AS caption,
        t0.name AS name,
        t0.dob AS dob,
        t0.partyType AS partyType,
        t0.court AS court,
        t0.courtId AS courtId,
        t0.caseType AS caseType,
        t0.filingDate AS filingDate,
        t0.status AS status,
        t0.courtSystem AS courtSystem,
        t0.checksum AS checksum,
        t0.misc AS misc,
        t0.lastScrape AS lastScrape,
        t0.notFoundAt AS notFoundAt,
        t0.createdAt AS createdAt,
        t0.updatedAt AS updatedAt,
        t0.trackingNumber AS trackingNumber,
        t0.caseTypeCode AS caseTypeCode,
        t0.location AS location,
        t0.defendant AS defendant,
        t0.plaintiff AS plaintiff,
        t0.error AS error,
        t0.versions AS versions,
        t0.parser AS parser,
        t0.cjis AS cjis,
        t0.sentenceLength AS sentenceLength,
        t1.subtype as subtype
    FROM
        source_data AS t0
        LEFT JOIN case_types t1 ON t0.caseType = t1.caseType
),
-- Branch A
limit_by_subtype AS (
    SELECT
        t0.id AS id,
        t0.caseNumber AS caseNumber,
        t0.caption AS caption,
        t0.name AS name,
        t0.dob AS dob,
        t0.partyType AS partyType,
        t0.court AS court,
        t0.courtId AS courtId,
        t0.caseType AS caseType,
        t0.filingDate AS filingDate,
        t0.status AS status,
        t0.courtSystem AS courtSystem,
        t0.checksum AS checksum,
        t0.misc AS misc,
        t0.lastScrape AS lastScrape,
        t0.notFoundAt AS notFoundAt,
        t0.createdAt AS createdAt,
        t0.updatedAt AS updatedAt,
        t0.trackingNumber AS trackingNumber,
        t0.caseTypeCode AS caseTypeCode,
        t0.location AS location,
        t0.defendant AS defendant,
        t0.plaintiff AS plaintiff,
        t0.error AS error,
        t0.versions AS versions,
        t0.parser AS parser,
        t0.cjis AS cjis,
        t0.sentenceLength AS sentenceLength,
        CASE
          WHEN t0.subtype IN ('PE, PD', 'PD, PE') THEN 'UNDETERMINED'
          WHEN t0.subtype = 'D' THEN 'DEBT'
          WHEN t0.subtype = 'E' THEN 'EVICTION'
          WHEN t0.subtype = 'PD' THEN 'POSSIBLE DEBT'
          WHEN t0.subtype = 'PE' THEN 'POSSIBLE EVICTION'
          WHEN t0.subtype IS NULL THEN 'NO INFORMATION'
        ELSE NULL END AS clearType
    FROM
        label_cases AS t0
    WHERE
        (t0.subtype = 'D')
        OR (t0.subtype = 'E')
        OR (t0.subtype = 'PE')
        OR (t0.subtype = 'PD')
        OR (t0.subtype = 'PE, PD')
        OR (t0.subtype = 'PD, PE')
),
real_nulls AS (
    SELECT
        t0.id AS id,
        CASE WHEN t0.caseNumber = 'NA' THEN NULL ELSE t0.caseNumber END AS caseNumber,
        CASE WHEN t0.caption = 'NA' THEN NULL ELSE t0.caption END AS caption,
        CASE WHEN t0.name = 'NA' THEN NULL ELSE t0.name END AS name,
        CASE WHEN t0.dob = 'NA' THEN NULL ELSE t0.dob END AS dob,
        CASE WHEN t0.partyType = 'NA' THEN NULL ELSE t0.partyType END AS partyType,
        CASE WHEN t0.court = 'NA' THEN NULL ELSE t0.court END AS court,
        CASE WHEN t0.courtId = 'NA' THEN NULL ELSE t0.courtId END AS courtId,
        CASE WHEN t0.caseType = 'NA' THEN NULL ELSE t0.caseType END AS caseType,
        CASE WHEN t0.filingDate = 'NA' THEN NULL ELSE t0.filingDate END AS filingDate,
        CASE WHEN t0.status = 'NA' THEN NULL ELSE t0.status END AS status,
        CASE WHEN t0.courtSystem = 'NA' THEN NULL ELSE t0.courtSystem END AS courtSystem,
        CASE WHEN t0.checksum = 'NA' THEN NULL ELSE t0.checksum END AS checksum,
        CASE WHEN t0.misc = 'NA' THEN NULL ELSE t0.misc END AS misc,
        CASE WHEN t0.lastScrape = 'NA' THEN NULL ELSE t0.lastScrape END AS lastScrape,
        CASE WHEN t0.notFoundAt = 'NA' THEN NULL ELSE t0.notFoundAt END AS notFoundAt,
        t0.createdAt AS createdAt,
        t0.updatedAt AS updatedAt,
        CASE WHEN t0.trackingNumber = 'NA' THEN NULL ELSE t0.trackingNumber END AS trackingNumber,
        CASE WHEN t0.caseTypeCode = 'NA' THEN NULL ELSE t0.caseTypeCode END AS caseTypeCode,
        CASE WHEN t0.location = 'NA' THEN NULL ELSE t0.location END AS location,
        CASE WHEN t0.defendant = 'NA' THEN NULL ELSE t0.defendant END AS defendant,
        CASE WHEN t0.plaintiff = 'NA' THEN NULL ELSE t0.plaintiff END AS plaintiff,
        t0.error AS error,
        CASE WHEN t0.versions = 'NA' THEN NULL ELSE t0.versions END AS versions,
        CASE WHEN t0.parser = 'NA' THEN NULL ELSE t0.parser END AS parser,
        CASE WHEN t0.cjis = 'NA' THEN NULL ELSE t0.cjis END AS cjis,
        CASE WHEN t0.sentenceLength = 'NA' THEN NULL ELSE t0.sentenceLength END AS sentenceLength,
        CASE WHEN t0.clearType = 'NA' THEN NULL ELSE t0.clearType END AS clearType
    FROM
        limit_by_subtype AS t0
),
-- Branch A1
drop_extras AS (
    SELECT
        t0.id AS id,
        t0.caseNumber AS caseNumber,
        t0.caption AS caption,
        t0.name AS name,
        t0.dob AS dob,
        t0.partyType AS partyType,
        t0.court AS court,
        t0.courtId AS courtId,
        t0.caseType AS caseType,
        t0.filingDate AS filingDate,
        t0.status AS status,
        t0.courtSystem AS courtSystem,
        t0.checksum AS checksum,
        t0.misc AS misc,
        t0.lastScrape AS lastScrape,
        t0.notFoundAt AS notFoundAt,
        t0.createdAt AS createdAt,
        t0.updatedAt AS updatedAt,
        t0.trackingNumber AS trackingNumber,
        t0.caseTypeCode AS caseTypeCode,
        t0.location AS location,
        t0.defendant AS defendant,
        t0.plaintiff AS plaintiff,
        t0.error AS error,
        t0.versions AS versions,
        t0.parser AS parser,
        t0.cjis AS cjis,
        t0.sentenceLength AS sentenceLength,
        t0.clearType AS clearType
    FROM
        real_nulls AS t0
    WHERE
        (UPPER(courtSystem) NOT LIKE '%CRIMINAL%' OR courtSystem IS NULL) AND
        (UPPER(courtSystem) NOT LIKE '%FAMILY%' OR courtSystem IS NULL) AND
        (UPPER(courtSystem) NOT LIKE '%TRAFFIC%' OR courtSystem IS NULL) AND
        (UPPER(courtSystem) NOT LIKE '%PROBATE%' OR courtSystem IS NULL) AND
        cjis IS NULL
),
-- Branch A1a - Case table
case_table AS (
    SELECT
        DISTINCT t0.id::BIGINT AS rowID,
        t0.caseNumber AS caseNumber,
        t0.caption AS caption,
        t0.name AS name,
        t0.dob AS birthDate,
        t0.partyType AS role,
        t0.court AS court,
        t0.caseType AS caseType,
        safe_to_date(t0.filingDate, 'YYYY-MM-DD') AS fileDate,
        t0.status AS caseStatus,
        t0.courtSystem AS courtSystem,
        t0.createdAt AS createDate,
        t0.updatedAt AS updateDate,
        t0.location AS courtLocation,
        t0.versions AS versions,
        t0.clearType AS category
    FROM
        drop_extras AS t0
),
-- Branch A1b - Defendant table (expanded from JSON)
defendant_json AS (
    SELECT
        t0.id::BIGINT AS rowID,
        safe_json(t0.defendant) AS defendant
    FROM drop_extras AS t0
    WHERE t0.defendant IS NOT NULL AND t0.defendant != 'NA'
),
defendant_array AS (
    SELECT
        rowID,
        CASE
            WHEN jsonb_typeof(defendant) = 'array' THEN
                jsonb_array_elements(defendant)
            ELSE defendant
        END AS defendant
    FROM defendant_json
    WHERE defendant IS NOT NULL
),
unpack_defendant_json AS (
    SELECT
        rowID,
        defendant->>'Name' AS name,
        defendant->>'City' AS city,
        defendant->>'State' AS state,
        defendant->>'Zip Code' AS zip,
        defendant->>'Address' AS address,
        defendant->>'Address1' AS addressLn1,
        defendant->>'Amount' AS addressLn2,
        defendant->>'Party No.' AS partyNum,
        defendant->>'Party Type' AS role,
        defendant->>'Business or Organization Name' AS orgName,
        safe_to_date(defendant->>'Removal Date', 'MM/DD/YYYY') AS removalDate
    FROM defendant_array
),
-- Branch A1c - Plaintiff table (expanded from JSON)
plaintiff_json AS (
    SELECT
        t0.id::BIGINT AS rowID,
        safe_json(t0.plaintiff) AS plaintiff
    FROM drop_extras AS t0
    WHERE t0.plaintiff IS NOT NULL AND t0.plaintiff != 'NA'
),
plaintiff_array AS (
    SELECT
        rowID,
        CASE
            WHEN jsonb_typeof(plaintiff) = 'array' THEN
                jsonb_array_elements(plaintiff)
            ELSE plaintiff
        END AS plaintiff
    FROM plaintiff_json
    WHERE plaintiff IS NOT NULL
),
unpack_plaintiff_json AS (
    SELECT
        rowID,
        plaintiff->>'Name' AS name,
        plaintiff->>'City' AS city,
        plaintiff->>'State' AS state,
        plaintiff->>'Zip Code' AS zip,
        plaintiff->>'Address' AS address,
        plaintiff->>'Address1' AS addressLn1,
        plaintiff->>'Amount' AS addressLn2,
        plaintiff->>'Party No.' AS partyNum,
        plaintiff->>'Party Type' AS role,
        plaintiff->>'Business or Organization Name' AS orgName,
        safe_to_date(plaintiff->>'Removal Date', 'MM/DD/YYYY') AS removalDate
    FROM plaintiff_array
),
-- Branch B - Non-civil cases
save_off_the_non_civil AS (
    SELECT
        t0.id AS id,
        t0.caseNumber AS caseNumber,
        t0.caption AS caption,
        t0.name AS name,
        t0.dob AS dob,
        t0.partyType AS partyType,
        t0.court AS court,
        t0.courtId AS courtId,
        t0.caseType AS caseType,
        t0.filingDate AS filingDate,
        t0.status AS status,
        t0.courtSystem AS courtSystem,
        t0.checksum AS checksum,
        t0.misc AS misc,
        t0.lastScrape AS lastScrape,
        t0.notFoundAt AS notFoundAt,
        t0.createdAt AS createdAt,
        t0.updatedAt AS updatedAt,
        t0.trackingNumber AS trackingNumber,
        t0.caseTypeCode AS caseTypeCode,
        t0.location AS location,
        t0.defendant AS defendant,
        t0.plaintiff AS plaintiff,
        t0.error AS error,
        t0.versions AS versions,
        t0.parser AS parser,
        t0.cjis AS cjis,
        t0.sentenceLength AS sentenceLength,
        CASE
          WHEN t0.subtype IN ('PE, PD', 'PD, PE') THEN 'UNDETERMINED'
          WHEN t0.subtype = 'D' THEN 'DEBT'
          WHEN t0.subtype = 'E' THEN 'EVICTION'
          WHEN t0.subtype = 'PD' THEN 'POSSIBLE DEBT'
          WHEN t0.subtype = 'PE' THEN 'POSSIBLE EVICTION'
          WHEN t0.subtype = 'X' THEN 'OTHER CIVIL'
        ELSE 'NO INFORMATION' END AS clearType
    FROM
        label_cases AS t0
    WHERE
        ((t0.subtype != 'D')
        AND (t0.subtype != 'E')
        AND (t0.subtype != 'PE')
        AND (t0.subtype != 'PD')
        AND (t0.subtype != 'PE, PD')
        AND (t0.subtype != 'PD, PE'))
        OR t0.subtype IS NULL
),
fix_the_nulls_here_too AS (
    SELECT
        t0.id AS id,
        CASE WHEN t0.caseNumber = 'NA' THEN NULL ELSE t0.caseNumber END AS caseNumber,
        CASE WHEN t0.caption = 'NA' THEN NULL ELSE t0.caption END AS caption,
        CASE WHEN t0.name = 'NA' THEN NULL ELSE t0.name END AS name,
        CASE WHEN t0.dob = 'NA' THEN NULL ELSE t0.dob END AS dob,
        CASE WHEN t0.partyType = 'NA' THEN NULL ELSE t0.partyType END AS partyType,
        CASE WHEN t0.court = 'NA' THEN NULL ELSE t0.court END AS court,
        CASE WHEN t0.courtId = 'NA' THEN NULL ELSE t0.courtId END AS courtId,
        CASE WHEN t0.caseType = 'NA' THEN NULL ELSE t0.caseType END AS caseType,
        CASE WHEN t0.filingDate = 'NA' THEN NULL ELSE t0.filingDate END AS filingDate,
        CASE WHEN t0.status = 'NA' THEN NULL ELSE t0.status END AS status,
        CASE WHEN t0.courtSystem = 'NA' THEN NULL ELSE t0.courtSystem END AS courtSystem,
        CASE WHEN t0.checksum = 'NA' THEN NULL ELSE t0.checksum END AS checksum,
        CASE WHEN t0.misc = 'NA' THEN NULL ELSE t0.misc END AS misc,
        CASE WHEN t0.lastScrape = 'NA' THEN NULL ELSE t0.lastScrape END AS lastScrape,
        CASE WHEN t0.notFoundAt = 'NA' THEN NULL ELSE t0.notFoundAt END AS notFoundAt,
        t0.createdAt AS createdAt,
        t0.updatedAt AS updatedAt,
        CASE WHEN t0.trackingNumber = 'NA' THEN NULL ELSE t0.trackingNumber END AS trackingNumber,
        CASE WHEN t0.caseTypeCode = 'NA' THEN NULL ELSE t0.caseTypeCode END AS caseTypeCode,
        CASE WHEN t0.location = 'NA' THEN NULL ELSE t0.location END AS location,
        CASE WHEN t0.defendant = 'NA' THEN NULL ELSE t0.defendant END AS defendant,
        CASE WHEN t0.plaintiff = 'NA' THEN NULL ELSE t0.plaintiff END AS plaintiff,
        t0.error AS error,
        CASE WHEN t0.versions = 'NA' THEN NULL ELSE t0.versions END AS versions,
        CASE WHEN t0.parser = 'NA' THEN NULL ELSE t0.parser END AS parser,
        CASE WHEN t0.cjis = 'NA' THEN NULL ELSE t0.cjis END AS cjis,
        CASE WHEN t0.sentenceLength = 'NA' THEN NULL ELSE t0.sentenceLength END AS sentenceLength,
        CASE WHEN t0.clearType = 'NA' THEN NULL ELSE t0.clearType END AS clearType
    FROM
        save_off_the_non_civil AS t0
),
-- Branch B1 - Catch remaining civil cases
catch_the_remaining_civil_cases AS (
    SELECT
        t0.id AS id,
        t0.caseNumber AS caseNumber,
        t0.caption AS caption,
        t0.name AS name,
        t0.dob AS dob,
        t0.partyType AS partyType,
        t0.court AS court,
        t0.courtId AS courtId,
        t0.caseType AS caseType,
        t0.filingDate AS filingDate,
        t0.status AS status,
        t0.courtSystem AS courtSystem,
        t0.checksum AS checksum,
        t0.misc AS misc,
        t0.lastScrape AS lastScrape,
        t0.notFoundAt AS notFoundAt,
        t0.createdAt AS createdAt,
        t0.updatedAt AS updatedAt,
        t0.trackingNumber AS trackingNumber,
        t0.caseTypeCode AS caseTypeCode,
        t0.location AS location,
        t0.defendant AS defendant,
        t0.plaintiff AS plaintiff,
        t0.error AS error,
        t0.versions AS versions,
        t0.parser AS parser,
        t0.cjis AS cjis,
        t0.sentenceLength AS sentenceLength,
        t0.clearType AS clearType
    FROM
        fix_the_nulls_here_too AS t0
    WHERE courtSystem LIKE '%CIVIL%'
),
-- Branch B1a - Extra cases
extra_case AS (
    SELECT
        DISTINCT t0.id::BIGINT AS rowID,
        t0.caseNumber AS caseNumber,
        t0.caption AS caption,
        t0.name AS name,
        t0.dob AS birthDate,
        t0.partyType AS role,
        t0.court AS court,
        t0.caseType AS caseType,
        safe_to_date(t0.filingDate, 'YYYY-MM-DD') AS fileDate,
        t0.status AS caseStatus,
        t0.courtSystem AS courtSystem,
        t0.createdAt AS createDate,
        t0.updatedAt AS updateDate,
        t0.location AS courtLocation,
        t0.versions AS versions,
        t0.clearType AS category
    FROM
        catch_the_remaining_civil_cases AS t0
),
-- Branch B1b - Extra defendants
extra_defendant_json AS (
    SELECT
        t0.id::BIGINT AS rowID,
        safe_json(t0.defendant) AS defendant
    FROM catch_the_remaining_civil_cases AS t0
    WHERE t0.defendant IS NOT NULL AND t0.defendant != 'NA'
),
extra_defendant_array AS (
    SELECT
        rowID,
        CASE
            WHEN jsonb_typeof(defendant) = 'array' THEN
                jsonb_array_elements(defendant)
            ELSE defendant
        END AS defendant
    FROM extra_defendant_json
    WHERE defendant IS NOT NULL
),
unpack_extra_defendants AS (
    SELECT
        rowID,
        defendant->>'Name' AS name,
        defendant->>'City' AS city,
        defendant->>'State' AS state,
        defendant->>'Zip Code' AS zip,
        defendant->>'Address' AS address,
        defendant->>'Address1' AS addressLn1,
        defendant->>'Amount' AS addressLn2,
        defendant->>'Party No.' AS partyNum,
        defendant->>'Party Type' AS role,
        defendant->>'Business or Organization Name' AS orgName,
        safe_to_date(defendant->>'Removal Date', 'MM/DD/YYYY') AS removalDate
    FROM extra_defendant_array
),
-- Branch B1c - Extra plaintiffs
extra_plaintiff_json AS (
    SELECT
        t0.id::BIGINT AS rowID,
        safe_json(t0.plaintiff) AS plaintiff
    FROM catch_the_remaining_civil_cases AS t0
    WHERE t0.plaintiff IS NOT NULL AND t0.plaintiff != 'NA'
),
extra_plaintiff_array AS (
    SELECT
        rowID,
        CASE
            WHEN jsonb_typeof(plaintiff) = 'array' THEN
                jsonb_array_elements(plaintiff)
            ELSE plaintiff
        END AS plaintiff
    FROM extra_plaintiff_json
    WHERE plaintiff IS NOT NULL
),
unpack_extra_plaintiffs AS (
    SELECT
        rowID,
        plaintiff->>'Name' AS name,
        plaintiff->>'City' AS city,
        plaintiff->>'State' AS state,
        plaintiff->>'Zip Code' AS zip,
        plaintiff->>'Address' AS address,
        plaintiff->>'Address1' AS addressLn1,
        plaintiff->>'Amount' AS addressLn2,
        plaintiff->>'Party No.' AS partyNum,
        plaintiff->>'Party Type' AS role,
        plaintiff->>'Business or Organization Name' AS orgName,
        safe_to_date(plaintiff->>'Removal Date', 'MM/DD/YYYY') AS removalDate
    FROM extra_plaintiff_array
),
-- Branch B2 - Remaining civil cases 2
remaining_civil_cases_2 AS (
    SELECT
        t0.id AS id,
        t0.caseNumber AS caseNumber,
        t0.caption AS caption,
        t0.name AS name,
        t0.dob AS dob,
        t0.partyType AS partyType,
        t0.court AS court,
        t0.courtId AS courtId,
        t0.caseType AS caseType,
        t0.filingDate AS filingDate,
        t0.status AS status,
        t0.courtSystem AS courtSystem,
        t0.checksum AS checksum,
        t0.misc AS misc,
        t0.lastScrape AS lastScrape,
        t0.notFoundAt AS notFoundAt,
        t0.createdAt AS createdAt,
        t0.updatedAt AS updatedAt,
        t0.trackingNumber AS trackingNumber,
        t0.caseTypeCode AS caseTypeCode,
        t0.location AS location,
        t0.defendant AS defendant,
        t0.plaintiff AS plaintiff,
        t0.error AS error,
        t0.versions AS versions,
        t0.parser AS parser,
        t0.cjis AS cjis,
        t0.sentenceLength AS sentenceLength,
        t0.clearType AS clearType
    FROM
        fix_the_nulls_here_too AS t0
    WHERE
        courtSystem NOT LIKE '%CIVIL%' AND
        UPPER(courtSystem) LIKE '%CIVIL%'
),
-- Branch B2a - Extra cases 2
extra_case_2 AS (
    SELECT
        DISTINCT t0.id::BIGINT AS rowID,
        t0.caseNumber AS caseNumber,
        t0.caption AS caption,
        t0.name AS name,
        t0.dob AS birthDate,
        t0.partyType AS role,
        t0.court AS court,
        t0.caseType AS caseType,
        safe_to_date(t0.filingDate, 'YYYY-MM-DD') AS fileDate,
        t0.status AS caseStatus,
        t0.courtSystem AS courtSystem,
        t0.createdAt AS createDate,
        t0.updatedAt AS updateDate,
        t0.location AS courtLocation,
        t0.versions AS versions,
        t0.clearType AS category
    FROM
        remaining_civil_cases_2 AS t0
),
-- Combine all case data
all_cases AS (
    SELECT * FROM case_table
    UNION ALL
    SELECT * FROM extra_case
    UNION ALL
    SELECT * FROM extra_case_2
),
-- Combine all defendant data
all_defendants AS (
    SELECT * FROM unpack_defendant_json
    UNION ALL
    SELECT * FROM unpack_extra_defendants
),
-- Combine all plaintiff data
all_plaintiffs AS (
    SELECT * FROM unpack_plaintiff_json
    UNION ALL
    SELECT * FROM unpack_extra_plaintiffs
)

-- Create final output tables
-- Cases table
SELECT * INTO final_cases FROM all_cases;

-- Defendants table
SELECT * INTO final_defendants FROM all_defendants;

-- Plaintiffs table
SELECT * INTO final_plaintiffs FROM all_plaintiffs;

-- Show summary statistics
SELECT
    'Cases' AS table_name,
    COUNT(*) AS row_count,
    COUNT(DISTINCT rowID) AS unique_cases
FROM final_cases
UNION ALL
SELECT
    'Defendants' AS table_name,
    COUNT(*) AS row_count,
    COUNT(DISTINCT rowID) AS unique_cases
FROM final_defendants
UNION ALL
SELECT
    'Plaintiffs' AS table_name,
    COUNT(*) AS row_count,
    COUNT(DISTINCT rowID) AS unique_cases
FROM final_plaintiffs;
