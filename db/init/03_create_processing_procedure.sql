-- Processing Procedure for CLUE Data
-- This procedure processes raw source data into final tables

\c governance_catalog

SET search_path TO clue, public;

-- Procedure to process a specific load_id
CREATE OR REPLACE PROCEDURE clue.process_case_data(p_load_id INTEGER DEFAULT NULL)
LANGUAGE plpgsql AS $$
DECLARE
    v_rows_processed INTEGER := 0;
    v_load_ids INTEGER[];
BEGIN
    -- If no load_id specified, process all unprocessed loads
    IF p_load_id IS NULL THEN
        SELECT ARRAY_AGG(load_id) INTO v_load_ids
        FROM clue.file_load_metadata
        WHERE load_status = 'completed'
        AND NOT EXISTS (
            SELECT 1 FROM clue.cases c
            INNER JOIN clue.source_data s ON c.source_id = s.source_id
            WHERE s.load_id = file_load_metadata.load_id
        );
    ELSE
        v_load_ids := ARRAY[p_load_id];
    END IF;

    RAISE NOTICE 'Processing load IDs: %', v_load_ids;

    -- Process the data using a CTE-based transformation
    WITH
    label_cases AS (
        SELECT
            t0.source_id,
            t0.id::BIGINT AS id,
            t0.case_number,
            t0.caption,
            t0.name,
            t0.dob,
            t0.party_type,
            t0.court,
            t0.court_id,
            t0.case_type,
            t0.filing_date,
            t0.status,
            t0.court_system,
            t0.checksum,
            t0.misc,
            t0.last_scrape,
            t0.not_found_at,
            t0.created_at,
            t0.updated_at,
            t0.tracking_number,
            t0.case_type_code,
            t0.location,
            t0.defendant,
            t0.plaintiff,
            t0.error,
            t0.versions,
            t0.parser,
            t0.cjis,
            t0.sentence_length,
            t1.subtype
        FROM clue.source_data AS t0
        LEFT JOIN clue.case_types t1 ON t0.case_type = t1.case_type
        WHERE t0.load_id = ANY(v_load_ids)
    ),
    limit_by_subtype AS (
        SELECT
            t0.*,
            CASE
                WHEN t0.subtype IN ('PE, PD', 'PD, PE') THEN 'UNDETERMINED'
                WHEN t0.subtype = 'D' THEN 'DEBT'
                WHEN t0.subtype = 'E' THEN 'EVICTION'
                WHEN t0.subtype = 'PD' THEN 'POSSIBLE DEBT'
                WHEN t0.subtype = 'PE' THEN 'POSSIBLE EVICTION'
                WHEN t0.subtype IS NULL THEN 'NO INFORMATION'
                ELSE NULL
            END AS clear_type
        FROM label_cases AS t0
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
            source_id,
            id,
            clue.nullif_na(case_number) AS case_number,
            clue.nullif_na(caption) AS caption,
            clue.nullif_na(name) AS name,
            clue.nullif_na(dob) AS dob,
            clue.nullif_na(party_type) AS party_type,
            clue.nullif_na(court) AS court,
            clue.nullif_na(case_type) AS case_type,
            clue.nullif_na(filing_date) AS filing_date,
            clue.nullif_na(status) AS status,
            clue.nullif_na(court_system) AS court_system,
            created_at,
            updated_at,
            clue.nullif_na(location) AS location,
            clue.nullif_na(defendant) AS defendant,
            clue.nullif_na(plaintiff) AS plaintiff,
            clue.nullif_na(versions) AS versions,
            clue.nullif_na(cjis) AS cjis,
            clue.nullif_na(clear_type) AS clear_type
        FROM limit_by_subtype
    ),
    drop_extras AS (
        SELECT *
        FROM real_nulls
        WHERE
            (UPPER(court_system) NOT LIKE '%CRIMINAL%' OR court_system IS NULL) AND
            (UPPER(court_system) NOT LIKE '%FAMILY%' OR court_system IS NULL) AND
            (UPPER(court_system) NOT LIKE '%TRAFFIC%' OR court_system IS NULL) AND
            (UPPER(court_system) NOT LIKE '%PROBATE%' OR court_system IS NULL) AND
            cjis IS NULL
    )
    -- Insert into cases table
    INSERT INTO clue.cases (
        row_id, source_id, case_number, caption, name, birth_date, role,
        court, case_type, file_date, case_status, court_system,
        create_date, update_date, court_location, versions, category
    )
    SELECT DISTINCT
        t0.id AS row_id,
        t0.source_id,
        t0.case_number,
        t0.caption,
        t0.name,
        t0.dob AS birth_date,
        t0.party_type AS role,
        t0.court,
        t0.case_type,
        clue.safe_to_date(t0.filing_date, 'YYYY-MM-DD') AS file_date,
        t0.status AS case_status,
        t0.court_system,
        t0.created_at AS create_date,
        t0.updated_at AS update_date,
        t0.location AS court_location,
        t0.versions,
        t0.clear_type AS category
    FROM drop_extras AS t0
    ON CONFLICT (row_id) DO NOTHING;

    GET DIAGNOSTICS v_rows_processed = ROW_COUNT;
    RAISE NOTICE 'Inserted % cases', v_rows_processed;

    -- Process defendants
    WITH
    defendant_json AS (
        SELECT
            sd.id::BIGINT AS row_id,
            clue.safe_json(sd.defendant) AS defendant
        FROM clue.source_data sd
        WHERE sd.load_id = ANY(v_load_ids)
        AND sd.defendant IS NOT NULL
        AND sd.defendant != 'NA'
    ),
    defendant_array AS (
        SELECT
            row_id,
            defendant_elem AS defendant
        FROM defendant_json
        CROSS JOIN LATERAL (
            SELECT
                CASE
                    WHEN jsonb_typeof(defendant) = 'array' THEN elem
                    ELSE defendant
                END AS defendant_elem
            FROM (
                SELECT COALESCE(jsonb_array_elements(CASE WHEN jsonb_typeof(defendant) = 'array' THEN defendant ELSE NULL END), defendant) AS elem
            ) sub
        ) AS expanded
        WHERE defendant IS NOT NULL
    )
    INSERT INTO clue.defendants (
        row_id, name, city, state, zip, address, address_ln1,
        address_ln2, party_num, role, org_name, removal_date
    )
    SELECT
        row_id,
        defendant->>'Name' AS name,
        defendant->>'City' AS city,
        defendant->>'State' AS state,
        defendant->>'Zip Code' AS zip,
        defendant->>'Address' AS address,
        defendant->>'Address1' AS address_ln1,
        defendant->>'Amount' AS address_ln2,
        defendant->>'Party No.' AS party_num,
        defendant->>'Party Type' AS role,
        defendant->>'Business or Organization Name' AS org_name,
        clue.safe_to_date(defendant->>'Removal Date', 'MM/DD/YYYY') AS removal_date
    FROM defendant_array
    WHERE EXISTS (SELECT 1 FROM clue.cases c WHERE c.row_id = defendant_array.row_id);

    GET DIAGNOSTICS v_rows_processed = ROW_COUNT;
    RAISE NOTICE 'Inserted % defendants', v_rows_processed;

    -- Process plaintiffs
    WITH
    plaintiff_json AS (
        SELECT
            sd.id::BIGINT AS row_id,
            clue.safe_json(sd.plaintiff) AS plaintiff
        FROM clue.source_data sd
        WHERE sd.load_id = ANY(v_load_ids)
        AND sd.plaintiff IS NOT NULL
        AND sd.plaintiff != 'NA'
    ),
    plaintiff_array AS (
        SELECT
            row_id,
            plaintiff_elem AS plaintiff
        FROM plaintiff_json
        CROSS JOIN LATERAL (
            SELECT
                CASE
                    WHEN jsonb_typeof(plaintiff) = 'array' THEN elem
                    ELSE plaintiff
                END AS plaintiff_elem
            FROM (
                SELECT COALESCE(jsonb_array_elements(CASE WHEN jsonb_typeof(plaintiff) = 'array' THEN plaintiff ELSE NULL END), plaintiff) AS elem
            ) sub
        ) AS expanded
        WHERE plaintiff IS NOT NULL
    )
    INSERT INTO clue.plaintiffs (
        row_id, name, city, state, zip, address, address_ln1,
        address_ln2, party_num, role, org_name, removal_date
    )
    SELECT
        row_id,
        plaintiff->>'Name' AS name,
        plaintiff->>'City' AS city,
        plaintiff->>'State' AS state,
        plaintiff->>'Zip Code' AS zip,
        plaintiff->>'Address' AS address,
        plaintiff->>'Address1' AS address_ln1,
        plaintiff->>'Amount' AS address_ln2,
        plaintiff->>'Party No.' AS party_num,
        plaintiff->>'Party Type' AS role,
        plaintiff->>'Business or Organization Name' AS org_name,
        clue.safe_to_date(plaintiff->>'Removal Date', 'MM/DD/YYYY') AS removal_date
    FROM plaintiff_array
    WHERE EXISTS (SELECT 1 FROM clue.cases c WHERE c.row_id = plaintiff_array.row_id);

    GET DIAGNOSTICS v_rows_processed = ROW_COUNT;
    RAISE NOTICE 'Inserted % plaintiffs', v_rows_processed;

    RAISE NOTICE 'Processing complete for load IDs: %', v_load_ids;
END;
$$;

COMMENT ON PROCEDURE clue.process_case_data IS 'Process raw source data into cases, defendants, and plaintiffs tables';
