-- Processing Procedure for CLUE Data (Simplified version)
-- This procedure processes raw source data into final tables

\c governance_catalog

SET search_path TO clue, public;

DROP PROCEDURE IF EXISTS clue.process_case_data(INTEGER);

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
    categorized_cases AS (
        SELECT
            t0.*,
            CASE
                WHEN t0.subtype IN ('PE, PD', 'PD, PE') THEN 'UNDETERMINED'
                WHEN t0.subtype = 'D' THEN 'DEBT'
                WHEN t0.subtype = 'E' THEN 'EVICTION'
                WHEN t0.subtype = 'PD' THEN 'POSSIBLE DEBT'
                WHEN t0.subtype = 'PE' THEN 'POSSIBLE EVICTION'
                WHEN t0.subtype = 'X' THEN 'OTHER CIVIL'
                WHEN t0.subtype IS NULL THEN 'NO INFORMATION'
                ELSE 'UNKNOWN'
            END AS clear_type
        FROM label_cases AS t0
    ),
    cleaned_data AS (
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
            clear_type
        FROM categorized_cases
    ),
    filtered_cases AS (
        SELECT *
        FROM cleaned_data
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
    FROM filtered_cases AS t0
    ON CONFLICT (row_id) DO NOTHING;

    GET DIAGNOSTICS v_rows_processed = ROW_COUNT;
    RAISE NOTICE 'Inserted % cases', v_rows_processed;

    -- Process defendants using simpler approach
    INSERT INTO clue.defendants (
        row_id, name, city, state, zip, address, address_ln1,
        address_ln2, party_num, role, org_name, removal_date
    )
    SELECT
        sd.id::BIGINT AS row_id,
        def_elem->>'Name' AS name,
        def_elem->>'City' AS city,
        def_elem->>'State' AS state,
        def_elem->>'Zip Code' AS zip,
        def_elem->>'Address' AS address,
        def_elem->>'Address1' AS address_ln1,
        def_elem->>'Amount' AS address_ln2,
        def_elem->>'Party No.' AS party_num,
        def_elem->>'Party Type' AS role,
        def_elem->>'Business or Organization Name' AS org_name,
        clue.safe_to_date(def_elem->>'Removal Date', 'MM/DD/YYYY') AS removal_date
    FROM clue.source_data sd
    CROSS JOIN LATERAL (
        SELECT
            CASE
                WHEN jsonb_typeof(clue.safe_json(sd.defendant)) = 'array'
                THEN jsonb_array_elements(clue.safe_json(sd.defendant))
                ELSE clue.safe_json(sd.defendant)
            END AS def_elem
        WHERE clue.safe_json(sd.defendant) IS NOT NULL
    ) AS def_expanded
    WHERE sd.load_id = ANY(v_load_ids)
    AND EXISTS (SELECT 1 FROM clue.cases c WHERE c.row_id = sd.id::BIGINT);

    GET DIAGNOSTICS v_rows_processed = ROW_COUNT;
    RAISE NOTICE 'Inserted % defendants', v_rows_processed;

    -- Process plaintiffs using simpler approach
    INSERT INTO clue.plaintiffs (
        row_id, name, city, state, zip, address, address_ln1,
        address_ln2, party_num, role, org_name, removal_date
    )
    SELECT
        sd.id::BIGINT AS row_id,
        plt_elem->>'Name' AS name,
        plt_elem->>'City' AS city,
        plt_elem->>'State' AS state,
        plt_elem->>'Zip Code' AS zip,
        plt_elem->>'Address' AS address,
        plt_elem->>'Address1' AS address_ln1,
        plt_elem->>'Amount' AS address_ln2,
        plt_elem->>'Party No.' AS party_num,
        plt_elem->>'Party Type' AS role,
        plt_elem->>'Business or Organization Name' AS org_name,
        clue.safe_to_date(plt_elem->>'Removal Date', 'MM/DD/YYYY') AS removal_date
    FROM clue.source_data sd
    CROSS JOIN LATERAL (
        SELECT
            CASE
                WHEN jsonb_typeof(clue.safe_json(sd.plaintiff)) = 'array'
                THEN jsonb_array_elements(clue.safe_json(sd.plaintiff))
                ELSE clue.safe_json(sd.plaintiff)
            END AS plt_elem
        WHERE clue.safe_json(sd.plaintiff) IS NOT NULL
    ) AS plt_expanded
    WHERE sd.load_id = ANY(v_load_ids)
    AND EXISTS (SELECT 1 FROM clue.cases c WHERE c.row_id = sd.id::BIGINT);

    GET DIAGNOSTICS v_rows_processed = ROW_COUNT;
    RAISE NOTICE 'Inserted % plaintiffs', v_rows_processed;

    RAISE NOTICE 'Processing complete for load IDs: %', v_load_ids;
END;
$$;

COMMENT ON PROCEDURE clue.process_case_data IS 'Process raw source data into cases, defendants, and plaintiffs tables';
