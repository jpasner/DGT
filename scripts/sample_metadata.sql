-- Sample governance metadata for quick validation
WITH agency_upsert AS (
    INSERT INTO agency (name, acronym, description, contact_email)
    VALUES (
        'Department of Transportation',
        'DOT',
        'Leads the federal transportation system including safety oversight.',
        'cio@dot.gov'
    )
    ON CONFLICT (LOWER(name)) DO UPDATE
        SET acronym = EXCLUDED.acronym,
            description = EXCLUDED.description,
            contact_email = EXCLUDED.contact_email,
            updated_at = now()
    RETURNING agency_id
)
INSERT INTO dataset (
    title,
    description,
    source_identifier,
    agency_id,
    landing_page,
    license,
    public_access_level,
    is_sensitive,
    source_modified
)
SELECT
    'Transit Ridership Monthly Summary',
    'Summarized ridership metrics reported monthly by regional transit agencies.',
    'dot-transit-ridership-2024',
    agency_id,
    'https://data.transportation.gov/transit/ridership-2024',
    'Creative Commons CC0',
    'public',
    false,
    DATE '2024-06-30'
FROM agency_upsert
ON CONFLICT (source_identifier) DO UPDATE
    SET title = EXCLUDED.title,
        description = EXCLUDED.description,
        agency_id = EXCLUDED.agency_id,
        landing_page = EXCLUDED.landing_page,
        license = EXCLUDED.license,
        public_access_level = EXCLUDED.public_access_level,
        updated_at = now();

WITH dataset_row AS (
    SELECT dataset_id FROM dataset WHERE source_identifier = 'dot-transit-ridership-2024'
)
INSERT INTO distribution (
    dataset_id,
    title,
    description,
    download_url,
    media_type,
    format,
    size_bytes,
    temporal_start,
    temporal_end
)
SELECT
    dataset_id,
    'RIDERSHIP_SUMMARY_2024_06.csv',
    'Monthly totals and percent change for each region.',
    'https://data.transportation.gov/api/views/abcd-1234/rows.csv',
    'text/csv',
    'csv',
    245760,
    DATE '2024-01-01',
    DATE '2024-06-30'
FROM dataset_row
WHERE NOT EXISTS (
    SELECT 1 FROM distribution
    WHERE distribution.dataset_id = dataset_row.dataset_id
      AND distribution.download_url = 'https://data.transportation.gov/api/views/abcd-1234/rows.csv'
);

INSERT INTO tag (name, description, tag_type)
VALUES
    ('transit', 'Public transit operations and planning', 'keyword'),
    ('ridership', 'Counts of riders served', 'keyword'),
    ('transportation', 'Transportation-related dataset', 'theme')
ON CONFLICT (LOWER(name), tag_type) DO NOTHING;

WITH dataset_row AS (
    SELECT dataset_id FROM dataset WHERE source_identifier = 'dot-transit-ridership-2024'
),
selected_tags AS (
    SELECT tag_id, name FROM tag WHERE name IN ('transit', 'ridership', 'transportation')
)
INSERT INTO dataset_tag (dataset_id, tag_id, tag_source)
SELECT dataset_id, tag_id, 'governance'
FROM dataset_row CROSS JOIN selected_tags
ON CONFLICT (dataset_id, tag_id, tag_source) DO NOTHING;

INSERT INTO dataset_policy (dataset_id, policy_id, applied_by, justification)
SELECT
    d.dataset_id,
    p.policy_id,
    'Data Steward',
    'Dataset includes aggregated counts only; Privacy Act still governs release.'
FROM dataset d
JOIN policy p ON p.name = 'Privacy Act of 1974'
WHERE d.source_identifier = 'dot-transit-ridership-2024'
ON CONFLICT (dataset_id, policy_id) DO NOTHING;

INSERT INTO permissible_use_condition (
    permissible_use_id,
    dataset_id,
    condition_text,
    condition_type,
    condition_value,
    enforced_by
)
SELECT
    pu.permissible_use_id,
    d.dataset_id,
    'Limit disclosure to aggregated counts at regional level; no PII present.',
    'aggregation_level',
    'regional',
    'Privacy Officer'
FROM permissible_use pu
JOIN policy pol ON pol.policy_id = pu.policy_id AND pol.name = 'Privacy Act of 1974'
JOIN dataset d ON d.source_identifier = 'dot-transit-ridership-2024'
WHERE pu.use_case = 'Program service delivery'
  AND NOT EXISTS (
        SELECT 1 FROM permissible_use_condition existing
        WHERE existing.permissible_use_id = pu.permissible_use_id
          AND existing.dataset_id = d.dataset_id
          AND existing.condition_type = 'aggregation_level'
          AND existing.condition_value = 'regional'
    );

WITH updated AS (
    UPDATE person
    SET full_name = 'Jordan Rivera',
        organization = 'Department of Transportation'
    WHERE LOWER(email) = 'jordan.rivera@dot.gov'
    RETURNING person_id
)
INSERT INTO person (full_name, email, organization)
SELECT 'Jordan Rivera', 'jordan.rivera@dot.gov', 'Department of Transportation'
WHERE NOT EXISTS (SELECT 1 FROM updated);

INSERT INTO role_assignment (
    governance_role_id,
    person_id,
    dataset_id,
    scope,
    start_date,
    assigned_by,
    notes
)
SELECT
    gr.governance_role_id,
    pr.person_id,
    d.dataset_id,
    'dataset',
    CURRENT_DATE,
    'Chief Data Officer',
    'Responsible for monthly quality checks.'
FROM governance_role gr
JOIN person pr ON LOWER(pr.email) = 'jordan.rivera@dot.gov'
JOIN dataset d ON d.source_identifier = 'dot-transit-ridership-2024'
WHERE gr.name = 'Data Steward'
  AND NOT EXISTS (
        SELECT 1 FROM role_assignment existing
        WHERE existing.governance_role_id = gr.governance_role_id
          AND existing.person_id = pr.person_id
          AND existing.dataset_id = d.dataset_id
    );

INSERT INTO dataset_security_marking (dataset_id, security_marking_id, assigned_by, rationale)
SELECT
    d.dataset_id,
    sm.security_marking_id,
    'Security Officer',
    'Aggregated public information; minimal risk.'
FROM dataset d
JOIN security_marking sm ON sm.name = 'Public'
WHERE d.source_identifier = 'dot-transit-ridership-2024'
ON CONFLICT (dataset_id, security_marking_id) DO NOTHING;
