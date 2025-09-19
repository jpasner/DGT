CREATE OR REPLACE VIEW vw_catalog_dataset AS
SELECT
    d.dataset_id,
    d.title,
    d.description,
    d.source_identifier,
    a.name              AS agency_name,
    a.acronym           AS agency_acronym,
    d.landing_page,
    d.license,
    d.public_access_level,
    d.is_sensitive,
    d.source_modified,
    d.harvest_timestamp,
    ARRAY_REMOVE(ARRAY_AGG(DISTINCT t.name) FILTER (WHERE t.name IS NOT NULL), NULL) AS tags,
    ARRAY_REMOVE(ARRAY_AGG(DISTINCT sm.name) FILTER (WHERE sm.name IS NOT NULL), NULL) AS security_markings,
    ARRAY_REMOVE(ARRAY_AGG(DISTINCT p.name) FILTER (WHERE p.name IS NOT NULL), NULL) AS policies
FROM dataset d
LEFT JOIN agency a ON a.agency_id = d.agency_id
LEFT JOIN dataset_tag dt ON dt.dataset_id = d.dataset_id
LEFT JOIN tag t ON t.tag_id = dt.tag_id
LEFT JOIN dataset_security_marking dsm ON dsm.dataset_id = d.dataset_id
LEFT JOIN security_marking sm ON sm.security_marking_id = dsm.security_marking_id
LEFT JOIN dataset_policy dp ON dp.dataset_id = d.dataset_id
LEFT JOIN policy p ON p.policy_id = dp.policy_id
GROUP BY d.dataset_id, a.name, a.acronym;

CREATE OR REPLACE VIEW vw_permissible_use AS
SELECT
    pu.permissible_use_id,
    pu.policy_id,
    pol.name AS policy_name,
    pu.use_case,
    pu.description,
    pu.requires_approval,
    pu.approval_authority,
    ARRAY_REMOVE(ARRAY_AGG(jsonb_build_object(
        'dataset_id', puc.dataset_id,
        'condition_type', puc.condition_type,
        'condition_value', puc.condition_value,
        'condition_text', puc.condition_text,
        'enforced_by', puc.enforced_by
    )) FILTER (WHERE puc.permissible_use_condition_id IS NOT NULL), NULL) AS conditions
FROM permissible_use pu
LEFT JOIN permissible_use_condition puc ON puc.permissible_use_id = pu.permissible_use_id
LEFT JOIN policy pol ON pol.policy_id = pu.policy_id
GROUP BY pu.permissible_use_id, pu.policy_id, pol.name;

CREATE OR REPLACE VIEW vw_governance_assignments AS
SELECT
    ra.role_assignment_id,
    gr.name AS role_name,
    gr.description AS role_description,
    gr.responsibilities,
    ra.scope,
    ra.start_date,
    ra.end_date,
    ra.assigned_by,
    ra.notes,
    p.full_name,
    p.email,
    p.organization,
    d.dataset_id,
    d.title AS dataset_title
FROM role_assignment ra
JOIN governance_role gr ON gr.governance_role_id = ra.governance_role_id
JOIN person p ON p.person_id = ra.person_id
LEFT JOIN dataset d ON d.dataset_id = ra.dataset_id;

CREATE OR REPLACE VIEW vw_data_access_request AS
SELECT
    dar.request_id,
    d.dataset_id,
    d.title AS dataset_title,
    dar.requested_by,
    dar.requested_on,
    dar.purpose,
    dar.status,
    dl.decision_type,
    dl.decision_text,
    dl.decided_by,
    dl.decided_on
FROM data_access_request dar
LEFT JOIN dataset d ON d.dataset_id = dar.dataset_id
LEFT JOIN decision_log dl ON dl.decision_log_id = dar.decision_log_id;
