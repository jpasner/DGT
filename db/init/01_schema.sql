CREATE TABLE IF NOT EXISTS agency (
    agency_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    acronym TEXT,
    description TEXT,
    contact_email TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS agency_name_uniq ON agency (LOWER(name));

CREATE TABLE IF NOT EXISTS dataset (
    dataset_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title TEXT NOT NULL,
    description TEXT,
    source_identifier TEXT NOT NULL,
    agency_id UUID REFERENCES agency(agency_id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    landing_page TEXT,
    license TEXT,
    public_access_level TEXT,
    is_sensitive BOOLEAN NOT NULL DEFAULT false,
    source_modified TIMESTAMPTZ,
    harvest_timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_identifier)
);

CREATE INDEX IF NOT EXISTS dataset_agency_idx ON dataset (agency_id);

CREATE TABLE IF NOT EXISTS distribution (
    distribution_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID NOT NULL REFERENCES dataset(dataset_id) ON DELETE CASCADE,
    title TEXT,
    description TEXT,
    download_url TEXT,
    media_type TEXT,
    format TEXT,
    size_bytes BIGINT,
    conforms_to TEXT,
    checksum TEXT,
    temporal_start DATE,
    temporal_end DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS distribution_dataset_idx ON distribution (dataset_id);

CREATE TABLE IF NOT EXISTS tag (
    tag_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    description TEXT,
    tag_type TEXT NOT NULL DEFAULT 'keyword'
);

CREATE UNIQUE INDEX IF NOT EXISTS tag_name_type_uniq ON tag (LOWER(name), tag_type);

CREATE TABLE IF NOT EXISTS dataset_tag (
    dataset_id UUID NOT NULL REFERENCES dataset(dataset_id) ON DELETE CASCADE,
    tag_id UUID NOT NULL REFERENCES tag(tag_id) ON DELETE CASCADE,
    tag_source TEXT NOT NULL DEFAULT 'data_gov',
    assigned_on TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset_id, tag_id, tag_source)
);

CREATE TABLE IF NOT EXISTS policy (
    policy_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    policy_type TEXT NOT NULL,
    authority TEXT,
    summary TEXT,
    effective_date DATE,
    review_cycle_days INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS policy_name_uniq ON policy (LOWER(name));

CREATE TABLE IF NOT EXISTS policy_clause (
    policy_clause_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_id UUID NOT NULL REFERENCES policy(policy_id) ON DELETE CASCADE,
    clause_number TEXT,
    clause_text TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dataset_policy (
    dataset_policy_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID NOT NULL REFERENCES dataset(dataset_id) ON DELETE CASCADE,
    policy_id UUID NOT NULL REFERENCES policy(policy_id) ON DELETE CASCADE,
    applied_by TEXT,
    applied_on TIMESTAMPTZ NOT NULL DEFAULT now(),
    justification TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS dataset_policy_unique ON dataset_policy (dataset_id, policy_id);

CREATE TABLE IF NOT EXISTS permissible_use (
    permissible_use_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    policy_id UUID NOT NULL REFERENCES policy(policy_id) ON DELETE CASCADE,
    use_case TEXT NOT NULL,
    description TEXT,
    requires_approval BOOLEAN NOT NULL DEFAULT false,
    approval_authority TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS permissible_use_policy_case_uniq ON permissible_use (policy_id, LOWER(use_case));

CREATE TABLE IF NOT EXISTS permissible_use_condition (
    permissible_use_condition_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    permissible_use_id UUID NOT NULL REFERENCES permissible_use(permissible_use_id) ON DELETE CASCADE,
    dataset_id UUID REFERENCES dataset(dataset_id) ON DELETE CASCADE,
    condition_text TEXT NOT NULL,
    condition_type TEXT NOT NULL,
    condition_value TEXT,
    enforced_by TEXT
);

CREATE TABLE IF NOT EXISTS security_marking (
    security_marking_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    description TEXT,
    classification_level TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS security_marking_name_uniq ON security_marking (LOWER(name));

CREATE TABLE IF NOT EXISTS dataset_security_marking (
    dataset_security_marking_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID NOT NULL REFERENCES dataset(dataset_id) ON DELETE CASCADE,
    security_marking_id UUID NOT NULL REFERENCES security_marking(security_marking_id) ON DELETE CASCADE,
    assigned_on TIMESTAMPTZ NOT NULL DEFAULT now(),
    assigned_by TEXT,
    rationale TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS dataset_security_unique ON dataset_security_marking (dataset_id, security_marking_id);

CREATE TABLE IF NOT EXISTS governance_role (
    governance_role_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    description TEXT,
    responsibilities TEXT,
    authority_level TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS governance_role_name_uniq ON governance_role (LOWER(name));

CREATE TABLE IF NOT EXISTS person (
    person_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    full_name TEXT NOT NULL,
    email TEXT,
    organization TEXT,
    notes TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS person_email_uniq ON person (LOWER(email)) WHERE email IS NOT NULL;

CREATE TABLE IF NOT EXISTS role_assignment (
    role_assignment_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    governance_role_id UUID NOT NULL REFERENCES governance_role(governance_role_id) ON DELETE CASCADE,
    person_id UUID NOT NULL REFERENCES person(person_id) ON DELETE CASCADE,
    dataset_id UUID REFERENCES dataset(dataset_id) ON DELETE CASCADE,
    scope TEXT NOT NULL DEFAULT 'enterprise',
    start_date DATE NOT NULL DEFAULT CURRENT_DATE,
    end_date DATE,
    assigned_by TEXT,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS role_assignment_role_idx ON role_assignment (governance_role_id);
CREATE INDEX IF NOT EXISTS role_assignment_dataset_idx ON role_assignment (dataset_id);

CREATE TABLE IF NOT EXISTS decision_log (
    decision_log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID REFERENCES dataset(dataset_id) ON DELETE SET NULL,
    decision_type TEXT NOT NULL,
    decision_text TEXT NOT NULL,
    decided_by TEXT,
    decided_on TIMESTAMPTZ NOT NULL DEFAULT now(),
    related_request UUID
);

CREATE TABLE IF NOT EXISTS data_access_request (
    request_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_id UUID NOT NULL REFERENCES dataset(dataset_id) ON DELETE CASCADE,
    requested_by TEXT NOT NULL,
    requested_on TIMESTAMPTZ NOT NULL DEFAULT now(),
    purpose TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    decision_log_id UUID REFERENCES decision_log(decision_log_id) ON DELETE SET NULL
);

ALTER TABLE decision_log
    ADD CONSTRAINT decision_log_related_request_fk
    FOREIGN KEY (related_request)
    REFERENCES data_access_request(request_id)
    ON DELETE SET NULL;

CREATE TABLE IF NOT EXISTS dataset_lineage (
    dataset_lineage_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    upstream_dataset_id UUID REFERENCES dataset(dataset_id) ON DELETE CASCADE,
    downstream_dataset_id UUID REFERENCES dataset(dataset_id) ON DELETE CASCADE,
    lineage_type TEXT NOT NULL,
    transformation_summary TEXT,
    recorded_on TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS dataset_lineage_upstream_idx ON dataset_lineage (upstream_dataset_id);
CREATE INDEX IF NOT EXISTS dataset_lineage_downstream_idx ON dataset_lineage (downstream_dataset_id);

CREATE TABLE IF NOT EXISTS glossary_term (
    glossary_term_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    description TEXT,
    stewardship_role_id UUID REFERENCES governance_role(governance_role_id) ON DELETE SET NULL,
    glossary_category TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS glossary_term_name_uniq ON glossary_term (LOWER(name));
