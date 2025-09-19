INSERT INTO governance_role (name, description, responsibilities, authority_level)
VALUES
  ('Data Owner',
   'Executive accountable for data assets within a program or mission area.',
   'Approve permissible uses, allocate resources for data quality remediation, and escalate policy exceptions.',
   'executive'),
  ('Data Steward',
   'Operational lead responsible for data quality, metadata completeness, and issue triage.',
   'Maintain business metadata, coordinate data quality assessments, and ensure policy adherence.',
   'manager'),
  ('Privacy Officer',
   'Agency official who oversees privacy impact assessments and privacy risk mitigations.',
   'Review data collections for privacy compliance, manage consent artifacts, and respond to incidents.',
   'executive'),
  ('Security Officer',
   'Cybersecurity authority responsible for system authorization and security controls.',
   'Validate security markings, monitor audit logs, and certify system security posture.',
   'executive'),
  ('Data Governance Council Chair',
   'Chairperson for the agency-wide Data Governance Council.',
   'Set meeting agendas, ratify policy updates, and broker decisions across stakeholder groups.',
   'executive')
ON CONFLICT ((LOWER(name))) DO NOTHING;

INSERT INTO policy (name, policy_type, authority, summary, effective_date, review_cycle_days)
VALUES
  ('Privacy Act of 1974', 'privacy', '5 U.S.C. § 552a', 'Governs the collection, maintenance, use, and dissemination of personally identifiable information by federal agencies.', '1974-09-27', 365),
  ('CIPSEA Confidentiality', 'privacy', '44 U.S.C. § 3501', 'Requires confidentiality protections for statistical data and restricts re-identification.', '2002-12-17', 365),
  ('OMB M-19-23 FDS Strategy', 'governance', 'OMB Memorandum M-19-23', 'Establishes Federal Data Strategy implementation practices and governance guidelines.', '2019-06-04', 365),
  ('FedRAMP High Baseline', 'cybersecurity', 'FedRAMP', 'Defines security controls and assessment procedures for high-impact cloud systems.', '2017-06-01', 365),
  ('Records Retention Schedule', 'retention', 'Agency Records Schedule', 'Specifies retention and disposition requirements for administrative records.', '2021-01-01', 365)
ON CONFLICT ((LOWER(name))) DO NOTHING;

INSERT INTO security_marking (name, description, classification_level)
VALUES
  ('Public', 'Unrestricted public data.', 'low'),
  ('Controlled Unclassified Information', 'CUI requiring safeguarding or dissemination controls.', 'moderate'),
  ('High-Value Asset', 'Information systems designated as high value assets requiring heightened security.', 'high')
ON CONFLICT ((LOWER(name))) DO NOTHING;

INSERT INTO permissible_use (policy_id, use_case, description, requires_approval, approval_authority)
SELECT p.policy_id, data.use_case, data.description, data.requires_approval, data.approval_authority
FROM policy p
JOIN (
  VALUES
    ('Privacy Act of 1974', 'Program service delivery', 'Use PII to determine eligibility and deliver services for the originating program.', true, 'Data Owner'),
    ('Privacy Act of 1974', 'Statistical analysis with de-identification', 'Use de-identified records for internal statistical reporting when privacy risk mitigations are applied.', true, 'Privacy Officer'),
    ('CIPSEA Confidentiality', 'Statistical release', 'Publish aggregate statistical data that meets disclosure avoidance standards.', true, 'Data Governance Council Chair'),
    ('OMB M-19-23 FDS Strategy', 'Data inventory publication', 'Publish dataset metadata to the public enterprise data inventory.', false, 'Data Steward'),
    ('FedRAMP High Baseline', 'Operational monitoring', 'Use security log data for continuous monitoring and incident response.', false, 'Security Officer')
) AS data(policy_name, use_case, description, requires_approval, approval_authority)
  ON p.name = data.policy_name
ON CONFLICT DO NOTHING;
