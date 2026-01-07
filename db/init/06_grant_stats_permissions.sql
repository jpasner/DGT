-- Grant pg_read_all_stats permission for usage and lineage extraction
-- This is required for OpenMetadata to access pg_stat_statements table
-- for extracting usage and lineage information

GRANT pg_read_all_stats TO metadata_admin;

-- Also grant to openmetadata_user if needed
GRANT pg_read_all_stats TO openmetadata_user;
