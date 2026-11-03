-- Catalog health from the usage profile: unused tables, unregistered tables
-- and tables without a description that people or agents rely on.
SELECT
    table_fqn,
    usage_status,
    owner_team_id,
    queries_30d,
    distinct_users_30d,
    CASE
        WHEN owner_team_id IS NULL AND queries_30d > 0      THEN 'unregistered table in use'
        WHEN owner_team_id IS NULL                          THEN 'unregistered and unused'
        WHEN usage_status IN ('never_used', 'dormant')      THEN 'candidate for deprecation'
        WHEN coalesce(description, '') = ''                 THEN 'missing description'
    END                                                     AS finding
FROM iceberg.marts.table_usage_profile
WHERE owner_team_id IS NULL
   OR usage_status IN ('never_used', 'dormant')
   OR coalesce(description, '') = ''
ORDER BY queries_30d DESC
