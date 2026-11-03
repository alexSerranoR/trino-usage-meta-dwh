-- Rebuilt in full on every run. Includes registered tables that were never read
-- and tables that were read but are missing from the registry. Read dates come from
-- the daily mart, which has one row per table and day instead of one per access.

DELETE FROM iceberg.core.dim_table;

INSERT INTO iceberg.core.dim_table
WITH observed AS (
    SELECT
        table_fqn,
        min(usage_date) AS first_read_date,
        max(usage_date) AS last_read_date
    FROM iceberg.marts.table_usage_daily
    GROUP BY table_fqn
),
registry AS (
    SELECT
        lower(table_fqn) AS table_fqn,
        owner_team_id,
        domain,
        description,
        contains_pii
    FROM iceberg.reference.table_registry
),
merged AS (
    SELECT
        coalesce(r.table_fqn, o.table_fqn) AS table_fqn,
        r.owner_team_id,
        r.domain,
        r.description,
        r.contains_pii,
        r.table_fqn IS NOT NULL            AS is_registered,
        o.first_read_date,
        o.last_read_date
    FROM registry AS r
    FULL OUTER JOIN observed AS o
        ON o.table_fqn = r.table_fqn
)
SELECT
    table_fqn,
    split_part(table_fqn, '.', 1) AS catalog_name,
    split_part(table_fqn, '.', 2) AS schema_name,
    split_part(table_fqn, '.', 3) AS table_name,
    owner_team_id,
    domain,
    description,
    contains_pii,
    is_registered,
    first_read_date,
    last_read_date
FROM merged;
