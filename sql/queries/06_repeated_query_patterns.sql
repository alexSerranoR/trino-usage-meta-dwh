-- Recurring query patterns ranked by total compute. Patterns that many users or
-- dashboards run over and over are candidates for a reusable, pre-computed data layer.
WITH patterns AS (
    SELECT
        query_fingerprint,
        count(*)                                        AS executions,
        count(DISTINCT user_name)                       AS distinct_users,
        array_sort(array_agg(DISTINCT team_id))         AS teams,
        sum(cpu_time_ms) / 3.6E6                        AS cpu_hours,
        sum(physical_input_bytes) / power(1024, 4)      AS tib_scanned,
        approx_percentile(wall_time_ms, 0.5) / 1.0E3    AS median_wall_seconds
    FROM iceberg.core.fct_query
    WHERE query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
      AND query_type = 'SELECT'
      AND input_table_count > 0
    GROUP BY query_fingerprint
    HAVING count(*) >= 20
),
pattern_tables AS (
    SELECT
        f.query_fingerprint,
        array_sort(array_agg(DISTINCT a.table_fqn))     AS tables_read
    FROM iceberg.core.fct_query AS f
    JOIN iceberg.core.fct_query_table_access AS a
        ON a.query_id = f.query_id AND a.query_date = f.query_date
    WHERE f.query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
      AND a.query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
    GROUP BY f.query_fingerprint
)
SELECT
    p.query_fingerprint,
    p.executions,
    p.distinct_users,
    p.teams,
    round(p.cpu_hours, 2)                               AS cpu_hours,
    round(p.tib_scanned, 3)                             AS tib_scanned,
    round(p.median_wall_seconds, 1)                     AS median_wall_seconds,
    t.tables_read
FROM patterns AS p
JOIN pattern_tables AS t ON t.query_fingerprint = p.query_fingerprint
ORDER BY p.cpu_hours DESC
LIMIT 10
