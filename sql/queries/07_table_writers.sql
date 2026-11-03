-- Who writes table X, and what does producing it cost? Complements the read-side
-- usage: a table that is expensive to build and rarely read is a cleanup candidate.
SELECT
    f.output_table_fqn                                          AS table_fqn,
    f.team_id,
    f.user_name,
    count(*)                                                    AS writes,
    count_if(f.query_state = 'FAILED')                          AS failed_writes,
    round(sum(f.cpu_time_ms) / 3.6E6, 2)                        AS cpu_hours,
    round(sum(f.output_bytes) / power(1024, 3), 2)              AS gib_written,
    max(f.end_time)                                             AS last_write
FROM iceberg.core.fct_query AS f
WHERE f.output_table_fqn IS NOT NULL
  AND f.query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
GROUP BY f.output_table_fqn, f.team_id, f.user_name
ORDER BY cpu_hours DESC
LIMIT 10
