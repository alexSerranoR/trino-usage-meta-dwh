-- Which tables drive the most compute, and who reads them?
SELECT
    u.table_fqn,
    d.owner_team_id,
    round(sum(u.cpu_time_ms_allocated) / 3.6E6, 2)                  AS cpu_hours_allocated,
    round(sum(u.physical_input_bytes) / power(1024, 4), 3)          AS tib_scanned,
    sum(u.query_count)                                              AS queries,
    cardinality(merge(CAST(u.users_hll AS HyperLogLog)))            AS distinct_users
FROM iceberg.marts.table_usage_daily AS u
JOIN iceberg.core.dim_table AS d ON d.table_fqn = u.table_fqn
WHERE u.usage_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
GROUP BY u.table_fqn, d.owner_team_id
ORDER BY cpu_hours_allocated DESC
LIMIT 10
