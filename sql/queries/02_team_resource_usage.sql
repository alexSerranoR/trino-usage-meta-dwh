-- How many resources did team Y consume during a period?
SELECT
    t.team_id,
    t.team_name,
    sum(u.query_count)                                              AS queries,
    sum(u.failed_query_count)                                       AS failed_queries,
    cardinality(merge(CAST(u.users_hll AS HyperLogLog)))            AS distinct_users,
    round(sum(u.cpu_time_ms) / 3.6E6, 2)                            AS cpu_hours,
    round(sum(u.wall_time_ms) / 3.6E6, 2)                           AS wall_hours,
    round(sum(u.queued_time_ms) / 3.6E6, 2)                         AS queued_hours,
    round(sum(u.physical_input_bytes) / power(1024, 4), 3)          AS tib_scanned,
    round(sum(u.cumulative_memory) / power(1024, 3) / 3.6E6, 2)     AS memory_gib_hours,
    round(max(u.max_peak_memory_bytes) / power(1024, 3), 2)         AS max_peak_memory_gib
FROM iceberg.marts.team_usage_daily AS u
JOIN iceberg.core.dim_team AS t ON t.team_id = u.team_id
WHERE u.team_id = '${team_id}'
  AND u.usage_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
GROUP BY t.team_id, t.team_name
