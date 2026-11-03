-- Rebuilds table_usage_daily partitions for [start_date, end_date].

DELETE FROM iceberg.marts.table_usage_daily
WHERE usage_date BETWEEN DATE '${start_date}' AND DATE '${end_date}';

INSERT INTO iceberg.marts.table_usage_daily
SELECT
    query_date                                        AS usage_date,
    table_fqn,
    count(*)                                          AS query_count,
    count_if(query_state = 'FAILED')                  AS failed_query_count,
    count(DISTINCT user_name)                         AS distinct_users,
    count(DISTINCT team_id)                           AS distinct_teams,
    CAST(approx_set(user_name) AS varbinary)          AS users_hll,
    sum(physical_input_bytes)                         AS physical_input_bytes,
    sum(cpu_time_ms_allocated)                        AS cpu_time_ms_allocated
FROM iceberg.core.fct_query_table_access
WHERE query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
GROUP BY query_date, table_fqn;
