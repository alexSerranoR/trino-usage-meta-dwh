-- Rebuilds team_usage_daily partitions for [start_date, end_date].

DELETE FROM iceberg.marts.team_usage_daily
WHERE usage_date BETWEEN DATE '${start_date}' AND DATE '${end_date}';

INSERT INTO iceberg.marts.team_usage_daily
WITH tables_read AS (
    SELECT query_date, team_id, count(DISTINCT table_fqn) AS distinct_tables_read
    FROM iceberg.core.fct_query_table_access
    WHERE query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
    GROUP BY query_date, team_id
),
queries AS (
    SELECT
        query_date,
        team_id,
        count(*)                                  AS query_count,
        count_if(query_state = 'FAILED')          AS failed_query_count,
        count(DISTINCT user_name)                 AS distinct_users,
        CAST(approx_set(user_name) AS varbinary)  AS users_hll,
        sum(cpu_time_ms)                          AS cpu_time_ms,
        sum(wall_time_ms)                         AS wall_time_ms,
        sum(queued_time_ms)                       AS queued_time_ms,
        sum(cumulative_memory)                    AS cumulative_memory,
        max(peak_user_memory_bytes)               AS max_peak_memory_bytes,
        sum(physical_input_bytes)                 AS physical_input_bytes,
        sum(output_bytes)                         AS output_bytes
    FROM iceberg.core.fct_query
    WHERE query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
    GROUP BY query_date, team_id
)
SELECT
    q.query_date                              AS usage_date,
    q.team_id,
    q.query_count,
    q.failed_query_count,
    q.distinct_users,
    q.users_hll,
    coalesce(t.distinct_tables_read, 0)       AS distinct_tables_read,
    q.cpu_time_ms,
    q.wall_time_ms,
    q.queued_time_ms,
    q.cumulative_memory,
    q.max_peak_memory_bytes,
    q.physical_input_bytes,
    q.output_bytes
FROM queries AS q
LEFT JOIN tables_read AS t
    ON  t.query_date = q.query_date
    AND t.team_id = q.team_id;
