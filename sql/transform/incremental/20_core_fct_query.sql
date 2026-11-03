-- Rebuilds fct_query partitions for [start_date, end_date].
-- The team is resolved as of the query creation time (point-in-time join on SCD2 membership).

DELETE FROM iceberg.core.fct_query
WHERE query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}';

INSERT INTO iceberg.core.fct_query
SELECT
    q.query_id,
    q.query_date,
    q.user_name,
    coalesce(m.team_id, 'unassigned')                     AS team_id,
    coalesce(m.account_type, 'unknown')                   AS account_type,
    q.source,
    q.query_state,
    q.query_type,
    q.error_code,
    q.query_fingerprint,
    q.create_time,
    q.end_time,
    coalesce(q.cpu_time_ms, 0)                            AS cpu_time_ms,
    coalesce(q.wall_time_ms, 0)                           AS wall_time_ms,
    coalesce(q.queued_time_ms, 0)                         AS queued_time_ms,
    coalesce(q.peak_user_memory_bytes, 0)                 AS peak_user_memory_bytes,
    coalesce(q.cumulative_memory, 0)                      AS cumulative_memory,
    coalesce(q.physical_input_bytes, 0)                   AS physical_input_bytes,
    coalesce(q.output_bytes, 0)                           AS output_bytes,
    CAST(cardinality(array_distinct(transform(
        q.inputs, i -> i.catalog_name || '.' || i.schema_name || '.' || i.table_name
    ))) AS integer)                                       AS input_table_count,
    q.output_table_fqn
FROM iceberg.staging.query_completed AS q
LEFT JOIN iceberg.reference.user_team_membership AS m
    ON  m.user_name = q.user_name
    AND q.create_time >= m.valid_from
    AND (m.valid_to IS NULL OR q.create_time < m.valid_to)
WHERE q.query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}';
