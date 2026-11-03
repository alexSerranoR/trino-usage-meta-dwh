-- Rebuilds staging partitions for [start_date, end_date].
-- Deduplicates redelivered events and normalizes user and table identifiers.

DELETE FROM iceberg.staging.query_completed
WHERE query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}';

INSERT INTO iceberg.staging.query_completed
WITH window_events AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY query_id ORDER BY ingested_at DESC, end_time DESC) AS rn
    FROM iceberg.raw.trino_query_completed
    WHERE create_time >= TIMESTAMP '${start_date} 00:00:00 UTC'
      AND create_time <  TIMESTAMP '${end_date} 00:00:00 UTC' + INTERVAL '1' DAY
)
SELECT
    query_id,
    CAST(create_time AT TIME ZONE 'UTC' AS date)                       AS query_date,
    upper(query_state)                                                 AS query_state,
    upper(query_type)                                                  AS query_type,
    error_code,
    -- Literals are replaced so that executions of the same statement with different
    -- parameters share one fingerprint.
    to_hex(xxhash64(to_utf8(
        regexp_replace(
            regexp_replace(
                regexp_replace(lower(trim(query)), '''[^'']*''', '?'),
                '\b\d+(\.\d+)?\b', '?'),
            '\s+', ' ')
    )))                                                                AS query_fingerprint,
    lower(trim(user_name))                                             AS user_name,
    source,
    create_time,
    execution_start_time,
    end_time,
    cpu_time_ms,
    wall_time_ms,
    queued_time_ms,
    peak_user_memory_bytes,
    cumulative_memory,
    physical_input_bytes,
    physical_input_rows,
    output_bytes,
    output_rows,
    transform(
        coalesce(inputs, ARRAY[]),
        i -> CAST(ROW(
            lower(i.catalog_name),
            lower(i.schema_name),
            lower(i.table_name),
            coalesce(transform(i.columns, c -> lower(c)), ARRAY[]),
            i.physical_input_bytes,
            i.physical_input_rows
        ) AS row(
            catalog_name varchar,
            schema_name varchar,
            table_name varchar,
            columns array(varchar),
            physical_input_bytes bigint,
            physical_input_rows bigint
        ))
    )                                                                  AS inputs,
    CASE
        WHEN output IS NOT NULL AND output.table_name IS NOT NULL
        THEN lower(output.catalog_name || '.' || output.schema_name || '.' || output.table_name)
    END                                                                AS output_table_fqn,
    ingested_at
FROM window_events
WHERE rn = 1;
