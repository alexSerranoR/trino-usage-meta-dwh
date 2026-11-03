-- Rebuilds fct_query_table_access partitions for [start_date, end_date].
-- Query cost is split across the tables it reads, proportionally to bytes read.
-- If any input does not report bytes, proportional shares would give it nothing,
-- so the cost of that query is split evenly instead.
-- Trino's own metadata (the system catalog and information_schema) is not a data
-- table and is left out.

DELETE FROM iceberg.core.fct_query_table_access
WHERE query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}';

INSERT INTO iceberg.core.fct_query_table_access
WITH scans AS (
    SELECT
        q.query_id,
        q.query_date,
        i.catalog_name || '.' || i.schema_name || '.' || i.table_name AS table_fqn,
        i.columns,
        i.physical_input_bytes,
        i.physical_input_rows
    FROM iceberg.staging.query_completed AS q
    CROSS JOIN UNNEST(q.inputs) AS i (
        catalog_name, schema_name, table_name, columns, physical_input_bytes, physical_input_rows
    )
    WHERE q.query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
      AND i.catalog_name <> 'system'
      AND i.schema_name <> 'information_schema'
),
per_table AS (
    SELECT
        query_id,
        query_date,
        table_fqn,
        array_sort(array_distinct(flatten(array_agg(columns))))      AS columns,
        CAST(count(*) AS integer)                                    AS scan_count,
        coalesce(sum(physical_input_bytes), 0)                       AS physical_input_bytes,
        coalesce(sum(physical_input_rows), 0)                        AS physical_input_rows,
        bool_or(physical_input_bytes IS NULL)                        AS bytes_unknown
    FROM scans
    GROUP BY query_id, query_date, table_fqn
),
weighted AS (
    SELECT
        *,
        CASE
            WHEN NOT bool_or(bytes_unknown) OVER (PARTITION BY query_id)
             AND sum(physical_input_bytes) OVER (PARTITION BY query_id) > 0
            THEN CAST(physical_input_bytes AS double)
                 / sum(physical_input_bytes) OVER (PARTITION BY query_id)
            ELSE 1.0E0 / count(*) OVER (PARTITION BY query_id)
        END                                                          AS allocation_weight
    FROM per_table
)
SELECT
    w.query_id,
    w.query_date,
    w.table_fqn,
    f.user_name,
    f.team_id,
    f.query_state,
    w.columns,
    w.scan_count,
    w.physical_input_bytes,
    w.physical_input_rows,
    w.allocation_weight,
    w.allocation_weight * f.cpu_time_ms                              AS cpu_time_ms_allocated,
    w.allocation_weight * f.cumulative_memory                        AS cumulative_memory_allocated
FROM weighted AS w
JOIN iceberg.core.fct_query AS f
    ON  f.query_id = w.query_id
    AND f.query_date = w.query_date
WHERE f.query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}';
