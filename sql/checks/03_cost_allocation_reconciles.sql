-- Allocated CPU across a query's tables adds up to the query's CPU, and weights sum to 1.
SELECT count(*) AS failures
FROM (
    SELECT
        f.query_id,
        f.cpu_time_ms,
        sum(a.cpu_time_ms_allocated) AS allocated_cpu,
        sum(a.allocation_weight)     AS total_weight
    FROM iceberg.core.fct_query AS f
    JOIN iceberg.core.fct_query_table_access AS a
        ON a.query_id = f.query_id AND a.query_date = f.query_date
    WHERE f.query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
      AND a.query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
    GROUP BY f.query_id, f.cpu_time_ms
)
WHERE abs(allocated_cpu - cpu_time_ms) > 0.001
   OR abs(total_weight - 1.0E0) > 1.0E-9
