-- Every distinct raw query in the window reaches staging and fct_query exactly once.
SELECT abs(raw_queries - staged_queries) + abs(raw_queries - fact_queries) AS failures
FROM (
    SELECT
        (SELECT count(DISTINCT query_id)
         FROM iceberg.raw.trino_query_completed
         WHERE create_time >= TIMESTAMP '${start_date} 00:00:00 UTC'
           AND create_time <  TIMESTAMP '${end_date} 00:00:00 UTC' + INTERVAL '1' DAY) AS raw_queries,
        (SELECT count(*)
         FROM iceberg.staging.query_completed
         WHERE query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}')        AS staged_queries,
        (SELECT count(*)
         FROM iceberg.core.fct_query
         WHERE query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}')        AS fact_queries
)
