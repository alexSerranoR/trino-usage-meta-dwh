-- Every distinct query in the raw snapshot each date was built from reaches staging
-- and fct_query exactly once: no events lost, none duplicated.
WITH builds AS (
    SELECT build_date, source_queries
    FROM iceberg.ops.partition_builds
    WHERE build_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
),
staged AS (
    SELECT query_date, count(*) AS queries
    FROM iceberg.staging.query_completed
    WHERE query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
    GROUP BY query_date
),
facts AS (
    SELECT query_date, count(*) AS queries
    FROM iceberg.core.fct_query
    WHERE query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
    GROUP BY query_date
)
SELECT count(*) AS failures
FROM builds AS b
FULL OUTER JOIN staged AS s ON s.query_date = b.build_date
FULL OUTER JOIN facts AS f ON f.query_date = coalesce(b.build_date, s.query_date)
WHERE b.build_date IS NULL
   OR coalesce(s.queries, 0) <> b.source_queries
   OR coalesce(f.queries, 0) <> b.source_queries
