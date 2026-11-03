-- How many distinct users queried table X during a period?
-- Exact answer from the access fact, and the approximate answer from merged daily
-- HyperLogLog sketches, which only scans one small row per day.
SELECT
    exact.table_fqn,
    exact.distinct_users                     AS distinct_users_exact,
    approx.distinct_users                    AS distinct_users_hll,
    exact.query_count
FROM (
    SELECT
        table_fqn,
        count(DISTINCT user_name)            AS distinct_users,
        count(*)                             AS query_count
    FROM iceberg.core.fct_query_table_access
    WHERE table_fqn = '${table_fqn}'
      AND query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
    GROUP BY table_fqn
) AS exact
CROSS JOIN (
    SELECT cardinality(merge(CAST(users_hll AS HyperLogLog))) AS distinct_users
    FROM iceberg.marts.table_usage_daily
    WHERE table_fqn = '${table_fqn}'
      AND usage_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
) AS approx
