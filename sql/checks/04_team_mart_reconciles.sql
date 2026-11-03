-- Daily team totals in the mart match the query fact table.
SELECT count(*) AS failures
FROM (
    SELECT query_date AS usage_date, team_id, count(*) AS query_count, sum(cpu_time_ms) AS cpu_time_ms
    FROM iceberg.core.fct_query
    WHERE query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
    GROUP BY query_date, team_id
) AS f
FULL OUTER JOIN (
    SELECT usage_date, team_id, query_count, cpu_time_ms
    FROM iceberg.marts.team_usage_daily
    WHERE usage_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
) AS m
    ON m.usage_date = f.usage_date AND m.team_id = f.team_id
WHERE m.team_id IS NULL
   OR f.team_id IS NULL
   OR m.query_count <> f.query_count
   OR m.cpu_time_ms <> f.cpu_time_ms
