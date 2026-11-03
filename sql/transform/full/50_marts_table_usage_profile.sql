-- One row per known table summarizing the 30 days ending at as_of_date.
-- Rebuilt in full on every run; the output is small (one row per table).

DELETE FROM iceberg.marts.table_usage_profile;

INSERT INTO iceberg.marts.table_usage_profile
WITH recent_access AS (
    SELECT *
    FROM iceberg.core.fct_query_table_access
    WHERE query_date BETWEEN DATE '${as_of_date}' - INTERVAL '29' DAY AND DATE '${as_of_date}'
),
usage AS (
    SELECT
        table_fqn,
        count(*)                                                        AS queries_30d,
        count(DISTINCT user_name)                                       AS distinct_users_30d,
        count_if(query_date > DATE '${as_of_date}' - INTERVAL '7' DAY)  AS queries_7d,
        count_if(query_date <= DATE '${as_of_date}' - INTERVAL '7' DAY
             AND query_date >  DATE '${as_of_date}' - INTERVAL '14' DAY) AS queries_prev_7d
    FROM recent_access
    GROUP BY table_fqn
),
team_ranking AS (
    SELECT
        table_fqn,
        slice(array_agg(team_id ORDER BY query_count DESC, team_id), 1, 3) AS top_teams_30d
    FROM (
        SELECT table_fqn, team_id, count(*) AS query_count
        FROM recent_access
        GROUP BY table_fqn, team_id
    )
    GROUP BY table_fqn
),
column_ranking AS (
    SELECT
        table_fqn,
        slice(array_agg(column_name ORDER BY read_count DESC, column_name), 1, 5) AS top_columns_30d
    FROM (
        SELECT a.table_fqn, c.column_name, count(*) AS read_count
        FROM recent_access AS a
        CROSS JOIN UNNEST(a.columns) AS c (column_name)
        GROUP BY a.table_fqn, c.column_name
    )
    GROUP BY table_fqn
)
SELECT
    d.table_fqn,
    d.description,
    d.owner_team_id,
    d.domain,
    d.contains_pii,
    DATE '${as_of_date}'                          AS as_of_date,
    d.last_read_date,
    coalesce(u.queries_30d, 0)                    AS queries_30d,
    coalesce(u.distinct_users_30d, 0)             AS distinct_users_30d,
    coalesce(u.queries_7d, 0)                     AS queries_7d,
    coalesce(u.queries_prev_7d, 0)                AS queries_prev_7d,
    coalesce(t.top_teams_30d, ARRAY[])            AS top_teams_30d,
    coalesce(c.top_columns_30d, ARRAY[])          AS top_columns_30d,
    CASE
        WHEN d.last_read_date IS NULL                             THEN 'never_used'
        WHEN coalesce(u.queries_7d, 0) + coalesce(u.queries_prev_7d, 0) = 0 THEN 'dormant'
        WHEN coalesce(u.queries_7d, 0) < 0.5 * u.queries_prev_7d  THEN 'declining'
        ELSE 'active'
    END                                           AS usage_status
FROM iceberg.core.dim_table AS d
LEFT JOIN usage AS u          ON u.table_fqn = d.table_fqn
LEFT JOIN team_ranking AS t   ON t.table_fqn = d.table_fqn
LEFT JOIN column_ranking AS c ON c.table_fqn = d.table_fqn;
