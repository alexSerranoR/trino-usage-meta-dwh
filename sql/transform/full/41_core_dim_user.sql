-- Rebuilt in full on every run from observed users and the directory.
-- The latest membership row is used, so users who have left keep their last team.

DELETE FROM iceberg.core.dim_user;

INSERT INTO iceberg.core.dim_user
WITH observed AS (
    SELECT
        user_name,
        min(query_date) AS first_query_date,
        max(query_date) AS last_query_date
    FROM iceberg.core.fct_query
    GROUP BY user_name
),
latest_membership AS (
    SELECT user_name, account_type, team_id
    FROM (
        SELECT
            lower(trim(user_name)) AS user_name,
            account_type,
            team_id,
            row_number() OVER (PARTITION BY lower(trim(user_name)) ORDER BY valid_from DESC) AS rn
        FROM iceberg.reference.user_team_membership
    )
    WHERE rn = 1
)
SELECT
    o.user_name,
    coalesce(c.account_type, 'unknown')  AS account_type,
    coalesce(c.team_id, 'unassigned')    AS current_team_id,
    o.first_query_date,
    o.last_query_date
FROM observed AS o
LEFT JOIN latest_membership AS c
    ON c.user_name = o.user_name;
