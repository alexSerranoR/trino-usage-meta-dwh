-- Rebuilt in full on every run from observed users and the current directory state.

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
current_membership AS (
    SELECT user_name, account_type, team_id
    FROM iceberg.reference.user_team_membership
    WHERE valid_to IS NULL
)
SELECT
    o.user_name,
    coalesce(c.account_type, 'unknown')  AS account_type,
    coalesce(c.team_id, 'unassigned')    AS current_team_id,
    o.first_query_date,
    o.last_query_date
FROM observed AS o
LEFT JOIN current_membership AS c
    ON c.user_name = o.user_name;
