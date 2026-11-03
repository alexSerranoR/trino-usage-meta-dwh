-- A user belongs to at most one team at any point in time, so the point-in-time join cannot fan out.
SELECT count(*) AS failures
FROM iceberg.reference.user_team_membership AS a
JOIN iceberg.reference.user_team_membership AS b
    ON  a.user_name = b.user_name
    AND a.valid_from < b.valid_from
    AND (a.valid_to IS NULL OR a.valid_to > b.valid_from)
