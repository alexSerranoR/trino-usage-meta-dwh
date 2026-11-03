-- A user belongs to at most one team at any point in time, so the point-in-time join
-- cannot fan out. Catches overlapping periods, duplicated rows with the same start,
-- and empty or inverted periods.
SELECT count(*) AS failures
FROM (
    SELECT
        valid_from,
        valid_to,
        lead(valid_from) OVER (
            PARTITION BY lower(trim(user_name)) ORDER BY valid_from, valid_to
        ) AS next_valid_from
    FROM iceberg.reference.user_team_membership
)
WHERE (next_valid_from IS NOT NULL AND (valid_to IS NULL OR next_valid_from < valid_to))
   OR (valid_to IS NOT NULL AND valid_to <= valid_from)
