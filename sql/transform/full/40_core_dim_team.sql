-- Small dimension: rebuilt in full on every run.
-- Teams that still have history but were removed from the directory are kept,
-- so past usage never loses its team.

DELETE FROM iceberg.core.dim_team;

INSERT INTO iceberg.core.dim_team
SELECT team_id, team_name, cost_center
FROM iceberg.reference.teams
UNION ALL
SELECT 'unassigned', 'Unassigned', NULL
UNION ALL
SELECT DISTINCT f.team_id, f.team_id, NULL
FROM iceberg.core.fct_query AS f
WHERE f.team_id <> 'unassigned'
  AND f.team_id NOT IN (SELECT team_id FROM iceberg.reference.teams);
