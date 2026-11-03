-- Small dimension: rebuilt in full on every run.

DELETE FROM iceberg.core.dim_team;

INSERT INTO iceberg.core.dim_team
SELECT team_id, team_name, cost_center
FROM iceberg.reference.teams
UNION ALL
SELECT 'unassigned', 'Unassigned', NULL;
