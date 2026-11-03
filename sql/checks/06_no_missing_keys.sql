-- Facts always carry a user, a team and a known dimension key.
SELECT
    (SELECT count(*)
     FROM iceberg.core.fct_query AS f
     LEFT JOIN iceberg.core.dim_team AS t ON t.team_id = f.team_id
     WHERE f.query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
       AND (f.user_name IS NULL OR t.team_id IS NULL))
  + (SELECT count(*)
     FROM iceberg.core.fct_query_table_access AS a
     LEFT JOIN iceberg.core.dim_table AS d ON d.table_fqn = a.table_fqn
     WHERE a.query_date BETWEEN DATE '${start_date}' AND DATE '${end_date}'
       AND d.table_fqn IS NULL) AS failures
