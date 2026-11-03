-- Records which raw snapshot and logic version each rebuilt date reflects.
-- Reads the same raw snapshot as the staging step, so the record matches what was built.

INSERT INTO iceberg.ops.partition_builds
SELECT
    CAST(create_time AT TIME ZONE 'UTC' AS date)  AS build_date,
    '${logic_version}'                            AS logic_version,
    max(ingested_at)                              AS source_max_ingested_at,
    count(*)                                      AS source_events,
    count(DISTINCT query_id)                      AS source_queries,
    ${raw_snapshot_id}                            AS raw_snapshot_id,
    '${run_id}'                                   AS run_id,
    current_timestamp                             AS built_at
FROM iceberg.raw.trino_query_completed FOR VERSION AS OF ${raw_snapshot_id}
WHERE create_time >= TIMESTAMP '${start_date} 00:00:00 UTC'
  AND create_time <  TIMESTAMP '${end_date} 00:00:00 UTC' + INTERVAL '1' DAY
GROUP BY 1;
