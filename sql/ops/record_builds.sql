-- Records which raw events and which logic version each rebuilt date reflects.

DELETE FROM iceberg.ops.partition_builds
WHERE build_date BETWEEN DATE '${start_date}' AND DATE '${end_date}';

INSERT INTO iceberg.ops.partition_builds
SELECT
    CAST(create_time AT TIME ZONE 'UTC' AS date)  AS build_date,
    '${logic_version}'                            AS logic_version,
    max(ingested_at)                              AS source_max_ingested_at,
    count(*)                                      AS source_events,
    '${run_id}'                                   AS run_id,
    current_timestamp                             AS built_at
FROM iceberg.raw.trino_query_completed
WHERE create_time >= TIMESTAMP '${start_date} 00:00:00 UTC'
  AND create_time <  TIMESTAMP '${end_date} 00:00:00 UTC' + INTERVAL '1' DAY
  AND ingested_at <= TIMESTAMP '${ingested_cutoff}'
GROUP BY 1;
