-- Dates whose derived data is missing or out of date, and why.
-- Only create_time and ingested_at are read from raw, so the scan touches two columns.
WITH source AS (
    SELECT
        CAST(create_time AT TIME ZONE 'UTC' AS date)  AS build_date,
        max(ingested_at)                              AS max_ingested_at,
        count(*)                                      AS events
    FROM iceberg.raw.trino_query_completed
    WHERE ingested_at <= TIMESTAMP '${ingested_cutoff}'
    GROUP BY 1
)
SELECT
    s.build_date,
    CASE
        WHEN b.build_date IS NULL                        THEN 'not_built'
        WHEN b.logic_version <> '${logic_version}'       THEN 'logic_changed'
        WHEN s.max_ingested_at > b.source_max_ingested_at
          OR s.events <> b.source_events                 THEN 'new_events'
    END                                                  AS reason,
    s.events
FROM source AS s
LEFT JOIN iceberg.ops.partition_builds AS b
    ON b.build_date = s.build_date
WHERE b.build_date IS NULL
   OR b.logic_version <> '${logic_version}'
   OR s.max_ingested_at > b.source_max_ingested_at
   OR s.events <> b.source_events
ORDER BY s.build_date
