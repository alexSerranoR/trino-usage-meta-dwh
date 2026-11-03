-- Pipeline state. Lets the planner decide which dates need rebuilding.
CREATE TABLE IF NOT EXISTS iceberg.ops.partition_builds (
    build_date              date    COMMENT 'Date partition that was built in staging, core and marts',
    logic_version           varchar COMMENT 'Hash of the incremental SQL used for the build',
    source_max_ingested_at  timestamp(6) with time zone COMMENT 'Latest raw event ingestion time included',
    source_events           bigint  COMMENT 'Raw events for the date included in the build',
    run_id                  varchar,
    built_at                timestamp(6) with time zone
)
COMMENT 'Build state of each date partition. Grain: one row per date.';

CREATE TABLE IF NOT EXISTS iceberg.ops.pipeline_runs (
    run_id            varchar,
    command           varchar,
    window_start      date,
    window_end        date,
    logic_version     varchar,
    status            varchar COMMENT 'succeeded or failed',
    started_at        timestamp(6) with time zone,
    finished_at       timestamp(6) with time zone,
    duration_seconds  double,
    error             varchar
)
COMMENT 'Audit log of pipeline executions. Grain: one row per run.';
