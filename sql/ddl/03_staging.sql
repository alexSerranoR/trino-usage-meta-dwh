-- Deduplicated and normalized query events. Grain: one row per query_id.
CREATE TABLE IF NOT EXISTS iceberg.staging.query_completed (
    query_id                varchar,
    query_date              date,
    query_state             varchar,
    query_type              varchar,
    error_code              varchar,
    query_fingerprint       varchar,
    user_name               varchar,
    source                  varchar,
    create_time             timestamp(6) with time zone,
    execution_start_time    timestamp(6) with time zone,
    end_time                timestamp(6) with time zone,
    cpu_time_ms             bigint,
    wall_time_ms            bigint,
    queued_time_ms          bigint,
    peak_user_memory_bytes  bigint,
    cumulative_memory       double,
    physical_input_bytes    bigint,
    physical_input_rows     bigint,
    output_bytes            bigint,
    output_rows             bigint,
    inputs                  array(row(
                                catalog_name varchar,
                                schema_name varchar,
                                table_name varchar,
                                columns array(varchar),
                                physical_input_bytes bigint,
                                physical_input_rows bigint
                            )),
    output_table_fqn        varchar,
    ingested_at             timestamp(6) with time zone
)
WITH (partitioning = ARRAY['query_date']);
