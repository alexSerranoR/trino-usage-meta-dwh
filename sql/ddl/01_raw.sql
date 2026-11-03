-- One row per QueryCompletedEvent emitted by the Trino event listener.
-- Append-only: events may be delivered more than once and may arrive late.
CREATE TABLE IF NOT EXISTS iceberg.raw.trino_query_completed (
    query_id                varchar,
    query_state             varchar,
    query_type              varchar,
    error_code              varchar,
    query                   varchar,
    user_name               varchar,
    source                  varchar,
    session_catalog         varchar,
    session_schema          varchar,
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
    output                  row(
                                catalog_name varchar,
                                schema_name varchar,
                                table_name varchar
                            ),
    ingested_at             timestamp(6) with time zone
)
WITH (partitioning = ARRAY['day(create_time)']);
