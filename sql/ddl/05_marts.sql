CREATE TABLE IF NOT EXISTS iceberg.marts.table_usage_daily (
    usage_date             date    COMMENT 'Partition key',
    table_fqn              varchar,
    query_count            bigint,
    failed_query_count     bigint,
    distinct_users         bigint  COMMENT 'Exact for this day only; not additive across days',
    distinct_teams         bigint,
    users_hll              varbinary COMMENT 'HyperLogLog of users; merge across days for period distinct counts',
    physical_input_bytes   bigint,
    cpu_time_ms_allocated  double
)
COMMENT 'Daily usage per table. Grain: one row per (date, table).'
WITH (partitioning = ARRAY['usage_date'], sorted_by = ARRAY['table_fqn']);

CREATE TABLE IF NOT EXISTS iceberg.marts.team_usage_daily (
    usage_date              date    COMMENT 'Partition key',
    team_id                 varchar,
    query_count             bigint,
    failed_query_count      bigint,
    distinct_users          bigint  COMMENT 'Exact for this day only; not additive across days',
    users_hll               varbinary COMMENT 'HyperLogLog of users; merge across days for period distinct counts',
    distinct_tables_read    bigint,
    cpu_time_ms             bigint,
    wall_time_ms            bigint,
    queued_time_ms          bigint,
    cumulative_memory       double,
    max_peak_memory_bytes   bigint  COMMENT 'Largest single-query peak memory of the day',
    physical_input_bytes    bigint,
    output_bytes            bigint
)
COMMENT 'Daily resource consumption per team. Grain: one row per (date, team).'
WITH (partitioning = ARRAY['usage_date'], sorted_by = ARRAY['team_id']);

CREATE TABLE IF NOT EXISTS iceberg.marts.table_usage_profile (
    table_fqn              varchar,
    description            varchar,
    owner_team_id          varchar,
    domain                 varchar,
    contains_pii           boolean,
    as_of_date             date    COMMENT 'Last date covered by the profile',
    last_read_date         date,
    queries_30d            bigint,
    distinct_users_30d     bigint,
    queries_7d             bigint,
    queries_prev_7d        bigint,
    top_teams_30d          array(varchar) COMMENT 'Teams with most queries, descending',
    top_columns_30d        array(varchar) COMMENT 'Most frequently read columns, descending',
    usage_status           varchar COMMENT 'active, declining, dormant or never_used'
)
COMMENT 'One row per table summarizing recent usage. Intended for data discovery by people and AI agents.';
