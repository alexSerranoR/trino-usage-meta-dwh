CREATE TABLE IF NOT EXISTS iceberg.core.dim_team (
    team_id      varchar COMMENT 'Team identifier; ''unassigned'' for users without a known team',
    team_name    varchar COMMENT 'Human-readable team name',
    cost_center  varchar COMMENT 'Finance cost center used for chargeback'
)
COMMENT 'Teams that run queries on the Trino cluster. Grain: one row per team.';

CREATE TABLE IF NOT EXISTS iceberg.core.dim_user (
    user_name        varchar COMMENT 'Normalized (lower-case) Trino user',
    account_type     varchar COMMENT '''human'' or ''service''; ''unknown'' if not in the directory',
    current_team_id  varchar COMMENT 'Team the user belongs to today',
    first_query_date date    COMMENT 'Date of the first observed query',
    last_query_date  date    COMMENT 'Date of the most recent observed query'
)
COMMENT 'Users observed in query events. Grain: one row per user.';

CREATE TABLE IF NOT EXISTS iceberg.core.dim_table (
    table_fqn        varchar COMMENT 'Fully qualified name: catalog.schema.table (lower-case)',
    catalog_name     varchar,
    schema_name      varchar,
    table_name       varchar,
    owner_team_id    varchar COMMENT 'Owning team from the table registry; NULL if unregistered',
    domain           varchar COMMENT 'Business domain from the table registry',
    description      varchar COMMENT 'What the table contains, from the table registry',
    contains_pii     boolean COMMENT 'Whether the table holds personal data',
    is_registered    boolean COMMENT 'True if the table exists in the table registry',
    first_read_date  date    COMMENT 'First date the table was read; NULL if never read',
    last_read_date   date    COMMENT 'Most recent date the table was read; NULL if never read'
)
COMMENT 'Iceberg tables known from the registry or observed in queries. Grain: one row per table.';

CREATE TABLE IF NOT EXISTS iceberg.core.fct_query (
    query_id                   varchar COMMENT 'Trino query id',
    query_date                 date    COMMENT 'UTC date of query creation; partition key',
    user_name                  varchar COMMENT 'FK to dim_user',
    team_id                    varchar COMMENT 'FK to dim_team; team of the user at query time',
    account_type               varchar,
    source                     varchar COMMENT 'Client that submitted the query (e.g. trino-cli, airflow)',
    query_state                varchar COMMENT 'FINISHED or FAILED',
    query_type                 varchar COMMENT 'SELECT, INSERT, ...',
    error_code                 varchar,
    query_fingerprint          varchar COMMENT 'Hash of the query text with literals removed; groups repeated query patterns',
    create_time                timestamp(6) with time zone,
    end_time                   timestamp(6) with time zone,
    cpu_time_ms                bigint  COMMENT 'Total CPU time across all workers',
    wall_time_ms               bigint  COMMENT 'Elapsed time from execution start to end',
    queued_time_ms             bigint  COMMENT 'Time spent waiting for resources',
    peak_user_memory_bytes     bigint  COMMENT 'Peak memory; a maximum, do not sum',
    cumulative_memory          double  COMMENT 'Memory integrated over time (byte-milliseconds); additive',
    physical_input_bytes       bigint  COMMENT 'Bytes read from storage',
    output_bytes               bigint,
    input_table_count          integer COMMENT 'Number of distinct tables read',
    output_table_fqn           varchar COMMENT 'Written table for INSERT / CTAS queries'
)
COMMENT 'Executed queries. Grain: one row per query.'
WITH (partitioning = ARRAY['query_date']);

CREATE TABLE IF NOT EXISTS iceberg.core.fct_query_table_access (
    query_id                   varchar COMMENT 'FK to fct_query',
    query_date                 date    COMMENT 'Partition key',
    table_fqn                  varchar COMMENT 'FK to dim_table',
    user_name                  varchar COMMENT 'Denormalized from fct_query',
    team_id                    varchar COMMENT 'Denormalized from fct_query',
    query_state                varchar,
    columns                    array(varchar) COMMENT 'Columns read from the table',
    scan_count                 integer COMMENT 'Times the table appears in the plan (self-joins)',
    physical_input_bytes       bigint  COMMENT 'Bytes read from this table',
    physical_input_rows        bigint,
    allocation_weight          double  COMMENT 'Share of the query cost attributed to this table; sums to 1 per query',
    cpu_time_ms_allocated      double  COMMENT 'cpu_time_ms * allocation_weight',
    cumulative_memory_allocated double COMMENT 'cumulative_memory * allocation_weight'
)
COMMENT 'Tables read by each query. Grain: one row per (query, table).'
WITH (partitioning = ARRAY['query_date']);
