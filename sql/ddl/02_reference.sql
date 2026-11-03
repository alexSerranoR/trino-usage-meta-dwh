-- Reference data that Trino events do not contain.
-- In production these are synced from the company directory and the data catalog.

CREATE TABLE IF NOT EXISTS iceberg.reference.teams (
    team_id      varchar,
    team_name    varchar,
    cost_center  varchar
);

-- Type 2 history of user-to-team assignments. valid_to IS NULL marks the current row.
CREATE TABLE IF NOT EXISTS iceberg.reference.user_team_membership (
    user_name     varchar,
    account_type  varchar,
    team_id       varchar,
    valid_from    timestamp(6) with time zone,
    valid_to      timestamp(6) with time zone
);

CREATE TABLE IF NOT EXISTS iceberg.reference.table_registry (
    table_fqn      varchar,
    owner_team_id  varchar,
    domain         varchar,
    description    varchar,
    contains_pii   boolean
);
