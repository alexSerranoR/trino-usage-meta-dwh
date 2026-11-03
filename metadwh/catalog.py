"""Machine-readable catalog of the monitored tables, for data discovery by AI agents.

Combines what the table registry says about each table (owner, description, PII)
with what usage shows (who reads it, which columns, whether it is still used), and
documents the Meta DWH's own tables from their column comments so an agent can also
query the warehouse directly.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from typing import Any

from metadwh.sql_runner import SqlExecutor

_PROFILE_SQL = """
SELECT table_fqn, description, owner_team_id, domain, contains_pii, as_of_date, last_read_date,
       queries_30d, distinct_users_30d, queries_7d, queries_prev_7d, top_teams_30d, usage_status
FROM iceberg.marts.table_usage_profile
ORDER BY queries_30d DESC, table_fqn
"""

_COLUMN_USAGE_SQL = """
SELECT a.table_fqn, c.column_name, count(*) AS reads
FROM iceberg.core.fct_query_table_access AS a
CROSS JOIN UNNEST(a.columns) AS c (column_name)
WHERE a.query_date > DATE '{as_of}' - INTERVAL '30' DAY
GROUP BY 1, 2
ORDER BY 1, 3 DESC, 2
"""

_WAREHOUSE_TABLES_SQL = """
SELECT schema_name, table_name, comment
FROM system.metadata.table_comments
WHERE catalog_name = 'iceberg' AND schema_name IN ('core', 'marts')
"""

_WAREHOUSE_COLUMNS_SQL = """
SELECT t.table_schema, t.table_name, c.column_name, c.data_type, c.comment
FROM iceberg.information_schema.tables AS t
JOIN iceberg.information_schema.columns AS c
    ON c.table_schema = t.table_schema AND c.table_name = t.table_name
WHERE t.table_schema IN ('core', 'marts')
ORDER BY 1, 2, c.ordinal_position
"""


def _iso(value: Any) -> Any:
    return value.isoformat() if isinstance(value, (date, datetime)) else value


def build_catalog(executor: SqlExecutor) -> dict[str, Any]:
    columns, rows = executor.query(_PROFILE_SQL.strip())
    profiles = [dict(zip(columns, map(_iso, row), strict=True)) for row in rows]
    as_of = profiles[0]["as_of_date"] if profiles else date.today().isoformat()

    column_usage: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for table_fqn, column_name, reads in executor.query(_COLUMN_USAGE_SQL.format(as_of=as_of).strip())[1]:
        column_usage[table_fqn].append({"name": column_name, "reads_30d": reads})

    datasets = []
    for profile in profiles:
        warnings = []
        if profile["owner_team_id"] is None:
            warnings.append("Not in the table registry: no owner or documentation.")
        if not profile["description"]:
            warnings.append("No description.")
        if profile["usage_status"] in ("dormant", "never_used"):
            warnings.append("Not used recently; may be deprecated.")
        if profile["contains_pii"]:
            warnings.append("Contains personal data; check access policies before use.")
        datasets.append({
            "table": profile["table_fqn"],
            "description": profile["description"],
            "owner_team": profile["owner_team_id"],
            "domain": profile["domain"],
            "contains_pii": profile["contains_pii"],
            "usage": {
                "status": profile["usage_status"],
                "last_read_date": profile["last_read_date"],
                "queries_30d": profile["queries_30d"],
                "distinct_users_30d": profile["distinct_users_30d"],
                "queries_7d": profile["queries_7d"],
                "queries_prev_7d": profile["queries_prev_7d"],
                "top_teams": profile["top_teams_30d"],
            },
            "columns_by_usage": column_usage.get(profile["table_fqn"], []),
            "warnings": warnings,
        })

    warehouse: dict[str, dict[str, Any]] = {}
    for schema, table, comment in executor.query(_WAREHOUSE_TABLES_SQL.strip())[1]:
        warehouse[f"iceberg.{schema}.{table}"] = {"description": comment, "columns": []}
    for schema, table, column, data_type, comment in executor.query(_WAREHOUSE_COLUMNS_SQL.strip())[1]:
        entry = warehouse.setdefault(f"iceberg.{schema}.{table}", {"description": None, "columns": []})
        entry["columns"].append({"name": column, "type": data_type, "description": comment})

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "as_of_date": as_of,
        "datasets": datasets,
        "usage_warehouse": [{"table": name, **entry} for name, entry in warehouse.items()],
    }
