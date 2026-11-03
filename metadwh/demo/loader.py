"""Loads demo reference data and raw events into Iceberg through Trino."""

from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta, timezone

from metadwh.demo.events import EventGenerator
from metadwh.demo.reference import TABLES, TEAMS, USERS
from metadwh.demo.sql_literals import event_values, literal
from metadwh.sql_runner import SqlExecutor

log = logging.getLogger(__name__)

_BATCH_SIZE = 250


def _insert(executor: SqlExecutor, table: str, rows: list[str]) -> None:
    for offset in range(0, len(rows), _BATCH_SIZE):
        executor.execute(f"INSERT INTO {table} VALUES " + ",\n".join(rows[offset:offset + _BATCH_SIZE]))


def _row(*values: object) -> str:
    return "(" + ", ".join(literal(v) for v in values) + ")"


def load_reference(executor: SqlExecutor, start: date) -> None:
    period_start = datetime.combine(start, time(0), tzinfo=timezone.utc)
    directory_start = period_start - timedelta(days=365)

    memberships = []
    for user in USERS:
        if not user.in_directory:
            continue
        if user.moves_to:
            moved_at = period_start + timedelta(days=user.moves_to[0])
            memberships.append(_row(user.name, user.account_type, user.team_id, directory_start, moved_at))
            memberships.append(_row(user.name, user.account_type, user.moves_to[1], moved_at, None))
        else:
            memberships.append(_row(user.name, user.account_type, user.team_id, directory_start, None))

    registry = [
        _row(t.fqn, t.owner_team_id, t.domain, t.description, t.contains_pii)
        for t in TABLES
        if t.registered
    ]

    for table, rows in [
        ("iceberg.reference.teams", [_row(*team) for team in TEAMS]),
        ("iceberg.reference.user_team_membership", memberships),
        ("iceberg.reference.table_registry", registry),
    ]:
        executor.execute(f"DELETE FROM {table}")
        _insert(executor, table, rows)
        log.info("Loaded %4d rows into %s", len(rows), table)


def load_events(executor: SqlExecutor, start: date, end: date, seed: int, events_per_weekday: int) -> int:
    events = EventGenerator(start, end, seed=seed, events_per_weekday=events_per_weekday).generate()
    executor.execute("DELETE FROM iceberg.raw.trino_query_completed")
    _insert(executor, "iceberg.raw.trino_query_completed", [event_values(e) for e in events])
    log.info("Loaded %d raw events (%s .. %s)", len(events), start, end)
    return len(events)
