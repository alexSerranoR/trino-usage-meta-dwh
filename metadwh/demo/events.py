"""Generator of synthetic QueryCompletedEvent records.

The generated data deliberately includes the issues a real event stream has:
redelivered events, late arrivals, inconsistent identifier casing, failed queries,
multi-table and self-join queries, queries without inputs, a user who changes
team and a user missing from the company directory.
"""

from __future__ import annotations

import random
import string
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone

from metadwh.demo.reference import AIRFLOW_OUTPUTS, TABLES, USERS, Table, User

_ERROR_CODES = ["EXCEEDED_TIME_LIMIT", "EXCEEDED_USER_MEMORY_LIMIT", "SYNTAX_ERROR", "TABLE_NOT_FOUND"]
_TABLES_BY_FQN = {table.fqn: table for table in TABLES}


@dataclass
class TableInput:
    catalog_name: str
    schema_name: str
    table_name: str
    columns: list[str]
    physical_input_bytes: int | None
    physical_input_rows: int | None


@dataclass
class QueryEvent:
    query_id: str
    query_state: str
    query_type: str
    error_code: str | None
    query: str
    user_name: str
    source: str
    session_catalog: str
    session_schema: str
    create_time: datetime
    execution_start_time: datetime
    end_time: datetime
    cpu_time_ms: int
    wall_time_ms: int
    queued_time_ms: int
    peak_user_memory_bytes: int
    cumulative_memory: float
    physical_input_bytes: int
    physical_input_rows: int
    output_bytes: int
    output_rows: int
    inputs: list[TableInput] = field(default_factory=list)
    output: tuple[str, str, str] | None = None
    ingested_at: datetime | None = None


class EventGenerator:
    def __init__(self, start: date, end: date, seed: int = 42, events_per_weekday: int = 420) -> None:
        self.start = start
        self.end = end
        self.events_per_weekday = events_per_weekday
        self._rng = random.Random(seed)
        self._sequence = 0

    def generate(self) -> list[QueryEvent]:
        events: list[QueryEvent] = []
        day = self.start
        while day <= self.end:
            events.extend(self._generate_day(day))
            day += timedelta(days=1)
        return self._add_redeliveries(events)

    def _generate_day(self, day: date) -> list[QueryEvent]:
        weekend = day.weekday() >= 5
        expected = self.events_per_weekday * (0.35 if weekend else 1.0)
        count = max(1, int(self._rng.gauss(expected, expected * 0.08)))
        weights = [user.activity * (0.15 if weekend and user.account_type == "human" else 1.0) for user in USERS]
        return [self._generate_event(day, user) for user in self._rng.choices(USERS, weights=weights, k=count)]

    def _generate_event(self, day: date, user: User) -> QueryEvent:
        rng = self._rng
        create_time = self._create_time(day, user)
        source = rng.choice(user.sources)
        user_name = user.name.title() if rng.random() < 0.03 else user.name

        if user.name == "svc_airflow" and rng.random() < 0.7:
            output_fqn, input_fqns = rng.choice(AIRFLOW_OUTPUTS)
            query_type = "INSERT"
            tables = [_TABLES_BY_FQN[fqn] for fqn in input_fqns]
        elif rng.random() < 0.02:
            output_fqn, query_type, tables = None, "SELECT", []
        else:
            output_fqn, query_type = None, "SELECT"
            tables = self._pick_tables(self._team_on(user, day))

        inputs = [self._table_input(table, user) for table in tables]
        if tables and rng.random() < 0.05:
            inputs.append(self._table_input(tables[0], user))

        total_bytes = sum(i.physical_input_bytes or 0 for i in inputs)
        total_rows = sum(i.physical_input_rows or 0 for i in inputs)
        join_factor = 1 + 0.3 * max(len(inputs) - 1, 0)
        cpu_ms = int(total_bytes / 1_000_000 * rng.uniform(5, 20) * join_factor) + rng.randint(5, 80)

        failed = rng.random() < 0.03
        error_code = rng.choice(_ERROR_CODES) if failed else None
        if failed:
            cpu_ms = int(cpu_ms * rng.uniform(0.05, 0.6))

        queued_ms = int(rng.expovariate(1 / 250)) if rng.random() > 0.05 else rng.randint(5_000, 60_000)
        wall_ms = int(cpu_ms / rng.uniform(4, 32)) + rng.randint(80, 400)
        peak_memory = int(min(total_bytes * rng.uniform(0.02, 0.25), 12 * 1024**3)) + rng.randint(2, 20) * 1024**2
        execution_start = create_time + timedelta(milliseconds=queued_ms)
        end_time = execution_start + timedelta(milliseconds=wall_ms)

        delay = timedelta(seconds=rng.randint(5, 300))
        if rng.random() < 0.02:
            delay = timedelta(days=rng.randint(1, 3), seconds=rng.randint(0, 3600))

        output_bytes = int(total_bytes * rng.uniform(0.01, 0.1)) if output_fqn and not failed else 0
        return QueryEvent(
            query_id=self._query_id(create_time),
            query_state="FAILED" if failed else "FINISHED",
            query_type=query_type,
            error_code=error_code,
            query=self._query_text(query_type, inputs, output_fqn, day, user),
            user_name=user_name,
            source=source,
            session_catalog="iceberg",
            session_schema="analytics",
            create_time=create_time,
            execution_start_time=execution_start,
            end_time=end_time,
            cpu_time_ms=cpu_ms,
            wall_time_ms=wall_ms,
            queued_time_ms=queued_ms,
            peak_user_memory_bytes=peak_memory,
            cumulative_memory=float(peak_memory) * wall_ms * rng.uniform(0.3, 0.7),
            physical_input_bytes=total_bytes,
            physical_input_rows=total_rows,
            output_bytes=output_bytes,
            output_rows=output_bytes // 120,
            inputs=inputs,
            output=tuple(output_fqn.split(".")) if output_fqn else None,
            ingested_at=end_time + delay,
        )

    def _team_on(self, user: User, day: date) -> str:
        if user.moves_to and (day - self.start).days >= user.moves_to[0]:
            return user.moves_to[1]
        return user.team_id

    def _pick_tables(self, team_id: str) -> list[Table]:
        candidates = [t for t in TABLES if team_id in t.readers]
        weights = [t.readers[team_id] for t in candidates]
        wanted = self._rng.choices([1, 2, 3, 4], weights=[60, 25, 10, 5])[0]
        picked: list[Table] = []
        while len(picked) < min(wanted, len(candidates)):
            table = self._rng.choices(candidates, weights=weights)[0]
            if table not in picked:
                picked.append(table)
        return picked

    def _table_input(self, table: Table, user: User) -> TableInput:
        rng = self._rng
        catalog, schema, name = table.fqn.split(".")
        if rng.random() < 0.04:
            schema = schema.title()
        width = rng.randint(2, len(table.columns))
        columns = sorted(rng.sample(list(table.columns), width), key=table.columns.index)
        # Service accounts tend to scan full partitions; people filter more.
        fraction = rng.uniform(0.5, 1.2) if user.account_type == "service" else rng.uniform(0.05, 0.6)
        scanned = int(rng.lognormvariate(0, 0.8) * table.scan_bytes_median * fraction)
        bytes_reported = None if rng.random() < 0.01 else scanned
        return TableInput(catalog, schema, name, columns, bytes_reported, scanned // 150 if bytes_reported else None)

    def _create_time(self, day: date, user: User) -> datetime:
        rng = self._rng
        if user.account_type == "service":
            seconds = rng.randint(0, 86_399)
        else:
            seconds = int(min(max(rng.gauss(13.5 * 3600, 2.7 * 3600), 6 * 3600), 21 * 3600))
        moment = datetime.combine(day, time(0), tzinfo=timezone.utc) + timedelta(seconds=seconds)
        return moment + timedelta(microseconds=rng.randint(0, 999_999))

    def _query_id(self, created: datetime) -> str:
        self._sequence += 1
        suffix = "".join(self._rng.choices(string.ascii_lowercase, k=5))
        return f"{created:%Y%m%d_%H%M%S}_{self._sequence:05d}_{suffix}"

    def _query_text(
        self, query_type: str, inputs: list[TableInput], output_fqn: str | None, day: date, user: User
    ) -> str:
        if not inputs:
            return "SELECT 1"
        tables = [f"{i.catalog_name}.{i.schema_name}.{i.table_name}" for i in inputs]
        columns = ", ".join(inputs[0].columns)
        sql = f"SELECT {columns} FROM {tables[0]} t0"
        for index, table in enumerate(tables[1:], start=1):
            sql += f" JOIN {table} t{index} USING ({inputs[0].columns[0]})"
        # Dashboards and agents issue the same statements repeatedly; people add ad-hoc filters.
        if user.account_type == "human":
            sql += f" WHERE t0.{inputs[0].columns[0]} >= DATE '{day - timedelta(days=self._rng.randint(1, 90))}'"
        if query_type == "INSERT" and output_fqn:
            sql = f"INSERT INTO {output_fqn} {sql}"
        return sql

    def _add_redeliveries(self, events: list[QueryEvent]) -> list[QueryEvent]:
        """At-least-once delivery: about 1% of events arrive twice."""
        duplicates = []
        for event in events:
            if self._rng.random() < 0.01:
                copy = QueryEvent(**{**event.__dict__})
                copy.ingested_at = event.ingested_at + timedelta(minutes=self._rng.randint(1, 90))
                duplicates.append(copy)
        return events + duplicates
