from __future__ import annotations

import hashlib
import logging
import re
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from metadwh.config import SQL_DIR
from metadwh.literals import literal
from metadwh.sql_runner import SqlExecutor, render

log = logging.getLogger(__name__)

DDL_DIR = SQL_DIR / "ddl"
INCREMENTAL_DIR = SQL_DIR / "transform" / "incremental"
FULL_REFRESH_DIR = SQL_DIR / "transform" / "full"
CHECKS_DIR = SQL_DIR / "checks"
OPS_DIR = SQL_DIR / "ops"

MAINTAINED_SCHEMAS = ("raw", "staging", "core", "marts", "ops")

_LINE_COMMENT = re.compile(r"^\s*--.*$", re.MULTILINE)


def _sorted_sql(directory: Path) -> list[Path]:
    return sorted(directory.glob("*.sql"))


def normalized_sql(sql: str) -> str:
    """SQL without comments and with whitespace collapsed, so formatting changes are not logic changes."""
    return " ".join(_LINE_COMMENT.sub("", sql).split())


def logic_version() -> str:
    """Fingerprint of the incremental transformations. Changes whenever their logic changes."""
    digest = hashlib.sha256()
    for path in _sorted_sql(INCREMENTAL_DIR):
        digest.update(path.name.encode())
        digest.update(normalized_sql(path.read_text(encoding="utf-8")).encode())
    return digest.hexdigest()[:12]


def date_chunks(start: date, end: date, chunk_days: int) -> list[tuple[date, date]]:
    """Split the inclusive range [start, end] into consecutive chunks of at most chunk_days."""
    if end < start:
        raise ValueError(f"end date {end} is before start date {start}")
    if chunk_days < 1:
        raise ValueError("chunk_days must be at least 1")
    chunks = []
    chunk_start = start
    while chunk_start <= end:
        chunk_end = min(chunk_start + timedelta(days=chunk_days - 1), end)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end + timedelta(days=1)
    return chunks


def contiguous_ranges(dates: list[date]) -> list[tuple[date, date]]:
    """Group dates into inclusive ranges of consecutive days."""
    ranges: list[tuple[date, date]] = []
    for day in sorted(set(dates)):
        if ranges and day == ranges[-1][1] + timedelta(days=1):
            ranges[-1] = (ranges[-1][0], day)
        else:
            ranges.append((day, day))
    return ranges


def initialize(executor: SqlExecutor) -> None:
    """Create schemas and tables. Safe to run repeatedly."""
    for path in _sorted_sql(DDL_DIR):
        executor.run_file(path)


def raw_snapshot(executor: SqlExecutor) -> int | None:
    """Current Iceberg snapshot of raw. A run reads raw as of this snapshot only.

    A snapshot is an atomic, committed state of the table, so every step of the run
    (planning, staging, recording the build) sees exactly the same events, whatever
    the collector commits meanwhile.
    """
    _, rows = executor.query(
        'SELECT snapshot_id FROM iceberg.raw."trino_query_completed$snapshots" ORDER BY committed_at DESC LIMIT 1'
    )
    return int(rows[0][0]) if rows else None


@dataclass(frozen=True)
class PlannedDate:
    build_date: date
    reason: str
    events: int


def plan(executor: SqlExecutor, snapshot: int | None = None) -> list[PlannedDate]:
    """Dates that are not built, were built with older logic, or received new events."""
    snapshot = snapshot or raw_snapshot(executor)
    if snapshot is None:
        return []
    sql = render((OPS_DIR / "plan.sql").read_text(encoding="utf-8"),
                 {"raw_snapshot_id": snapshot, "logic_version": logic_version()})
    _, rows = executor.query(sql)
    return [PlannedDate(build_date=row[0], reason=row[1], events=int(row[2])) for row in rows]


def run(
    executor: SqlExecutor,
    start: date,
    end: date,
    chunk_days: int = 7,
    snapshot: int | None = None,
    refresh_after: bool = True,
) -> str:
    """Recompute every derived table for [start, end].

    Each incremental step replaces whole date partitions, so re-running the same
    range produces the same result. Backfills are the same call over a longer range.
    """
    snapshot = snapshot or raw_snapshot(executor)
    if snapshot is None:
        log.info("Raw table is empty; nothing to do")
        return ""
    run_id = uuid.uuid4().hex[:12]
    params = {"raw_snapshot_id": snapshot, "logic_version": logic_version(), "run_id": run_id}
    for chunk_start, chunk_end in date_chunks(start, end, chunk_days):
        log.info("Processing %s .. %s", chunk_start, chunk_end)
        window = {**params, "start_date": chunk_start.isoformat(), "end_date": chunk_end.isoformat()}
        # Forget the previous build first: if a step fails half-way, the planner must
        # see these dates as not built rather than as up to date.
        executor.execute(
            "DELETE FROM iceberg.ops.partition_builds "
            f"WHERE build_date BETWEEN DATE '{window['start_date']}' AND DATE '{window['end_date']}'"
        )
        for path in _sorted_sql(INCREMENTAL_DIR):
            executor.run_file(path, window)
        executor.run_file(OPS_DIR / "record_builds.sql", window)
    if refresh_after:
        refresh(executor, as_of=_latest_built_date(executor) or end)
    return run_id


def sync(executor: SqlExecutor, chunk_days: int = 7) -> list[PlannedDate]:
    """Rebuild exactly the dates the planner reports as missing or stale."""
    snapshot = raw_snapshot(executor)
    planned = plan(executor, snapshot)
    if not planned:
        log.info("Everything is up to date")
        return []
    for start, end in contiguous_ranges([p.build_date for p in planned]):
        run(executor, start, end, chunk_days=chunk_days, snapshot=snapshot, refresh_after=False)
    refresh(executor, as_of=_latest_built_date(executor) or planned[-1].build_date)
    return planned


def refresh(executor: SqlExecutor, as_of: date) -> None:
    log.info("Refreshing dimensions and profiles as of %s", as_of)
    for path in _sorted_sql(FULL_REFRESH_DIR):
        executor.run_file(path, {"as_of_date": as_of.isoformat()})


def _latest_built_date(executor: SqlExecutor) -> date | None:
    _, rows = executor.query("SELECT max(build_date) FROM iceberg.ops.partition_builds")
    return rows[0][0]


def record_run(
    executor: SqlExecutor,
    command: str,
    started: datetime,
    error: str | None,
    window: tuple[date, date] | None = None,
) -> None:
    finished = datetime.now(timezone.utc)
    values = [
        uuid.uuid4().hex[:12],
        command,
        window[0] if window else None,
        window[1] if window else None,
        logic_version(),
        "failed" if error else "succeeded",
        started,
        finished,
        (finished - started).total_seconds(),
        error[:2000] if error else None,
    ]
    executor.execute("INSERT INTO iceberg.ops.pipeline_runs VALUES (" + ", ".join(literal(v) for v in values) + ")")


@dataclass(frozen=True)
class MaintenanceResult:
    table: str
    files_before: int
    files_after: int


def maintain(executor: SqlExecutor, retention: str = "7d") -> list[MaintenanceResult]:
    """Compact small files and drop expired snapshots and orphan files."""
    schemas = ", ".join(literal(s) for s in MAINTAINED_SCHEMAS)
    _, tables = executor.query(
        "SELECT table_schema, table_name FROM iceberg.information_schema.tables "
        f"WHERE table_schema IN ({schemas}) AND table_type = 'BASE TABLE' ORDER BY 1, 2"
    )
    results = []
    for schema, table in tables:
        name = f"iceberg.{schema}.{table}"
        before = _file_count(executor, schema, table)
        started = time.monotonic()
        executor.execute(f"ALTER TABLE {name} EXECUTE optimize")
        executor.execute(f"ALTER TABLE {name} EXECUTE expire_snapshots(retention_threshold => '{retention}')")
        executor.execute(f"ALTER TABLE {name} EXECUTE remove_orphan_files(retention_threshold => '{retention}')")
        after = _file_count(executor, schema, table)
        log.info("%-40s files %5d -> %5d  %5.1fs", name, before, after, time.monotonic() - started)
        results.append(MaintenanceResult(name, before, after))
    return results


def _file_count(executor: SqlExecutor, schema: str, table: str) -> int:
    _, rows = executor.query(f'SELECT count(*) FROM iceberg.{schema}."{table}$files"')
    return int(rows[0][0])


@dataclass(frozen=True)
class CheckResult:
    name: str
    failures: int

    @property
    def passed(self) -> bool:
        return self.failures == 0


def run_checks(executor: SqlExecutor, start: date, end: date) -> list[CheckResult]:
    params = {"start_date": start.isoformat(), "end_date": end.isoformat()}
    results = []
    for path in _sorted_sql(CHECKS_DIR):
        sql = render(path.read_text(encoding="utf-8"), params).strip().rstrip(";")
        _, rows = executor.query(sql)
        results.append(CheckResult(name=path.stem, failures=int(rows[0][0])))
    return results
