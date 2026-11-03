from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from metadwh.config import SQL_DIR
from metadwh.sql_runner import SqlExecutor, render

log = logging.getLogger(__name__)

DDL_DIR = SQL_DIR / "ddl"
INCREMENTAL_DIR = SQL_DIR / "transform" / "incremental"
FULL_REFRESH_DIR = SQL_DIR / "transform" / "full"
CHECKS_DIR = SQL_DIR / "checks"


def _sorted_sql(directory: Path) -> list[Path]:
    return sorted(directory.glob("*.sql"))


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


def initialize(executor: SqlExecutor) -> None:
    """Create schemas and tables. Safe to run repeatedly."""
    for path in _sorted_sql(DDL_DIR):
        executor.run_file(path)


def run(executor: SqlExecutor, start: date, end: date, chunk_days: int = 7) -> None:
    """Recompute every derived table for [start, end].

    Each incremental step replaces whole date partitions, so re-running the same
    range produces the same result. Backfills are the same call over a longer range.
    """
    for chunk_start, chunk_end in date_chunks(start, end, chunk_days):
        log.info("Processing %s .. %s", chunk_start, chunk_end)
        params = {"start_date": chunk_start.isoformat(), "end_date": chunk_end.isoformat()}
        for path in _sorted_sql(INCREMENTAL_DIR):
            executor.run_file(path, params)

    log.info("Refreshing dimensions and profiles as of %s", end)
    for path in _sorted_sql(FULL_REFRESH_DIR):
        executor.run_file(path, {"as_of_date": end.isoformat()})


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
