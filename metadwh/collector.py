"""Receives QueryCompletedEvent JSON from the Trino HTTP event listener and appends it to raw.

The listener POSTs one JSON document per completed query. Events are buffered and
written in batches, so the collector issues one INSERT per flush instead of one per
query. Its own INSERTs produce events too; those are ignored to avoid a feedback loop.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from metadwh.raw_events import QueryEvent, TableInput, insert_events
from metadwh.sql_runner import SqlExecutor

log = logging.getLogger(__name__)

_DURATION_UNITS_MS = {"ns": 1e-6, "us": 1e-3, "ms": 1.0, "s": 1e3, "m": 6e4, "h": 3.6e6, "d": 8.64e7}
_AIRLIFT_DURATION = re.compile(r"^\s*([\d.]+)\s*(ns|us|ms|s|m|h|d)\s*$")
_ISO_DURATION = re.compile(r"^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:([\d.]+)S)?)?$")


_FRACTION = re.compile(r"\.(\d+)")
_DECIMAL_SECONDS = re.compile(r"^\s*\d+(\.\d+)?\s*$")


def duration_ms(value: Any) -> int | None:
    """Parse a Trino duration.

    Trino 476 serializes durations as decimal seconds ('0.757000000'); older versions
    and other listeners use airlift text ('1.50s') or ISO-8601 ('PT1.5S').
    """
    if value is None:
        return None
    if isinstance(value, (int, float, Decimal)):
        return round(float(value) * 1000)
    text = str(value)
    if _DECIMAL_SECONDS.match(text):
        return round(float(text) * 1000)
    if match := _AIRLIFT_DURATION.match(text):
        return round(float(match.group(1)) * _DURATION_UNITS_MS[match.group(2)])
    if match := _ISO_DURATION.match(text):
        days, hours, minutes, seconds = (float(g) if g else 0.0 for g in match.groups())
        return round((((days * 24 + hours) * 60 + minutes) * 60 + seconds) * 1000)
    raise ValueError(f"Unrecognized duration: {value!r}")


def instant(value: Any) -> datetime | None:
    """Parse a Trino instant: ISO-8601 text or epoch seconds with a fractional part."""
    if value is None:
        return None
    if isinstance(value, (int, float, Decimal)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    text = str(value).replace("Z", "+00:00")
    # Python 3.10 only parses fractions of exactly 3 or 6 digits.
    text = _FRACTION.sub(lambda m: "." + m.group(1).ljust(6, "0")[:6], text)
    return datetime.fromisoformat(text).astimezone(timezone.utc)


def _int(value: Any) -> int | None:
    return None if value is None else int(value)


def _column_name(column: Any) -> str:
    # Older Trino versions list column names; newer ones list objects with a name.
    if isinstance(column, Mapping):
        return str(column.get("column") or column.get("name"))
    return str(column)


def parse_event(payload: Mapping[str, Any], ingested_at: datetime) -> QueryEvent:
    metadata = payload["metadata"]
    context = payload.get("context") or {}
    statistics = payload.get("statistics") or {}
    io = payload.get("ioMetadata") or {}
    failure = payload.get("failureInfo") or {}

    inputs = [
        TableInput(
            catalog_name=item["catalogName"],
            schema_name=item["schema"],
            table_name=item["table"],
            columns=[_column_name(c) for c in item.get("columns") or []],
            physical_input_bytes=_int(item.get("physicalInputBytes")),
            physical_input_rows=_int(item.get("physicalInputRows")),
        )
        for item in io.get("inputs") or []
    ]
    output = io.get("output")
    error_code = (failure.get("errorCode") or {}).get("name") if failure else None

    return QueryEvent(
        query_id=metadata["queryId"],
        query_state=metadata.get("queryState", "FINISHED"),
        query_type=context.get("queryType"),
        error_code=error_code,
        query=metadata.get("query", ""),
        user_name=context.get("user", ""),
        source=context.get("source"),
        session_catalog=context.get("catalog"),
        session_schema=context.get("schema"),
        create_time=instant(payload["createTime"]),
        execution_start_time=instant(payload.get("executionStartTime")),
        end_time=instant(payload["endTime"]),
        cpu_time_ms=duration_ms(statistics.get("cpuTime")) or 0,
        wall_time_ms=duration_ms(statistics.get("wallTime")) or 0,
        queued_time_ms=duration_ms(statistics.get("queuedTime")) or 0,
        peak_user_memory_bytes=_int(statistics.get("peakUserMemoryBytes")) or 0,
        cumulative_memory=float(statistics.get("cumulativeMemory") or 0.0),
        physical_input_bytes=_int(statistics.get("physicalInputBytes")) or 0,
        physical_input_rows=_int(statistics.get("physicalInputRows")) or 0,
        output_bytes=_int(statistics.get("outputBytes")) or 0,
        output_rows=_int(statistics.get("outputRows")) or 0,
        inputs=inputs,
        output=(output["catalogName"], output["schema"], output["table"]) if output else None,
        ingested_at=ingested_at,
    )


def replay(executor: SqlExecutor, archive: Path, ignored_users: set[str]) -> int:
    """Load every payload from a collector archive into raw, e.g. after a failed write.

    Re-loading events that are already in raw is safe: staging keeps one row per query_id.
    Lines that cannot be parsed are skipped and reported instead of aborting the replay.
    """
    ingested_at = datetime.now(timezone.utc)
    events, skipped = [], 0
    with archive.open(encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                event = parse_event(json.loads(line, parse_float=Decimal), ingested_at)
            except Exception as error:
                skipped += 1
                log.warning("Skipping line %d of %s: %s", number, archive, error)
                continue
            if event.user_name not in ignored_users:
                events.append(event)
    insert_events(executor, events)
    log.info("Replayed %d events from %s (%d lines skipped)", len(events), archive, skipped)
    return len(events)


class EventBuffer:
    """Thread-safe buffer flushed to raw on a timer or when it reaches max_size.

    A batch that keeps failing is dropped after max_attempts and logged; every accepted
    payload is also in the archive, so it can be recovered with replay.
    """

    def __init__(self, executor: SqlExecutor, max_size: int = 200, max_attempts: int = 3) -> None:
        self._executor = executor
        self._max_size = max_size
        self._max_attempts = max_attempts
        self._events: list[QueryEvent] = []
        self._failures = 0
        self._lock = threading.Lock()
        self._flush_lock = threading.Lock()

    def add(self, event: QueryEvent) -> None:
        with self._lock:
            self._events.append(event)
            full = len(self._events) >= self._max_size
        if full:
            self.flush()

    def flush(self) -> int:
        with self._flush_lock:
            with self._lock:
                events, self._events = self._events, []
            if not events:
                return 0
            try:
                insert_events(self._executor, events)
            except Exception:
                self._failures += 1
                if self._failures >= self._max_attempts:
                    log.exception(
                        "Dropping %d events after %d failed writes; replay the archive to recover them",
                        len(events), self._failures,
                    )
                    self._failures = 0
                    return 0
                with self._lock:
                    self._events = events + self._events
                raise
            self._failures = 0
            log.info("Wrote %d events to raw", len(events))
            return len(events)


def serve(
    executor: SqlExecutor,
    port: int,
    ignored_users: set[str],
    archive: Path | None,
    flush_seconds: float = 5.0,
) -> None:
    buffer = EventBuffer(executor)
    archive_lock = threading.Lock()

    def accept(payload: Mapping[str, Any]) -> None:
        event = parse_event(payload, ingested_at=datetime.now(timezone.utc))
        if event.user_name in ignored_users:
            return
        if archive:
            with archive_lock, archive.open("ab") as handle:
                handle.write(json.dumps(payload, default=str).encode() + b"\n")
        buffer.add(event)

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
            try:
                payload = json.loads(body, parse_float=Decimal)
                for item in payload if isinstance(payload, list) else [payload]:
                    accept(item)
            except Exception:
                log.exception("Rejected event payload")
                self.send_response(400)
                self.end_headers()
                return
            self.send_response(200)
            self.end_headers()

        def log_message(self, message: str, *args: Any) -> None:
            log.debug(message, *args)

    def flush_periodically() -> None:
        while True:
            time.sleep(flush_seconds)
            try:
                buffer.flush()
            except Exception:
                log.exception("Failed to write events; they will be retried on the next flush")

    threading.Thread(target=flush_periodically, daemon=True).start()
    log.info("Collecting Trino query events on port %d (ignoring users: %s)", port, ", ".join(sorted(ignored_users)))
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
