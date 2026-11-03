"""Query completed events and their representation as rows of the raw table."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime

from metadwh.literals import literal
from metadwh.sql_runner import SqlExecutor

RAW_TABLE = "iceberg.raw.trino_query_completed"
_BATCH_ROWS = 250
_BATCH_CHARS = 800_000


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
    query_type: str | None
    error_code: str | None
    query: str
    user_name: str
    source: str | None
    session_catalog: str | None
    session_schema: str | None
    create_time: datetime
    execution_start_time: datetime | None
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


INPUT_ROW_TYPE = (
    "row(catalog_name varchar, schema_name varchar, table_name varchar, "
    "columns array(varchar), physical_input_bytes bigint, physical_input_rows bigint)"
)
OUTPUT_ROW_TYPE = "row(catalog_name varchar, schema_name varchar, table_name varchar)"


def varchar_array(values: list[str]) -> str:
    if not values:
        return "CAST(ARRAY[] AS array(varchar))"
    return "ARRAY[" + ", ".join(literal(v) for v in values) + "]"


def _input_row(table_input: TableInput) -> str:
    fields = ", ".join([
        literal(table_input.catalog_name),
        literal(table_input.schema_name),
        literal(table_input.table_name),
        varchar_array(table_input.columns),
        literal(table_input.physical_input_bytes),
        literal(table_input.physical_input_rows),
    ])
    return f"CAST(ROW({fields}) AS {INPUT_ROW_TYPE})"


def event_values(event: QueryEvent) -> str:
    inputs = (
        "ARRAY[" + ", ".join(_input_row(i) for i in event.inputs) + "]"
        if event.inputs
        else f"CAST(ARRAY[] AS array({INPUT_ROW_TYPE}))"
    )
    output = (
        f"CAST(ROW({', '.join(literal(part) for part in event.output)}) AS {OUTPUT_ROW_TYPE})"
        if event.output
        else f"CAST(NULL AS {OUTPUT_ROW_TYPE})"
    )
    values = [
        literal(event.query_id),
        literal(event.query_state),
        literal(event.query_type),
        literal(event.error_code),
        literal(event.query),
        literal(event.user_name),
        literal(event.source),
        literal(event.session_catalog),
        literal(event.session_schema),
        literal(event.create_time),
        literal(event.execution_start_time),
        literal(event.end_time),
        literal(event.cpu_time_ms),
        literal(event.wall_time_ms),
        literal(event.queued_time_ms),
        literal(event.peak_user_memory_bytes),
        literal(event.cumulative_memory),
        literal(event.physical_input_bytes),
        literal(event.physical_input_rows),
        literal(event.output_bytes),
        literal(event.output_rows),
        inputs,
        output,
        literal(event.ingested_at),
    ]
    return "(" + ", ".join(values) + ")"


def insert_rows(executor: SqlExecutor, table: str, rows: Sequence[str]) -> None:
    """INSERT rows in batches bounded by row count and by statement size.

    Trino rejects statements longer than query.max-length (1,000,000 characters by
    default), and event rows carry full query texts, so batches are cut by size too.
    """
    batch: list[str] = []
    size = 0
    for row in rows:
        if batch and (len(batch) >= _BATCH_ROWS or size + len(row) > _BATCH_CHARS):
            executor.execute(f"INSERT INTO {table} VALUES " + ",\n".join(batch))
            batch, size = [], 0
        batch.append(row)
        size += len(row) + 2
    if batch:
        executor.execute(f"INSERT INTO {table} VALUES " + ",\n".join(batch))


def insert_events(executor: SqlExecutor, events: Sequence[QueryEvent]) -> None:
    insert_rows(executor, RAW_TABLE, [event_values(e) for e in events])
