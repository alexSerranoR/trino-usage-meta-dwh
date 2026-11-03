"""Rendering of Python values as Trino SQL literals."""

from __future__ import annotations

from datetime import datetime, timezone

from metadwh.demo.events import QueryEvent, TableInput

INPUT_ROW_TYPE = (
    "row(catalog_name varchar, schema_name varchar, table_name varchar, "
    "columns array(varchar), physical_input_bytes bigint, physical_input_rows bigint)"
)
OUTPUT_ROW_TYPE = "row(catalog_name varchar, schema_name varchar, table_name varchar)"


def literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.9e}"
    if isinstance(value, datetime):
        moment = value.astimezone(timezone.utc)
        return f"TIMESTAMP '{moment:%Y-%m-%d %H:%M:%S.%f} UTC'"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise TypeError(f"Unsupported literal type: {type(value).__name__}")


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
