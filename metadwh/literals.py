"""Rendering of Python values as Trino SQL literals."""

from __future__ import annotations

from datetime import date, datetime, timezone


def timestamp_utc(value: datetime) -> str:
    """Timestamp text accepted inside a Trino TIMESTAMP '...' literal."""
    return f"{value.astimezone(timezone.utc):%Y-%m-%d %H:%M:%S.%f} UTC"


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
        return f"TIMESTAMP '{timestamp_utc(value)}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    raise TypeError(f"Unsupported literal type: {type(value).__name__}")
