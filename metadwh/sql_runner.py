from __future__ import annotations

import logging
import re
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from string import Template
from typing import Any, Protocol

log = logging.getLogger(__name__)

_STATEMENT_END = re.compile(r";[ \t]*(?:\r?\n|$)")
_LINE_COMMENT = re.compile(r"^\s*--.*$", re.MULTILINE)


class Connection(Protocol):
    def cursor(self) -> Any: ...


def render(sql: str, params: Mapping[str, object]) -> str:
    """Substitute ${name} placeholders. Missing parameters raise KeyError."""
    return Template(sql).substitute({key: str(value) for key, value in params.items()})


def split_statements(sql: str) -> list[str]:
    """Split a script on statement-terminating semicolons, dropping comment-only chunks."""
    statements = []
    for chunk in _STATEMENT_END.split(sql):
        if _LINE_COMMENT.sub("", chunk).strip():
            statements.append(chunk.strip())
    return statements


class SqlExecutor:
    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def query(self, statement: str) -> tuple[list[str], list[Sequence[Any]]]:
        cursor = self._connection.cursor()
        cursor.execute(statement)
        rows = cursor.fetchall()
        columns = [column[0] for column in cursor.description or []]
        return columns, rows

    def execute(self, statement: str) -> None:
        cursor = self._connection.cursor()
        cursor.execute(statement)
        cursor.fetchall()

    def run_file(self, path: Path, params: Mapping[str, object] | None = None) -> None:
        script = render(path.read_text(encoding="utf-8"), params or {})
        started = time.monotonic()
        for statement in split_statements(script):
            self.execute(statement)
        log.info("%-45s %6.1fs", path.name, time.monotonic() - started)
