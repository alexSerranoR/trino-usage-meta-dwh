from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

SQL_DIR = Path(os.environ.get("METADWH_SQL_DIR", Path(__file__).resolve().parent.parent / "sql"))


@dataclass(frozen=True)
class TrinoSettings:
    host: str = "localhost"
    port: int = 8080
    user: str = "meta_dwh_pipeline"
    catalog: str = "iceberg"

    @classmethod
    def from_env(cls) -> TrinoSettings:
        return cls(
            host=os.environ.get("TRINO_HOST", cls.host),
            port=int(os.environ.get("TRINO_PORT", cls.port)),
            user=os.environ.get("TRINO_USER", cls.user),
            catalog=os.environ.get("TRINO_CATALOG", cls.catalog),
        )
