from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from datetime import date, timedelta
from typing import Any

from metadwh import pipeline
from metadwh.config import SQL_DIR, TrinoSettings
from metadwh.sql_runner import SqlExecutor, render

DEMO_START = date(2026, 10, 1)
DEMO_END = date(2026, 10, 31)
QUERIES_DIR = SQL_DIR / "queries"
DEMO_QUERY_PARAMS = {
    "table_fqn": "iceberg.product_events.ide_sessions",
    "team_id": "product-analytics",
    "start_date": "2026-10-01",
    "end_date": "2026-10-31",
}


def _connect() -> SqlExecutor:
    import trino

    settings = TrinoSettings.from_env()
    connection = trino.dbapi.connect(
        host=settings.host,
        port=settings.port,
        user=settings.user,
        catalog=settings.catalog,
        http_scheme="http",
    )
    return SqlExecutor(connection)


def _resolve_window(args: argparse.Namespace) -> tuple[date, date]:
    if args.date:
        return args.date - timedelta(days=args.lookback_days), args.date
    return args.start, args.end


def _format_table(columns: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    cells = [[str(c) for c in columns]] + [["" if v is None else str(v) for v in row] for row in rows]
    widths = [max(len(row[i]) for row in cells) for i in range(len(columns))]
    lines = ["  ".join(value.ljust(widths[i]) for i, value in enumerate(row)) for row in cells]
    lines.insert(1, "  ".join("-" * w for w in widths))
    return "\n".join(lines)


def _print_query(executor: SqlExecutor, name: str, params: dict[str, str]) -> None:
    path = QUERIES_DIR / f"{name}.sql"
    sql = render(path.read_text(encoding="utf-8"), params).strip().rstrip(";")
    columns, rows = executor.query(sql)
    print(f"\n== {name} ==")
    print(_format_table(columns, rows))


def _print_checks(results: list[pipeline.CheckResult]) -> bool:
    for result in results:
        status = "PASS" if result.passed else f"FAIL ({result.failures})"
        print(f"{status:<12} {result.name}")
    return all(r.passed for r in results)


def cmd_init(args: argparse.Namespace) -> int:
    pipeline.initialize(_connect())
    return 0


def cmd_seed(args: argparse.Namespace) -> int:
    from metadwh.demo import loader

    executor = _connect()
    loader.load_reference(executor, args.start)
    loader.load_events(executor, args.start, args.end, args.seed, args.events_per_weekday)
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    start, end = _resolve_window(args)
    pipeline.run(_connect(), start, end, chunk_days=args.chunk_days)
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    start, end = _resolve_window(args)
    return 0 if _print_checks(pipeline.run_checks(_connect(), start, end)) else 1


def cmd_query(args: argparse.Namespace) -> int:
    if args.list or not args.name:
        for path in sorted(QUERIES_DIR.glob("*.sql")):
            print(path.stem)
        return 0
    params = dict(DEMO_QUERY_PARAMS)
    params.update(dict(p.split("=", 1) for p in args.param))
    _print_query(_connect(), args.name, params)
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    from metadwh.demo import loader

    executor = _connect()
    pipeline.initialize(executor)
    loader.load_reference(executor, DEMO_START)
    loader.load_events(executor, DEMO_START, DEMO_END, args.seed, args.events_per_weekday)
    pipeline.run(executor, DEMO_START, DEMO_END)
    passed = _print_checks(pipeline.run_checks(executor, DEMO_START, DEMO_END))
    for path in sorted(QUERIES_DIR.glob("*.sql")):
        _print_query(executor, path.stem, DEMO_QUERY_PARAMS)
    return 0 if passed else 1


def _add_window_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start", type=date.fromisoformat, default=DEMO_START, help="first date (inclusive)")
    parser.add_argument("--end", type=date.fromisoformat, default=DEMO_END, help="last date (inclusive)")
    parser.add_argument("--date", type=date.fromisoformat, help="scheduled run date; overrides --start/--end")
    parser.add_argument("--lookback-days", type=int, default=3, help="days reprocessed before --date for late events")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="metadwh", description="Trino usage Meta DWH")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="create schemas and tables").set_defaults(func=cmd_init)

    seed = sub.add_parser("seed", help="load demo reference data and synthetic raw events")
    seed.add_argument("--start", type=date.fromisoformat, default=DEMO_START)
    seed.add_argument("--end", type=date.fromisoformat, default=DEMO_END)
    seed.add_argument("--seed", type=int, default=42)
    seed.add_argument("--events-per-weekday", type=int, default=420)
    seed.set_defaults(func=cmd_seed)

    run = sub.add_parser("run", help="build staging, core and marts for a date window")
    _add_window_arguments(run)
    run.add_argument("--chunk-days", type=int, default=7, help="days processed per batch")
    run.set_defaults(func=cmd_run)

    check = sub.add_parser("check", help="run data quality checks for a date window")
    _add_window_arguments(check)
    check.set_defaults(func=cmd_check)

    query = sub.add_parser("query", help="run an example query from sql/queries")
    query.add_argument("name", nargs="?")
    query.add_argument("--param", action="append", default=[], help="override a parameter, e.g. team_id=finance")
    query.add_argument("--list", action="store_true")
    query.set_defaults(func=cmd_query)

    demo = sub.add_parser("demo", help="init, seed, run, check and print example queries")
    demo.add_argument("--seed", type=int, default=42)
    demo.add_argument("--events-per-weekday", type=int, default=420)
    demo.set_defaults(func=cmd_demo)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-5s %(message)s", datefmt="%H:%M:%S")
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
