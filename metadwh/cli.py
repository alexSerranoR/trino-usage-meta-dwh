from __future__ import annotations

import argparse
import logging
import sys
from collections import Counter
from collections.abc import Callable, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from metadwh import pipeline
from metadwh.config import SQL_DIR, TrinoSettings
from metadwh.sql_runner import SqlExecutor, render

DEMO_START = date(2026, 10, 1)
DEMO_END = date(2026, 10, 31)
LATE_EVENTS_DATE = date(2026, 10, 15)
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


def _print_plan(planned: list[pipeline.PlannedDate]) -> None:
    if not planned:
        print("Plan: everything is up to date")
        return
    print(f"Plan: {len(planned)} date(s) to rebuild")
    if len(planned) > 10:
        reasons = Counter(p.reason for p in planned)
        for reason, count in sorted(reasons.items()):
            print(f"  {reason:<14} {count} date(s), {planned[0].build_date} .. {planned[-1].build_date}")
        return
    for item in planned:
        print(f"  {item.build_date}  {item.reason:<14} {item.events} raw events")


def _print_maintenance(results: list[pipeline.MaintenanceResult]) -> None:
    print(_format_table(
        ["table", "files_before", "files_after"],
        [(r.table, r.files_before, r.files_after) for r in results],
    ))


def _audited(
    executor: SqlExecutor,
    command: str,
    action: Callable[[], Any],
    window: tuple[date, date] | None = None,
) -> Any:
    """Run an action and record its outcome in ops.pipeline_runs."""
    started = datetime.now(timezone.utc)
    try:
        result = action()
    except Exception as error:
        pipeline.record_run(executor, command, started, error=f"{type(error).__name__}: {error}", window=window)
        raise
    pipeline.record_run(executor, command, started, error=None, window=window)
    return result


def cmd_init(args: argparse.Namespace) -> int:
    pipeline.initialize(_connect())
    return 0


def cmd_seed(args: argparse.Namespace) -> int:
    from metadwh.demo import loader

    executor = _connect()
    loader.load_reference(executor, args.start)
    loader.load_events(executor, args.start, args.end, args.seed, args.events_per_weekday)
    return 0


def cmd_late_events(args: argparse.Namespace) -> int:
    from metadwh.demo import loader

    loader.load_late_events(_connect(), args.date, args.count)
    return 0


def cmd_plan(args: argparse.Namespace) -> int:
    _print_plan(pipeline.plan(_connect()))
    return 0


def cmd_sync(args: argparse.Namespace) -> int:
    executor = _connect()
    _audited(executor, "sync", lambda: pipeline.sync(executor, chunk_days=args.chunk_days))
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    executor = _connect()
    start, end = _resolve_window(args)
    _audited(executor, "run", lambda: pipeline.run(executor, start, end, chunk_days=args.chunk_days), (start, end))
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    start, end = _resolve_window(args)
    return 0 if _print_checks(pipeline.run_checks(_connect(), start, end)) else 1


def cmd_maintain(args: argparse.Namespace) -> int:
    executor = _connect()
    _print_maintenance(_audited(executor, "maintain", lambda: pipeline.maintain(executor, args.retention)))
    return 0


def cmd_collect(args: argparse.Namespace) -> int:
    from metadwh import collector

    ignored = {user.strip() for user in args.ignore_users.split(",") if user.strip()}
    archive = Path(args.archive) if args.archive else None
    collector.serve(_connect(), args.port, ignored, archive, flush_seconds=args.flush_seconds)
    return 0


def cmd_replay(args: argparse.Namespace) -> int:
    from metadwh import collector

    ignored = {user.strip() for user in args.ignore_users.split(",") if user.strip()}
    collector.replay(_connect(), Path(args.archive), ignored)
    return 0


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

    print("\n== Initial build ==")
    _print_plan(pipeline.plan(executor))
    _audited(executor, "sync", lambda: pipeline.sync(executor))
    passed = _print_checks(pipeline.run_checks(executor, DEMO_START, DEMO_END))

    print("\n== Late events arrive for one past day ==")
    loader.load_late_events(executor, LATE_EVENTS_DATE, count=60)
    _print_plan(pipeline.plan(executor))
    _audited(executor, "sync", lambda: pipeline.sync(executor))
    _print_plan(pipeline.plan(executor))
    passed = _print_checks(pipeline.run_checks(executor, DEMO_START, DEMO_END)) and passed

    print("\n== Table maintenance ==")
    _print_maintenance(_audited(executor, "maintain", lambda: pipeline.maintain(executor)))

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

    run = sub.add_parser("run", help="rebuild a fixed date window (manual backfill)")
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

    late = sub.add_parser("late-events", help="append demo events for a past day that arrive now")
    late.add_argument("--date", type=date.fromisoformat, default=LATE_EVENTS_DATE)
    late.add_argument("--count", type=int, default=60)
    late.set_defaults(func=cmd_late_events)

    sub.add_parser("plan", help="show which dates are missing or stale and why").set_defaults(func=cmd_plan)

    sync = sub.add_parser("sync", help="rebuild exactly the dates reported by plan")
    sync.add_argument("--chunk-days", type=int, default=7, help="days processed per batch")
    sync.set_defaults(func=cmd_sync)

    maintain = sub.add_parser("maintain", help="compact files and expire old snapshots")
    maintain.add_argument("--retention", default="7d", help="snapshot and orphan file retention")
    maintain.set_defaults(func=cmd_maintain)

    collect = sub.add_parser("collect", help="receive events from the Trino HTTP event listener into raw")
    collect.add_argument("--port", type=int, default=8000)
    collect.add_argument("--ignore-users", default="meta_dwh_collector,healthcheck",
                         help="comma-separated users whose queries are not recorded")
    collect.add_argument("--archive", help="optional JSON Lines file that keeps every received payload")
    collect.add_argument("--flush-seconds", type=float, default=5.0)
    collect.set_defaults(func=cmd_collect)

    replay = sub.add_parser("replay", help="load a collector archive into raw")
    replay.add_argument("archive")
    replay.add_argument("--ignore-users", default="meta_dwh_collector,healthcheck")
    replay.set_defaults(func=cmd_replay)

    demo = sub.add_parser("demo", help="end-to-end demonstration on synthetic events")
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
