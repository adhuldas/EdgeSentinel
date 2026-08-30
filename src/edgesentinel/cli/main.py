"""``edgesentinel`` CLI entry point.

A synchronous ``argparse`` front end over the async diagnostics APIs
(:class:`~edgesentinel.persistence.database.Database`,
:class:`~edgesentinel.diagnostics.timeline.EventLog`,
:func:`~edgesentinel.diagnostics.incidents.build_incidents`) -- the CLI wraps a
single :func:`asyncio.run` call per invocation rather than requiring callers
to already be inside an event loop.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Sequence
from pathlib import Path

from edgesentinel.core.events import Severity
from edgesentinel.diagnostics.incidents import build_incidents
from edgesentinel.diagnostics.report import format_incidents, format_timeline, summarize_incidents
from edgesentinel.diagnostics.timeline import EventLog
from edgesentinel.persistence.database import Database

#: Upper bound on events replayed to reconstruct incidents offline. Higher
#: than a typical `timeline` query's default limit since a single incident
#: can span many events and truncating mid-incident would misreport it.
_INCIDENT_REPLAY_LIMIT = 5000


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level ``edgesentinel`` argument parser."""
    parser = argparse.ArgumentParser(
        prog="edgesentinel",
        description=(
            "Inspect an edgesentinel runtime's local state. Reads the runtime's "
            "on-disk SQLite database directly; the runtime doesn't need to be "
            "running."
        ),
    )
    parser.add_argument(
        "--name",
        required=True,
        help="Runtime name -- the same value passed to EdgeSentinel(name, ...).",
    )
    parser.add_argument(
        "--data-dir",
        default="./data",
        type=Path,
        help="Directory holding the runtime's SQLite database (default: ./data).",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("status", help="Show the runtime's last-persisted lifecycle state.")

    timeline_parser = subparsers.add_parser("timeline", help="Show recorded events.")
    timeline_parser.add_argument("--component", help="Only events from this component.")
    timeline_parser.add_argument("--type", help="Only events of this type.")
    timeline_parser.add_argument(
        "--min-severity",
        choices=[s.value for s in Severity],
        help="Only events at this severity or above.",
    )
    timeline_parser.add_argument(
        "--limit", type=int, default=50, help="Maximum number of events to show (default: 50)."
    )
    timeline_parser.add_argument(
        "--oldest-first",
        action="store_true",
        help="Show the oldest matching events first instead of the newest.",
    )

    subparsers.add_parser(
        "incidents", help="Show incidents (spans of non-HEALTHY time) derived from the timeline."
    )

    return parser


async def _cmd_status(database: Database) -> int:
    row = await database.load_runtime_state()
    if row is None:
        print("no runtime state recorded yet")
        return 0
    print(f"{row['name']}: {row['state']} (as of {row['updated_at']})")
    return 0


async def _cmd_timeline(database: Database, args: argparse.Namespace) -> int:
    log = EventLog(database)
    min_severity = Severity(args.min_severity) if args.min_severity else None
    events = await log.query(
        component=args.component,
        type=args.type,
        min_severity=min_severity,
        limit=args.limit,
        newest_first=not args.oldest_first,
    )
    print(format_timeline(events))
    return 0


async def _cmd_incidents(database: Database) -> int:
    log = EventLog(database)
    events = await log.query(newest_first=False, limit=_INCIDENT_REPLAY_LIMIT)
    incidents = build_incidents(events)
    print(format_incidents(incidents))
    print()
    print(summarize_incidents(incidents))
    return 0


async def _dispatch(args: argparse.Namespace) -> int:
    db_path = Path(args.data_dir) / f"{args.name}.sqlite3"
    if not db_path.exists():
        print(f"error: no database found at {db_path}", file=sys.stderr)
        return 1

    database = Database(db_path)
    await database.connect()
    try:
        if args.command == "status":
            return await _cmd_status(database)
        if args.command == "timeline":
            return await _cmd_timeline(database, args)
        if args.command == "incidents":
            return await _cmd_incidents(database)
        raise AssertionError(f"unhandled command {args.command!r}")  # argparse guarantees a match
    finally:
        await database.close()


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point. Returns a process exit code; does not raise for
    ordinary usage errors (missing database, bad arguments)."""
    parser = build_parser()
    args = parser.parse_args(argv)
    return asyncio.run(_dispatch(args))


if __name__ == "__main__":
    sys.exit(main())
