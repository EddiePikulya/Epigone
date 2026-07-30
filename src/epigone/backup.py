"""Retention for the pre-deploy database dumps (issue #160).

Stdlib only, and deliberately runnable as a bare script: the deploy calls it on
the *host*, where there is a python3 but no uv, no venv and no installed
epigone package (`python3 src/epigone/backup.py prune <dir> --keep 3`). Nothing
in here may import from the rest of the package.

That host interpreter is not ours to pick either — it is whatever the server's
distro ships — so this module stays inside 3.9-era syntax (`timezone.utc`, not
3.11's `UTC`; postponed annotations for the `X | None` hints) while the rest of
the codebase targets 3.12. The failure mode it avoids is a deploy that aborts
at the dump step on a server that is otherwise fine.
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

DUMP_PREFIX = "epigone-"
DUMP_SUFFIX = ".dump"
STAMP_FORMAT = "%Y%m%dT%H%M%SZ"
# `epigone-20260730T120000Z.dump` — a fixed-width UTC stamp, so lexicographic
# order *is* chronological order and recency needs no filesystem metadata.
DUMP_NAME = re.compile(rf"^{re.escape(DUMP_PREFIX)}\d{{8}}T\d{{6}}Z{re.escape(DUMP_SUFFIX)}$")


def dump_name(now: datetime | None = None) -> str:
    """The filename a deploy's dump gets. The deploy calls this rather than
    formatting a date itself, so exactly one place defines the convention."""
    when = now or datetime.now(tz=timezone.utc)
    return f"{DUMP_PREFIX}{when.astimezone(timezone.utc).strftime(STAMP_FORMAT)}{DUMP_SUFFIX}"


def dumps_in(directory: Path) -> list[Path]:
    """Every dump this project wrote, oldest first.

    Matching is strict rather than a `epigone-*.dump` glob: retention deletes
    files, so it may only consider names it is certain it produced itself.
    """
    return sorted(p for p in directory.iterdir() if p.is_file() and DUMP_NAME.match(p.name))


def prune_dumps(directory: Path, keep: int) -> list[Path]:
    """Delete all but the `keep` most recent dumps; return what was deleted."""
    if keep < 1:
        raise ValueError(f"retention count must keep at least one dump, got {keep}")
    dumps = dumps_in(directory)
    doomed = dumps[: max(len(dumps) - keep, 0)]
    for path in doomed:
        path.unlink()
    return doomed


def main(argv: list[str] | None = None) -> int:
    """The deploy's entry point (`scripts/deploy.sh`)."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("name", help="print the filename this deploy's dump should get")
    prune = commands.add_parser("prune", help="delete all but the most recent dumps")
    prune.add_argument("directory", type=Path, help="the dump directory")
    prune.add_argument("--keep", type=int, required=True, help="how many dumps to retain")
    args = parser.parse_args(argv)

    if args.command == "name":
        print(dump_name())
        return 0

    for path in prune_dumps(args.directory, args.keep):
        print(f"pruned {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
