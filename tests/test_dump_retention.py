"""Retention for pre-deploy database dumps (issue #160): keep the most recent
few, delete the rest, and never touch anything that isn't ours.

The deploy takes a dump before the containers come up, so the dump directory
would grow one ~34 MB file per deploy forever without this — the state the
ticket found in production (19 files, 2.1 GB). The pruning is the one piece of
the mechanism with real behaviour, so it lives in Python and is tested here;
the rest of the deploy is shell around `pg_dump` and `docker compose`.
"""

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from epigone.backup import dump_name, dumps_in, prune_dumps


def _touch_dumps(directory: Path, *stamps: str) -> None:
    """Create dump files for the given UTC stamps, in the order given — which is
    deliberately not chronological order in these tests. Recency comes from the
    name, not from mtime: a dump copied off the server for a restore rehearsal
    gets a fresh mtime without becoming a newer backup.
    """
    for stamp in stamps:
        (directory / f"epigone-{stamp}.dump").write_bytes(b"PGDMP")


def test_prunes_all_but_the_most_recent(tmp_path: Path) -> None:
    _touch_dumps(
        tmp_path,
        "20260728T060000Z",
        "20260730T120000Z",
        "20260711T183500Z",
        "20260729T193200Z",
        "20260725T061100Z",
    )

    removed = prune_dumps(tmp_path, keep=3)

    assert sorted(p.name for p in tmp_path.iterdir()) == [
        "epigone-20260728T060000Z.dump",
        "epigone-20260729T193200Z.dump",
        "epigone-20260730T120000Z.dump",
    ]
    assert sorted(p.name for p in removed) == [
        "epigone-20260711T183500Z.dump",
        "epigone-20260725T061100Z.dump",
    ]


def test_keeps_everything_below_the_retention_count(tmp_path: Path) -> None:
    _touch_dumps(tmp_path, "20260730T120000Z", "20260729T193200Z")

    assert prune_dumps(tmp_path, keep=3) == []
    assert len(list(tmp_path.iterdir())) == 2


def test_keeps_everything_at_exactly_the_retention_count(tmp_path: Path) -> None:
    _touch_dumps(tmp_path, "20260730T120000Z", "20260729T193200Z", "20260728T060000Z")

    assert prune_dumps(tmp_path, keep=3) == []
    assert len(list(tmp_path.iterdir())) == 3


def test_leaves_files_it_did_not_write(tmp_path: Path) -> None:
    """Retention only ever deletes files it can positively identify as its own
    dumps. Anything else — the hand-made dumps from before this existed, an
    operator's scratch copy, a half-written dump a killed deploy left behind —
    is somebody else's, and disk pressure is not a reason to guess.
    """
    _touch_dumps(tmp_path, "20260730T120000Z", "20260729T193200Z", "20260711T183500Z")
    bystanders = [
        "epigone_pre_0023.sql",  # the pre-#160 hand-run convention
        "epigone-before-the-0028-backfill.dump",  # our prefix, no timestamp
        "epigone-20260730T140000Z.dump.partial",  # a dump that never finished
        "notes.txt",
    ]
    for name in bystanders:
        (tmp_path / name).write_bytes(b"not a dump of ours")

    removed = prune_dumps(tmp_path, keep=2)

    assert [p.name for p in removed] == ["epigone-20260711T183500Z.dump"]
    assert sorted(p.name for p in tmp_path.iterdir()) == sorted(
        ["epigone-20260730T120000Z.dump", "epigone-20260729T193200Z.dump", *bystanders]
    )


def test_refuses_a_retention_count_that_would_keep_nothing(tmp_path: Path) -> None:
    """The retention count comes from the deploy environment, so a zero — a
    typo, or an unset variable read as empty — must fail loudly. Silently
    deleting the dump taken seconds earlier is the exact outcome the ticket
    exists to prevent, and it would look like a successful deploy.
    """
    _touch_dumps(tmp_path, "20260730T120000Z", "20260729T193200Z")

    with pytest.raises(ValueError):
        prune_dumps(tmp_path, keep=0)

    assert len(list(tmp_path.iterdir())) == 2


def test_the_name_a_deploy_writes_is_a_name_retention_recognises(tmp_path: Path) -> None:
    """The one seam between the shell and this module. If the deploy's filename
    convention and the pattern retention matches ever drift apart, pruning
    quietly stops recognising anything and the directory grows forever — the
    production state this ticket found. So the deploy asks *here* for the name.
    """
    name = dump_name(datetime(2026, 7, 30, 12, 0, 0, tzinfo=UTC))

    assert name == "epigone-20260730T120000Z.dump"

    (tmp_path / name).write_bytes(b"PGDMP")
    assert [p.name for p in dumps_in(tmp_path)] == [name]


def test_the_name_is_stamped_in_utc(tmp_path: Path) -> None:
    """A server on local time would otherwise write names that sort wrong across
    a DST boundary, and sort order is what recency means here."""
    kyiv_afternoon = datetime(2026, 7, 30, 15, 0, 0, tzinfo=timezone(timedelta(hours=3)))

    assert dump_name(kyiv_afternoon) == "epigone-20260730T120000Z.dump"
