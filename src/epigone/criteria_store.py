"""Saved Criteria persistence (issue #7): a User's named definitions of
"best", surviving restarts. The criteria table stores the sort and timeframe
as columns and the filters as JSONB, with thresholds as strings so Decimals
round-trip exactly. All access is scoped to the owning User — a forged
callback with someone else's id loads nothing."""

import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import asyncpg

from epigone.gateway import Window
from epigone.screener import Criteria, Filter, Op


@dataclass(frozen=True)
class SavedCriteria:
    id: int
    name: str
    criteria: Criteria


def _filters_to_json(filters: tuple[Filter, ...]) -> str:
    return json.dumps(
        [{"metric": f.metric, "op": f.op.value, "threshold": str(f.threshold)} for f in filters]
    )


def _saved(row: asyncpg.Record) -> SavedCriteria:
    filters = tuple(
        Filter(metric=f["metric"], op=Op(f["op"]), threshold=Decimal(f["threshold"]))
        for f in json.loads(row["filters"])
    )
    return SavedCriteria(
        id=row["id"],
        name=row["name"],
        criteria=Criteria(
            filters=filters,
            time_window=Window(row["time_window"]),
            sort_key=row["sort_key"],
            sort_desc=row["sort_desc"],
        ),
    )


async def save_criteria(
    pool: asyncpg.Pool, user_id: int, name: str, criteria: Criteria, now: datetime
) -> int:
    """Save under a name; saving the same name again replaces the contents."""
    saved_id = await pool.fetchval(
        """
        INSERT INTO criteria
            (user_telegram_id, name, time_window, sort_key, sort_desc, filters,
             created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $7)
        ON CONFLICT (user_telegram_id, name) DO UPDATE
            SET time_window = EXCLUDED.time_window, sort_key = EXCLUDED.sort_key,
                sort_desc = EXCLUDED.sort_desc, filters = EXCLUDED.filters, updated_at = $7
        RETURNING id
        """,
        user_id,
        name,
        criteria.time_window.value,
        criteria.sort_key,
        criteria.sort_desc,
        _filters_to_json(criteria.filters),
        now,
    )
    assert isinstance(saved_id, int)
    return saved_id


async def update_criteria(
    pool: asyncpg.Pool, user_id: int, criteria_id: int, criteria: Criteria, now: datetime
) -> bool:
    """Edit in place, keeping the name. False when the row is already gone."""
    status = await pool.execute(
        """
        UPDATE criteria
        SET time_window = $3, sort_key = $4, sort_desc = $5, filters = $6::jsonb,
            updated_at = $7
        WHERE user_telegram_id = $1 AND id = $2
        """,
        user_id,
        criteria_id,
        criteria.time_window.value,
        criteria.sort_key,
        criteria.sort_desc,
        _filters_to_json(criteria.filters),
        now,
    )
    return bool(status != "UPDATE 0")


async def list_criteria(pool: asyncpg.Pool, user_id: int) -> list[SavedCriteria]:
    rows = await pool.fetch(
        "SELECT id, name, time_window, sort_key, sort_desc, filters FROM criteria "
        "WHERE user_telegram_id = $1 ORDER BY created_at, id",
        user_id,
    )
    return [_saved(row) for row in rows]


async def get_criteria(pool: asyncpg.Pool, user_id: int, criteria_id: int) -> SavedCriteria | None:
    row = await pool.fetchrow(
        "SELECT id, name, time_window, sort_key, sort_desc, filters FROM criteria "
        "WHERE user_telegram_id = $1 AND id = $2",
        user_id,
        criteria_id,
    )
    return _saved(row) if row is not None else None


async def delete_criteria(pool: asyncpg.Pool, user_id: int, criteria_id: int) -> str | None:
    """Delete and return the name, or None when it was already gone."""
    name = await pool.fetchval(
        "DELETE FROM criteria WHERE user_telegram_id = $1 AND id = $2 RETURNING name",
        user_id,
        criteria_id,
    )
    assert name is None or isinstance(name, str)
    return name


# --- Preset dismissals (issue #71) ---
#
# Presets themselves are defined in code (epigone.criteria_presets); only the
# per-User "I deleted this one" state lives here. Deleting a preset is a
# hide-for-me — one row per (User, preset) — keyed on the preset's stable code
# key so a later threshold recalibration never resurrects a dismissed preset.


async def hidden_preset_keys(pool: asyncpg.Pool, user_id: int) -> set[str]:
    """The preset keys this User has deleted (hidden) for themselves."""
    rows = await pool.fetch(
        "SELECT preset_key FROM criteria_preset_dismissals WHERE user_telegram_id = $1",
        user_id,
    )
    return {row["preset_key"] for row in rows}


async def dismiss_preset(
    pool: asyncpg.Pool, user_id: int, preset_key: str, now: datetime
) -> None:
    """Hide a preset for this User, permanently. Idempotent — deleting an
    already-hidden preset keeps the original dismissal timestamp."""
    await pool.execute(
        """
        INSERT INTO criteria_preset_dismissals (user_telegram_id, preset_key, dismissed_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (user_telegram_id, preset_key) DO NOTHING
        """,
        user_id,
        preset_key,
        now,
    )
