"""The global risk knobs: one row, read every cycle, changed by /limits
(issue #137, ADR-0007 amendment D-4).

Two kinds of configuration live in A5 and they are split by WHO the number
belongs to. A Base Stake and a leverage mode describe one Leader's mapping, so
they sit on `copy_subs` and are set with /copy. The numbers here describe
EPIGONE'S OWN STANCE — how thin a market it will trade, how much margin it
will put behind one coin or one sub, how much leverage it will let a Leader's
dial reach — so they are global, singular, and changed without touching any
mapping.

RE-READ EVERY CYCLE, never cached at startup. Same rule `copy_subs.enabled`
obeys and for the same reason: a limit an operator has to restart a process to
apply is a limit they will not change during the incident that needed it.

ABSENCE IS NOT PERMISSION. `load` falls back to the DEFAULTS below if the row
is missing or unreadable-as-a-row, because the alternative — a policy that
treats a vanished row as "no limits" — turns a bad migration into an unbounded
order. The migration seeds the same numbers, so in a healthy system the
fallback never fires and the two copies never disagree.
"""

from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import asyncpg

from epigone.decimals import plain

# Conservative by intent, and the same numbers migration 0036 seeds (module
# docstring). Measured 2026-08-05 against the live universe: a $100k/$100k
# floor denies 22 of 177 core coins and about 1 of 88 on xyz — a tripwire for
# markets where a copied trade's counterparty can be the Leader themselves,
# not a coin preference.
#
# The stake caps are MARGIN dollars, and the per-coin one is deliberately
# SEVERAL Base Stakes rather than one: a cap equal to the stake would let the
# opening order through and deny every scale-in after it, which is not a limit
# but a half-mirrored position. Three stakes of room per coin at the $100 the
# ticket reasons about; the aggregate leaves headroom under the $1,000-ish
# allocation a sub is typically funded with, so the CAP binds before the
# exchange's margin does — which is the point of having one at all.
DEFAULT_FLOOR_DAY_NOTIONAL_USD = Decimal("100000")
DEFAULT_FLOOR_OPEN_INTEREST_USD = Decimal("100000")
DEFAULT_MAX_COIN_STAKE_USD = Decimal("300")
DEFAULT_MAX_SUB_STAKE_USD = Decimal("900")
DEFAULT_BACKSTOP_LEVERAGE = 20


@dataclass(frozen=True)
class RiskLimits:
    """The global knobs as one immutable value. Passed to the policy per
    judgement rather than held by it, so nothing can judge one order against
    the limits and the next against a cached copy of them."""

    floor_day_notional_usd: Decimal = DEFAULT_FLOOR_DAY_NOTIONAL_USD
    floor_open_interest_usd: Decimal = DEFAULT_FLOOR_OPEN_INTEREST_USD
    max_coin_stake_usd: Decimal = DEFAULT_MAX_COIN_STAKE_USD
    max_sub_stake_usd: Decimal = DEFAULT_MAX_SUB_STAKE_USD
    backstop_leverage: int = DEFAULT_BACKSTOP_LEVERAGE
    updated_at: datetime | None = None
    updated_by: int | None = None

    @property
    def floor_disabled(self) -> bool:
        """Both halves at zero: the operator has turned the Liquidity Floor
        off, which the glossary explicitly permits. Said once here so the
        policy's decision prose can name it instead of printing "$0 >= $0"."""
        return self.floor_day_notional_usd <= 0 and self.floor_open_interest_usd <= 0


class UnknownKnobError(ValueError):
    """/limits named something this row does not carry."""


@dataclass(frozen=True)
class Knob:
    """One tunable, as the operator's command sees it. The registry is what
    makes /limits one command rather than five: a knob is its column, how to
    parse the operator's word into the column's type, and the sentence that
    says what it does."""

    name: str
    column: str
    is_integer: bool
    # Whether 0 MEANS something for this knob. It does for the two floors —
    # the floor is a default stance, not a cage — and it does not for a stake
    # cap or the leverage backstop, where zero would be "copy nothing" wearing
    # a limit's clothes (/uncopy says that, and says it reversibly). A field on
    # the registry row rather than a rule derived from the NAME: a knob renamed
    # for legibility must not silently change what values it accepts.
    zero_means_off: bool
    unit: str
    description: str

    def parse(self, raw: str) -> Decimal | int:
        """The operator's word as this knob's value, or ValueError."""
        try:
            value = Decimal(raw)
        except InvalidOperation as exc:
            raise ValueError(f"{self.name} must be a number, got {raw!r}") from exc
        floor = Decimal(0) if self.zero_means_off else Decimal("0.0000001")
        if value < floor:
            raise ValueError(
                f"{self.name} must be at least {plain(floor)}"
                + (" (0 turns this floor off)" if self.zero_means_off else "")
            )
        if self.is_integer:
            if value != value.to_integral_value():
                raise ValueError(f"{self.name} must be a whole number, got {raw!r}")
            return int(value)
        return value


KNOBS: tuple[Knob, ...] = (
    Knob(
        name="floor_volume",
        column="floor_day_notional_usd",
        is_integer=False,
        zero_means_off=True,
        unit="$",
        description="Liquidity Floor: minimum 24h traded notional (0 = off)",
    ),
    Knob(
        name="floor_oi",
        column="floor_open_interest_usd",
        is_integer=False,
        zero_means_off=True,
        unit="$",
        description="Liquidity Floor: minimum open-interest notional (0 = off)",
    ),
    Knob(
        name="coin_stake",
        column="max_coin_stake_usd",
        is_integer=False,
        zero_means_off=False,
        unit="$",
        description="max stake (margin) per coin per sub — orders over it CLAMP",
    ),
    Knob(
        name="sub_stake",
        column="max_sub_stake_usd",
        is_integer=False,
        zero_means_off=False,
        unit="$",
        description="max aggregate stake (margin) per sub — orders over it CLAMP",
    ),
    Knob(
        name="max_leverage",
        column="backstop_leverage",
        is_integer=True,
        zero_means_off=False,
        unit="x",
        description="backstop cap on mirrored leverage (the asset's own max still applies)",
    ),
)

_BY_NAME = {knob.name: knob for knob in KNOBS}


def knob(name: str) -> Knob:
    try:
        return _BY_NAME[name]
    except KeyError as exc:
        raise UnknownKnobError(
            f"unknown limit {name!r} — one of {', '.join(_BY_NAME)}"
        ) from exc


async def load(conn: asyncpg.Pool | asyncpg.Connection) -> RiskLimits:
    """The live knobs. Falls back to the module defaults when the row is
    missing (module docstring: absence is not permission)."""
    row = await conn.fetchrow("SELECT * FROM risk_limits WHERE id = 1")
    if row is None:
        return RiskLimits()
    return RiskLimits(
        floor_day_notional_usd=row["floor_day_notional_usd"],
        floor_open_interest_usd=row["floor_open_interest_usd"],
        max_coin_stake_usd=row["max_coin_stake_usd"],
        max_sub_stake_usd=row["max_sub_stake_usd"],
        backstop_leverage=row["backstop_leverage"],
        updated_at=row["updated_at"],
        updated_by=row["updated_by"],
    )


async def set_knob(
    conn: asyncpg.Connection,
    *,
    name: str,
    raw: str,
    operator_id: int,
    now: datetime,
) -> tuple[RiskLimits, RiskLimits]:
    """Move one knob, returning (before, after) so the caller can audit the
    change as old -> new.

    TAKES A CONNECTION, NEVER A POOL, for the reason `position_events`'
    producers do: the caller writes the audit row in the same transaction as
    this write, so a limit can never move without the trail that says who moved
    it — nor a trail claim a change that rolled back.

    ONE COLUMN IS WRITTEN, not five. The UPDATE names only the knob that
    moved, so this is not a read-modify-write of the whole row: `before` is
    read for the trail's old value, and nothing else in the row is re-stated on
    the way past. The column name is interpolated from the KNOB REGISTRY, never
    from the operator's text — `knob()` raises on anything not in it, so no
    caller can reach this with an arbitrary identifier.

    The INSERT is the fallback for a row that went missing, so a policy in that
    state is repairable from the command that configures it rather than from
    psql. It states the whole record because there is no record to merge into,
    and the values it states are the DEFAULTS with this knob applied — never a
    guess."""
    entry = knob(name)
    value = entry.parse(raw)
    before = await load(conn)
    # The RiskLimits field and the column share a name by construction (the
    # dataclass mirrors the row), so one registry entry addresses both. The
    # dict is `Any`-valued because a knob's type is decided by its registry
    # entry, not by the call site — `Knob.parse` is the one place that knows.
    changed: dict[str, Any] = {entry.column: value}
    after = replace(before, updated_at=now, updated_by=operator_id, **changed)
    updated = await conn.fetchval(
        f"""
        UPDATE risk_limits
        SET {entry.column} = $1, updated_at = $2, updated_by = $3
        WHERE id = 1
        RETURNING id
        """,
        value,
        now,
        operator_id,
    )
    if updated is None:
        await conn.execute(
            """
            INSERT INTO risk_limits
                (id, floor_day_notional_usd, floor_open_interest_usd, max_coin_stake_usd,
                 max_sub_stake_usd, backstop_leverage, updated_at, updated_by)
            VALUES (1, $1, $2, $3, $4, $5, $6, $7)
            """,
            after.floor_day_notional_usd,
            after.floor_open_interest_usd,
            after.max_coin_stake_usd,
            after.max_sub_stake_usd,
            after.backstop_leverage,
            now,
            operator_id,
        )
    return before, after


def value_of(limits: RiskLimits, entry: Knob) -> Decimal | int:
    """This knob's current value. `column` addresses the dataclass field too —
    the record mirrors the row field for field, deliberately, so /limits needs
    no second mapping to keep in step with the schema."""
    value = getattr(limits, entry.column)
    assert isinstance(value, Decimal | int)
    return value


def render(limits: RiskLimits, entry: Knob) -> str:
    """One knob as the operator reads it: `12x`, `$100000`, `$0 (off)`.

    FIXED-POINT, NEVER SCIENTIFIC (issue #185). A knob loaded from the row
    arrives as Postgres wrote it — `100000` decodes to `Decimal('1.0E+5')` —
    and this string is read twice: once in the /limits reply, once as the old →
    new of the `risk_limit_changed` audit row. One renderer means the trail
    cannot disagree with the message that announced the change."""
    value = value_of(limits, entry)
    shown = plain(value)
    text = f"{shown}{entry.unit}" if entry.unit == "x" else f"{entry.unit}{shown}"
    if entry.zero_means_off and value == 0:
        text += " (off)"
    return text


__all__ = [
    "KNOBS",
    "Knob",
    "RiskLimits",
    "UnknownKnobError",
    "knob",
    "load",
    "render",
    "set_knob",
    "value_of",
]
