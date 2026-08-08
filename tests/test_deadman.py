"""The scheduleCancel upgrade path (issue #135): eligibility-probed,
inactive-but-ready below the $1M volume gate, self-activating above it,
every transition on the audit trail."""

from datetime import timedelta

import asyncpg
import pytest

from epigone.gateway.execution import ActionRejectedError, AmbiguousExecutionError
from epigone.gateway.execution_fake import FakeExecutionGateway
from epigone.safety.audit import EVENT, WATCHDOG_ACTOR, AuditedExecutionGateway, ExecutionAudit
from epigone.safety.deadman import ACTIVE, INELIGIBLE, UNKNOWN, DeadMansSwitch
from tests.support.clock import FakeClock

MASTER = "0x" + "ab" * 20
SIGNER = "0x" + "cd" * 20

# The live refusal, verbatim (funded probe 2026-07-28, PR #141).
VOLUME_GATE_PROSE = (
    "Cannot set scheduled cancel time until enough volume traded. "
    "Required: $1000000. Traded: $0."
)

HORIZON = timedelta(seconds=300)
REPROBE = timedelta(hours=6)


@pytest.fixture
def fake() -> FakeExecutionGateway:
    return FakeExecutionGateway()


@pytest.fixture
def switch(
    fake: FakeExecutionGateway, pool: asyncpg.Pool, clock: FakeClock
) -> DeadMansSwitch:
    audit = ExecutionAudit(pool, clock)
    gateway = AuditedExecutionGateway(
        fake, audit, actor=WATCHDOG_ACTOR, master_address=MASTER, signer_address=SIGNER
    )
    return DeadMansSwitch(
        gateway, audit, clock, horizon=HORIZON, reprobe=REPROBE, master_address=MASTER
    )


async def _events(pool: asyncpg.Pool) -> list[str]:
    rows = await pool.fetch(
        "SELECT action FROM execution_audit WHERE outcome = $1 ORDER BY id", EVENT
    )
    return [r["action"] for r in rows]


async def test_volume_gate_means_ineligible_but_probed(
    switch: DeadMansSwitch, fake: FakeExecutionGateway, pool: asyncpg.Pool, clock: FakeClock
) -> None:
    fake.errors.append(ActionRejectedError(VOLUME_GATE_PROSE))
    await switch.maintain()
    assert switch.state == INELIGIBLE
    # The probe was a real signed attempt…
    assert [name for name, _ in fake.actions] == ["schedule_cancel"]
    # …and the transition is on the trail once, with the exchange's prose.
    assert await _events(pool) == ["deadman_ineligible"]

    # Not due again until the re-probe cadence — no second attempt now.
    await switch.maintain()
    assert len(fake.actions) == 1

    # Due again after the cadence; still gated → still ineligible, NO second
    # transition event (state didn't change).
    clock.advance(REPROBE.total_seconds())
    fake.errors.append(ActionRejectedError(VOLUME_GATE_PROSE))
    await switch.maintain()
    assert switch.state == INELIGIBLE
    assert len(fake.actions) == 2
    assert await _events(pool) == ["deadman_ineligible"]


async def test_acceptance_activates_and_heartbeats_forward(
    switch: DeadMansSwitch, fake: FakeExecutionGateway, pool: asyncpg.Pool, clock: FakeClock
) -> None:
    await switch.maintain()
    assert switch.state == ACTIVE
    assert await _events(pool) == ["deadman_activated"]
    # The probe IS the activation: the schedule is armed at now + horizon.
    assert fake.actions[-1] == ("schedule_cancel", clock.now() + HORIZON)

    # Pushed forward at half-horizon cadence, not every cycle.
    await switch.maintain()
    assert len(fake.actions) == 1
    clock.advance(HORIZON.total_seconds() / 2)
    await switch.maintain()
    assert len(fake.actions) == 2
    assert fake.actions[-1] == ("schedule_cancel", clock.now() + HORIZON)
    # Still exactly one activation event — pushes are wire rows, not events.
    assert await _events(pool) == ["deadman_activated"]


async def test_crossing_the_gate_later_activates(
    switch: DeadMansSwitch, fake: FakeExecutionGateway, pool: asyncpg.Pool, clock: FakeClock
) -> None:
    fake.errors.append(ActionRejectedError(VOLUME_GATE_PROSE))
    await switch.maintain()
    assert switch.state == INELIGIBLE
    clock.advance(REPROBE.total_seconds())
    await switch.maintain()  # the account crossed $1M; the probe now lands
    assert switch.state == ACTIVE
    assert await _events(pool) == ["deadman_ineligible", "deadman_activated"]


async def test_ambiguity_propagates_and_retries_next_cycle(
    switch: DeadMansSwitch, fake: FakeExecutionGateway, pool: asyncpg.Pool
) -> None:
    fake.errors.append(AmbiguousExecutionError("timed out — may have armed"))
    with pytest.raises(AmbiguousExecutionError):
        await switch.maintain()
    assert switch.state == UNKNOWN
    # Not throttled: the very next cycle re-sets the schedule (a repeated set
    # replaces it, so the retry is the reconciliation).
    await switch.maintain()
    assert switch.state == ACTIVE
    assert [name for name, _ in fake.actions] == ["schedule_cancel", "schedule_cancel"]


async def test_unexpected_reject_falls_back_to_unknown(
    switch: DeadMansSwitch, fake: FakeExecutionGateway, pool: asyncpg.Pool, clock: FakeClock
) -> None:
    await switch.maintain()
    assert switch.state == ACTIVE
    clock.advance(HORIZON.total_seconds() / 2)
    fake.errors.append(ActionRejectedError("Invalid time"))
    await switch.maintain()
    assert switch.state == UNKNOWN
    # The lapse is on the trail: the last eligibility event must never keep
    # claiming "activated" after maintenance stopped landing.
    assert await _events(pool) == ["deadman_activated", "deadman_unsettled"]


async def test_armed_until_is_the_time_the_standing_schedule_fires(
    switch: DeadMansSwitch, fake: FakeExecutionGateway, clock: FakeClock
) -> None:
    """Issue #212: the deadline crosses the Keepalive seam. Nothing is armed
    before the first accepted set, and from then on `armed_until` is the
    instant the exchange would cancel on its own — the number the watchdog's
    priority push is deadline-aware ABOUT."""
    assert switch.armed_until() is None
    await switch.maintain()
    assert switch.armed_until() == clock.now() + HORIZON

    clock.advance(HORIZON.total_seconds() / 2)
    await switch.maintain()  # the half-cadence push
    assert switch.armed_until() == clock.now() + HORIZON


async def test_a_failed_push_leaves_the_old_deadline_standing(
    switch: DeadMansSwitch, fake: FakeExecutionGateway, clock: FakeClock
) -> None:
    """What a caller must NOT be told is that a schedule was re-armed when it
    may not have been. An ambiguous set may or may not have landed and an
    unexpected reject certainly did not, so both keep the deadline the
    exchange is KNOWN to hold — the earlier one, which is the conservative
    direction: it keeps the watchdog's priority window open rather than
    closing it on a push that never happened."""
    await switch.maintain()
    armed = switch.armed_until()
    assert armed is not None

    clock.advance(HORIZON.total_seconds() / 2)
    fake.errors.append(AmbiguousExecutionError("timed out — may have armed"))
    with pytest.raises(AmbiguousExecutionError):
        await switch.maintain()
    assert switch.armed_until() == armed

    fake.errors.append(ActionRejectedError("Invalid time"))
    await switch.maintain()
    assert switch.armed_until() == armed


async def test_an_ineligible_account_has_no_deadline(
    switch: DeadMansSwitch, fake: FakeExecutionGateway
) -> None:
    """The volume-gated case, which is the operator's TODAY: no schedule was
    ever armed, so there is no horizon to be near — and the watchdog's
    priority push must stay dormant rather than fire against a number it
    invented."""
    fake.errors.append(ActionRejectedError(VOLUME_GATE_PROSE))
    await switch.maintain()
    assert switch.state == INELIGIBLE
    assert switch.armed_until() is None


async def test_a_reject_does_not_sit_out_the_reprobe_while_a_schedule_stands(
    switch: DeadMansSwitch, fake: FakeExecutionGateway, clock: FakeClock
) -> None:
    """The re-probe cadence is about ELIGIBILITY, which moves in weeks; a
    standing schedule's horizon is about SAFETY, which does not wait for it.

    An unexpected reject used to defer the next attempt by the full reprobe
    (6h) even when a schedule this switch armed was still standing and hours
    from lapsing — so every later call, the watchdog's budget-exempt priority
    push included (issue #212), returned without reaching the wire while the
    net it was trying to save ran out. A reject with a schedule standing now
    leaves the switch DUE, and only falls back to the slow cadence once there
    is no schedule left to lose."""
    await switch.maintain()
    armed = switch.armed_until()
    assert armed is not None

    clock.advance(HORIZON.total_seconds() / 2)
    fake.errors.append(ActionRejectedError("Invalid time"))
    await switch.maintain()
    assert switch.state == UNKNOWN

    # Due now, not in six hours: the standing schedule outranks the cadence.
    await switch.maintain()
    assert [name for name, _ in fake.actions] == ["schedule_cancel"] * 3
    assert switch.state == ACTIVE  # …and this one landed, re-arming the net

    # Once nothing stands, the slow cadence is back in charge.
    clock.advance(HORIZON.total_seconds())  # the schedule has lapsed
    fake.errors.append(ActionRejectedError(VOLUME_GATE_PROSE))
    await switch.maintain()
    assert switch.state == INELIGIBLE
    await switch.maintain()
    assert len(fake.actions) == 4  # nothing new: re-probing in six hours
