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
