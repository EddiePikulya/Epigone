"""Harness for the copy executor's tests (issue #136).

Seam per the house convention: fake read gateway, fake execution gateway, fake
clock, real Postgres. The execution fake stands in AFTER the signer seam, so
nothing here touches key material — the executor's logic is what is under
test, never the signing.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

import asyncpg

from epigone.budget import WeightBudget
from epigone.execute import limits as risk_limits
from epigone.execute import subs as subs_store
from epigone.execute.executor import CopyExecutor
from epigone.execute.policy import MIRROR_LEVERAGE, RiskPolicy
from epigone.execute.subs import CopySub
from epigone.gateway import Position, Side
from epigone.gateway.execution_fake import FakeExecutionGateway
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.position_events import PositionEvent, record_events
from epigone.safety.audit import (
    EXECUTOR_ACTOR,
    AuditedExecutionGateway,
    AuditedProvisioning,
    ExecutionAudit,
)
from tests.support.clock import FakeClock

WIDE_OPEN_BUDGET = 1_000_000

OPERATOR = 4242
LEADER = "0xleader00000000000000000000000000000000aa"
MASTER = "0xmaster00000000000000000000000000000000bb"
SIGNER = "0xagent000000000000000000000000000000000cc"
SUB = "0x00000000000000000000000000000000000000001"


@dataclass
class CopyHarness:
    executor: CopyExecutor
    pool: asyncpg.Pool
    clock: FakeClock
    read: FakeHyperliquidGateway
    signal: FakeHyperliquidGateway
    exec_fake: FakeExecutionGateway
    audit: ExecutionAudit

    async def notices(self) -> list[str]:
        rows = await self.pool.fetch("SELECT body FROM copy_notices ORDER BY id")
        return [row["body"] for row in rows]

    async def audit_actions(self) -> list[tuple[str, str]]:
        rows = await self.pool.fetch(
            "SELECT action, risk_decision FROM execution_audit ORDER BY id"
        )
        return [(row["action"], row["risk_decision"]) for row in rows]

    def placed(self) -> list[tuple[object, ...]]:
        """Every place_orders call's payload, in submission order."""
        return [payload for method, payload in self.exec_fake.actions if method == "place_orders"]

    async def sub(self, sub_id: int) -> CopySub:
        """The mapping as it stands NOW — the enabled flag and the budget
        ledger are what a cycle writes, so a test asserting either has to
        re-read rather than trust the handle it started with."""
        rows = await subs_store.all_subs(self.pool, OPERATOR)
        return next(sub for sub in rows if sub.id == sub_id)

    async def episodes(self, sub_id: int) -> list[asyncpg.Record]:
        return list(
            await self.pool.fetch(
                "SELECT * FROM copy_episodes WHERE sub_id = $1 ORDER BY id", sub_id
            )
        )


def position(
    coin: str = "BTC",
    side: Side = Side.LONG,
    size_usd: str = "200",
    size_coin: str = "0.1",
    entry_price: str = "2000",
    leverage: str = "1",
) -> Position:
    """One held position as the exchange reports it.

    `leverage` defaults to 1x, which makes `Position.margin` equal the notional
    — the honest reading of an unlevered position, and the one that keeps a
    test's stake arithmetic obvious: a $200 1x position uses $200 of the stake
    caps. Tests about levered sizing set it."""
    return Position(
        coin=coin,
        side=side,
        size_usd=Decimal(size_usd),
        leverage=Decimal(leverage),
        entry_price=Decimal(entry_price),
        unrealized_pnl=Decimal("0"),
        size_coin=Decimal(size_coin),
    )


async def seed_trader(pool: asyncpg.Pool, clock: FakeClock, address: str = LEADER) -> None:
    await pool.execute(
        """
        INSERT INTO traders (address, first_seen_at, last_seen_at)
        VALUES ($1, $2, $2) ON CONFLICT (address) DO NOTHING
        """,
        address,
        clock.now(),
    )


async def build_harness(
    pool: asyncpg.Pool,
    clock: FakeClock,
    read: FakeHyperliquidGateway,
    *,
    policy: RiskPolicy | None = None,
) -> CopyHarness:
    """A wired executor over a fake exchange, with a core universe that prices
    BTC and ETH and a leader comfortably above the liveness floor.

    TWO read fakes, because the executor has two read gateways (issue #184):
    `read` is the book it trades and answers every read but one; `signal` is
    the network the signal came from and answers the Leader's liveness equity
    alone. They are DISTINCT by default, and the leader's equity is set only on
    the signal side — so the shakedown topology (a mainnet Leader holding $0 on
    the testnet book) is what every test in this suite runs against, and a
    liveness read that slipped back onto the trade gateway would read $0 and
    fail loudly."""
    await seed_trader(pool, clock)
    signal = FakeHyperliquidGateway()
    signal.account_values[(LEADER, None)] = Decimal("250000")
    read.perp_universes[None] = ["BTC", "ETH"]
    # The covered builder DEX has to answer too: fetch_asset_specs walks
    # POSITION_VENUES, so an empty perpDexs listing fails the whole map.
    read.perp_universes["xyz"] = ["xyz:META"]
    read.perp_dex_listing = ["xyz"]
    read.sz_decimals = {"BTC": 5, "ETH": 4, "xyz:META": 2}
    read.mid_prices[None] = {"BTC": Decimal("63500"), "ETH": Decimal("2000")}
    read.mid_prices["xyz"] = {"xyz:META": Decimal("600")}
    # The sub holds NOTHING by default — the honest state for an account no
    # test funded, and the one that makes provisioning transfer the whole
    # allocation. Tests about the top-up set a balance explicitly.

    audit = ExecutionAudit(pool, clock)
    exec_fake = FakeExecutionGateway()
    gateway = AuditedExecutionGateway(
        exec_fake,
        audit,
        actor=EXECUTOR_ACTOR,
        master_address=MASTER,
        signer_address=SIGNER,
    )
    provisioning = AuditedProvisioning(
        exec_fake,
        audit,
        actor=EXECUTOR_ACTOR,
        master_address=MASTER,
        signer_address=SIGNER,
    )
    executor = CopyExecutor(
        pool,
        clock,
        read,
        gateway,
        provisioning,
        audit,
        WeightBudget(WIDE_OPEN_BUDGET, clock),
        policy or RiskPolicy(),
        signal_gateway=signal,
        operator_id=OPERATOR,
        master_address=MASTER,
        signer_address=SIGNER,
    )
    return CopyHarness(
        executor=executor,
        pool=pool,
        clock=clock,
        read=read,
        signal=signal,
        exec_fake=exec_fake,
        audit=audit,
    )


async def copy_sub(
    pool: asyncpg.Pool,
    clock: FakeClock,
    *,
    provisioned: bool = True,
    mode: str = "default",
    base_stake: str = "200",
    leverage_mode: str = MIRROR_LEVERAGE,
    fixed_leverage: int | None = None,
    allocation: str = "1000",
    take_profit_pct: str | None = None,
    stop_loss_pct: str | None = None,
    leader: str = LEADER,
    sub_address: str = SUB,
    loss_budget: str | None = None,
    baseline: str | None = None,
    operator: int = OPERATOR,
) -> CopySub:
    """One Leader→sub mapping, as /copy would have written it.

    `loss_budget` arms it the way the operator does — the number alone, with no
    baseline — so a test that wants the executor's own arming to happen just
    passes it. `baseline` additionally stands in for a budget the executor has
    ALREADY armed, which is what a test about a mid-run breach or a restart
    needs: the ledger it inherits, not the one it creates."""
    sub = await subs_store.register_sub(
        pool,
        operator_id=operator,
        leader_address=leader,
        sub_name=f"epicopy-{leader[-4:]}",
        allocation_usd=Decimal(allocation),
        base_stake_usd=Decimal(base_stake),
        leverage_mode=leverage_mode,
        fixed_leverage=fixed_leverage,
        copy_mode=mode,
        take_profit_pct=Decimal(take_profit_pct) if take_profit_pct else None,
        stop_loss_pct=Decimal(stop_loss_pct) if stop_loss_pct else None,
        now=clock.now(),
        loss_budget_usd=Decimal(loss_budget) if loss_budget else None,
    )
    if baseline is not None:
        await subs_store.arm_budget(
            pool, sub.id, baseline_usd=Decimal(baseline), now=clock.now()
        )
    if provisioned:
        await subs_store.record_sub_address(pool, sub.id, sub_address)
        await subs_store.mark_funded(pool, sub.id, clock.now())
    rows = await subs_store.all_subs(pool, operator)
    return next(s for s in rows if s.id == sub.id)


async def record_spend(
    pool: asyncpg.Pool,
    sub: CopySub,
    spent: str,
    *,
    warned_at: datetime | None = None,
    breached_at: datetime | None = None,
) -> None:
    """Put a sub's budget ledger where a test needs it, through the same setter
    the executor uses — so a test can never reach a budget state by a route the
    executor does not have.

    It passes the sub's OWN terms as the compare-and-set, which is exactly what
    the executor passes when nothing changed under it."""
    assert sub.loss_budget_usd is not None and sub.budget_armed_at is not None
    await subs_store.record_budget_spend(
        pool,
        sub.id,
        spent_usd=Decimal(spent),
        warned_at=warned_at,
        breached_at=breached_at,
        judged_budget_usd=sub.loss_budget_usd,
        judged_armed_at=sub.budget_armed_at,
    )


async def set_limits(pool: asyncpg.Pool, clock: FakeClock, **knobs: str) -> None:
    """Move global risk knobs the way /limits does — through the same setter,
    so a test can never configure a limit by a route the operator does not
    have (and so a knob renamed in the registry fails these tests too)."""
    for name, raw in knobs.items():
        async with pool.acquire() as conn, conn.transaction():
            await risk_limits.set_knob(
                conn, name=name, raw=raw, operator_id=OPERATOR, now=clock.now()
            )


async def emit(
    pool: asyncpg.Pool,
    event: PositionEvent,
    observed_at: datetime,
    *,
    trader: str = LEADER,
    source: str = "poll",
    authoritative: bool = True,
) -> None:
    """One position event, as the lane that observed it would have written it.

    `authoritative` is what the executor filters on since the cutover (#158):
    the lane that did NOT own production when it saw the change writes the same
    row with this False, and nothing consumes it."""
    async with pool.acquire() as conn, conn.transaction():
        await record_events(
            conn, trader, [event], observed_at, source=source, authoritative=authoritative
        )
