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
from epigone.execute import subs as subs_store
from epigone.execute.executor import CopyExecutor
from epigone.execute.policy import RiskPolicyV0
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
) -> Position:
    return Position(
        coin=coin,
        side=side,
        size_usd=Decimal(size_usd),
        leverage=Decimal("1"),
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
    policy: RiskPolicyV0 | None = None,
) -> CopyHarness:
    """A wired executor over a fake exchange, with a core universe that prices
    BTC and ETH and a leader comfortably above the liveness floor."""
    await seed_trader(pool, clock)
    read.perp_universes[None] = ["BTC", "ETH"]
    # The covered builder DEX has to answer too: fetch_asset_specs walks
    # POSITION_VENUES, so an empty perpDexs listing fails the whole map.
    read.perp_universes["xyz"] = ["xyz:META"]
    read.perp_dex_listing = ["xyz"]
    read.sz_decimals = {"BTC": 5, "ETH": 4, "xyz:META": 2}
    read.mid_prices[None] = {"BTC": Decimal("63500"), "ETH": Decimal("2000")}
    read.mid_prices["xyz"] = {"xyz:META": Decimal("600")}
    read.account_values[(LEADER, None)] = Decimal("250000")
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
        policy or RiskPolicyV0(),
        operator_id=OPERATOR,
        master_address=MASTER,
        signer_address=SIGNER,
    )
    return CopyHarness(
        executor=executor,
        pool=pool,
        clock=clock,
        read=read,
        exec_fake=exec_fake,
        audit=audit,
    )


async def copy_sub(
    pool: asyncpg.Pool,
    clock: FakeClock,
    *,
    provisioned: bool = True,
    mode: str = "default",
    base_notional: str = "200",
    allocation: str = "1000",
    take_profit_pct: str | None = None,
    stop_loss_pct: str | None = None,
    leader: str = LEADER,
    sub_address: str = SUB,
) -> CopySub:
    sub = await subs_store.register_sub(
        pool,
        operator_id=OPERATOR,
        leader_address=leader,
        sub_name=f"epicopy-{leader[-4:]}",
        allocation_usd=Decimal(allocation),
        base_notional_usd=Decimal(base_notional),
        copy_mode=mode,
        take_profit_pct=Decimal(take_profit_pct) if take_profit_pct else None,
        stop_loss_pct=Decimal(stop_loss_pct) if stop_loss_pct else None,
        now=clock.now(),
    )
    if provisioned:
        await subs_store.record_sub_address(pool, sub.id, sub_address)
        await subs_store.mark_funded(pool, sub.id, clock.now())
        rows = await subs_store.enabled_subs(pool, OPERATOR)
        return next(s for s in rows if s.id == sub.id)
    return sub


async def emit(
    pool: asyncpg.Pool,
    event: PositionEvent,
    observed_at: datetime,
    *,
    trader: str = LEADER,
    source: str = "poll",
) -> None:
    """One position event, as the poller would have written it."""
    async with pool.acquire() as conn, conn.transaction():
        await record_events(conn, trader, [event], observed_at, source=source)
