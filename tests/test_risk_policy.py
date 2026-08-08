"""The A5 risk policy as arithmetic and verdicts (issue #137).

Pure functions and one stateless class, so these pin the DECISIONS — what a
Copyable Coin is, how a mirrored leverage is capped, how a stake cap clamps —
with no wire anywhere near them. The executor's own tests (test_copy_executor)
then check that the executor ASKS these questions at the right moments; this
file checks the answers.

The Loss Budget's arithmetic (issue #181) is the one part with a database
under it, and only for its deposit adjustment: what that join EXCLUDES — an
unconfirmed transfer, a transfer from before the budget was armed — is the
arithmetic, not a detail of it.
"""

from decimal import Decimal

import asyncpg
import pytest

from epigone.execute.limits import RiskLimits
from epigone.execute.policy import (
    BUDGET_BREACHED,
    BUDGET_WARNING,
    BUDGET_WITHIN,
    FIXED_LEVERAGE,
    MIRROR_LEVERAGE,
    LeverageUnknownError,
    RiskPolicy,
    budget_loss,
    budget_stage,
    clears_liquidity_floor,
    committed_stake,
    resolve_leverage,
    stake_headroom,
)
from epigone.execute.subs import USD_MICRO, deposits_since
from epigone.gateway import GatewayError, MarketStats, Position, Side, fetch_account_state
from epigone.gateway.fake import FakeHyperliquidGateway
from epigone.safety.audit import EXECUTOR_ACTOR, OK, ExecutionAudit
from tests.support.clock import FakeClock
from tests.support.copy import SUB

LIMITS = RiskLimits(
    floor_day_notional_usd=Decimal("100000"),
    floor_open_interest_usd=Decimal("100000"),
    max_coin_stake_usd=Decimal("300"),
    max_sub_stake_usd=Decimal("900"),
    backstop_leverage=20,
)


def market(volume: str = "5000000", open_interest: str = "500", max_leverage: int = 40):
    """A market with a $1,000 mark, so open interest reads in round dollars."""
    return MarketStats(
        coin="ETH",
        day_notional_volume=Decimal(volume),
        open_interest=Decimal(open_interest),
        mark_price=Decimal("1000"),
        max_leverage=max_leverage,
    )


def held(size_usd: str, leverage: str, coin: str = "ETH") -> Position:
    return Position(
        coin=coin,
        side=Side.LONG,
        size_usd=Decimal(size_usd),
        leverage=Decimal(leverage),
        entry_price=Decimal("1000"),
        unrealized_pnl=Decimal("0"),
    )


# --- the Liquidity Floor ------------------------------------------------------


def test_a_coin_clears_the_floor_only_by_clearing_BOTH_halves() -> None:
    # A market can be churned by volume with nothing standing behind it, and it
    # can carry stale open interest nobody trades. Either alone is a market
    # where getting out costs more than the thesis was worth.
    assert clears_liquidity_floor(market(), LIMITS) is True
    assert clears_liquidity_floor(market(volume="99999"), LIMITS) is False
    assert clears_liquidity_floor(market(open_interest="99"), LIMITS) is False


def test_the_floor_is_an_at_least_not_a_greater_than() -> None:
    # Exactly at the threshold passes: the operator set the number as the
    # minimum they will trade, not as the first number they will refuse.
    assert clears_liquidity_floor(market(volume="100000", open_interest="100"), LIMITS)


def test_a_zero_threshold_turns_that_half_off() -> None:
    off = RiskLimits(floor_day_notional_usd=Decimal(0), floor_open_interest_usd=Decimal(0))
    assert clears_liquidity_floor(market(volume="0", open_interest="0"), off) is True


def test_a_coin_with_no_market_data_is_denied_not_waved_through() -> None:
    # This gate only ever stops an ENTRY: not entering costs a missed copy,
    # entering blind costs money in exactly the market the gate exists to keep
    # us out of.
    verdict = RiskPolicy().judge_coin(coin="ZZZ", stats=None, limits=LIMITS)
    assert verdict.allowed is False
    assert "no live market data" in verdict.decision


def test_a_missing_market_is_denied_even_with_the_floor_switched_off() -> None:
    # Turning the floor off says "trade thin markets", never "trade a market I
    # have no data for" — and the same read carries the asset's own leverage
    # ceiling, so a coin missing from it cannot be sized either.
    off = RiskLimits(floor_day_notional_usd=Decimal(0), floor_open_interest_usd=Decimal(0))
    verdict = RiskPolicy().judge_coin(coin="ZZZ", stats=None, limits=off)
    assert verdict.allowed is False
    assert "what leverage the asset allows" in verdict.decision


def test_a_denial_says_did_not_enter_and_never_did_not_exit() -> None:
    # §3's wording rule. Nothing in this policy can stop an exit, so no denial
    # may read as though it did.
    verdict = RiskPolicy().judge_coin(coin="ETH", stats=market(volume="10"), limits=LIMITS)
    assert verdict.allowed is False
    assert "did not enter" in verdict.decision
    assert "did not exit" not in verdict.decision


# --- mirrored leverage --------------------------------------------------------


def test_mirroring_takes_the_leaders_own_leverage() -> None:
    choice = resolve_leverage(
        mode=MIRROR_LEVERAGE,
        fixed_leverage=None,
        leader_leverage=Decimal("10"),
        asset_max_leverage=40,
        limits=LIMITS,
    )
    assert choice.value == 10
    assert choice.capped is False


def test_a_leaders_fractional_leverage_rounds_down() -> None:
    # The same conservative direction every size rounds: 10.9x mirrors at 10x,
    # which can only make the copy smaller in proportion, never larger.
    choice = resolve_leverage(
        mode=MIRROR_LEVERAGE,
        fixed_leverage=None,
        leader_leverage=Decimal("10.9"),
        asset_max_leverage=40,
        limits=LIMITS,
    )
    assert choice.value == 10


def test_the_backstop_and_the_asset_max_both_cap_and_the_lower_one_wins() -> None:
    backstopped = resolve_leverage(
        mode=MIRROR_LEVERAGE,
        fixed_leverage=None,
        leader_leverage=Decimal("40"),
        asset_max_leverage=40,
        limits=LIMITS,
    )
    assert backstopped.value == 20
    assert "backstop" in backstopped.reason

    asset_capped = resolve_leverage(
        mode=MIRROR_LEVERAGE,
        fixed_leverage=None,
        leader_leverage=Decimal("40"),
        asset_max_leverage=5,
        limits=LIMITS,
    )
    assert asset_capped.value == 5
    assert "asset's own maximum" in asset_capped.reason


def test_a_fixed_mode_ignores_the_leader_but_not_the_caps() -> None:
    choice = resolve_leverage(
        mode=FIXED_LEVERAGE,
        fixed_leverage=50,
        leader_leverage=Decimal("2"),
        asset_max_leverage=40,
        limits=LIMITS,
    )
    assert choice.value == 20  # the backstop still binds
    assert choice.asked == 50


def test_a_mirror_with_no_leader_leverage_raises_rather_than_defaulting() -> None:
    # Every plausible default is a decision about position size that nobody
    # made: 1x silently shrinks the copy tenfold, the backstop maximises it.
    with pytest.raises(LeverageUnknownError):
        resolve_leverage(
            mode=MIRROR_LEVERAGE,
            fixed_leverage=None,
            leader_leverage=None,
            asset_max_leverage=40,
            limits=LIMITS,
        )


# --- stake caps ---------------------------------------------------------------


def test_committed_stake_is_margin_not_notional() -> None:
    # The caps are denominated in the money at risk. A $2,000 position at 20x
    # is $100 of it, and a cap that counted the notional would be a different
    # limit wearing the same name.
    assert committed_stake([held("2000", "20"), held("300", "3")]) == Decimal("200")


def test_headroom_is_the_tighter_of_the_two_caps() -> None:
    assert stake_headroom(
        coin_stake_used=Decimal("100"), sub_stake_used=Decimal("100"), limits=LIMITS
    ) == Decimal("200")  # coin: 300-100 binds before sub: 900-100
    assert stake_headroom(
        coin_stake_used=Decimal("0"), sub_stake_used=Decimal("850"), limits=LIMITS
    ) == Decimal("50")  # now the aggregate binds
    # Never negative: an over-cap position (adopted, or moved against us) means
    # no room, not negative room.
    assert stake_headroom(
        coin_stake_used=Decimal("400"), sub_stake_used=Decimal("400"), limits=LIMITS
    ) == Decimal("0")


def test_an_entry_within_the_caps_is_granted_what_it_asked_for() -> None:
    verdict = RiskPolicy().judge_entry(
        coin="ETH",
        requested_stake_usd=Decimal("100"),
        leverage=resolve_leverage(
            mode=FIXED_LEVERAGE,
            fixed_leverage=10,
            leader_leverage=None,
            asset_max_leverage=40,
            limits=LIMITS,
        ),
        coin_stake_used=Decimal("0"),
        sub_stake_used=Decimal("0"),
        limits=LIMITS,
    )
    assert verdict.allowed and verdict.clamped is False
    assert verdict.stake_usd == Decimal("100")
    assert "$1000.00" in verdict.decision  # the position the stake buys


def test_an_entry_over_a_cap_is_clamped_with_both_figures_on_the_record() -> None:
    verdict = RiskPolicy().judge_entry(
        coin="ETH",
        requested_stake_usd=Decimal("200"),
        leverage=resolve_leverage(
            mode=FIXED_LEVERAGE,
            fixed_leverage=2,
            leader_leverage=None,
            asset_max_leverage=40,
            limits=LIMITS,
        ),
        coin_stake_used=Decimal("250"),
        sub_stake_used=Decimal("250"),
        limits=LIMITS,
    )
    assert verdict.allowed and verdict.clamped is True
    assert verdict.stake_usd == Decimal("50")
    assert "asked $200" in verdict.decision and "given $50.00" in verdict.decision


def test_a_clamp_under_the_exchange_minimum_is_a_denial() -> None:
    verdict = RiskPolicy().judge_entry(
        coin="ETH",
        requested_stake_usd=Decimal("200"),
        leverage=resolve_leverage(
            mode=FIXED_LEVERAGE,
            fixed_leverage=1,
            leader_leverage=None,
            asset_max_leverage=40,
            limits=LIMITS,
        ),
        coin_stake_used=Decimal("295"),
        sub_stake_used=Decimal("0"),
        limits=LIMITS,
    )
    assert verdict.allowed is False
    assert "minimum order value" in verdict.decision
    assert "did not enter" in verdict.decision


def test_leverage_lifts_a_small_stake_over_the_exchange_minimum() -> None:
    # The minimum is judged on the POSITION, which is what the exchange sees:
    # $5 of stake at 1x is refused and the same $5 at 10x is a $50 order.
    policy = RiskPolicy()
    at_one = policy.judge_entry(
        coin="ETH",
        requested_stake_usd=Decimal("5"),
        leverage=resolve_leverage(
            mode=FIXED_LEVERAGE,
            fixed_leverage=1,
            leader_leverage=None,
            asset_max_leverage=40,
            limits=LIMITS,
        ),
        coin_stake_used=Decimal("0"),
        sub_stake_used=Decimal("0"),
        limits=LIMITS,
    )
    at_ten = policy.judge_entry(
        coin="ETH",
        requested_stake_usd=Decimal("5"),
        leverage=resolve_leverage(
            mode=FIXED_LEVERAGE,
            fixed_leverage=10,
            leader_leverage=None,
            asset_max_leverage=40,
            limits=LIMITS,
        ),
        coin_stake_used=Decimal("0"),
        sub_stake_used=Decimal("0"),
        limits=LIMITS,
    )
    assert at_one.allowed is False
    assert at_ten.allowed is True


def test_no_headroom_at_all_is_a_denial_that_touches_nothing_held() -> None:
    verdict = RiskPolicy().judge_entry(
        coin="ETH",
        requested_stake_usd=Decimal("100"),
        leverage=resolve_leverage(
            mode=FIXED_LEVERAGE,
            fixed_leverage=10,
            leader_leverage=None,
            asset_max_leverage=40,
            limits=LIMITS,
        ),
        coin_stake_used=Decimal("300"),
        sub_stake_used=Decimal("300"),
        limits=LIMITS,
    )
    assert verdict.allowed is False
    assert "no stake headroom" in verdict.decision


# --- provisioning and exits ---------------------------------------------------


def test_a_stake_above_its_own_cap_is_refused_at_provisioning_time() -> None:
    # It would be clamped on EVERY open — a copy that never does what the
    # operator asked for. Better said before any money moves.
    verdict = RiskPolicy().judge_provisioning(
        allocation_usd=Decimal("1000"), base_stake_usd=Decimal("500"), limits=LIMITS
    )
    assert verdict.allowed is False
    assert "per-coin stake cap" in verdict.decision


def test_a_stake_larger_than_the_allocation_could_never_be_margined() -> None:
    verdict = RiskPolicy().judge_provisioning(
        allocation_usd=Decimal("50"), base_stake_usd=Decimal("200"), limits=LIMITS
    )
    assert verdict.allowed is False
    assert "exceeds the allocation" in verdict.decision


def test_the_allocation_ceiling_is_a_typo_catcher_and_says_so() -> None:
    verdict = RiskPolicy().judge_provisioning(
        allocation_usd=Decimal("500000"), base_stake_usd=Decimal("100"), limits=LIMITS
    )
    assert verdict.allowed is False
    assert "funding ceiling" in verdict.decision


def test_exits_are_never_declined() -> None:
    assert RiskPolicy().judge_exit().allowed is True


# --- the Loss Budget's arithmetic (issue #181) --------------------------------
#
# The only tests in this file that touch Postgres, and they earn it: the
# deposit adjustment is a JOIN against the audit trail, and what the join
# excludes — an attempt nobody confirmed, a transfer from before the budget was
# armed — IS the arithmetic. Testing it against a hand-built dict would test a
# fixture. The equity side stays pure: it is one subtraction.


def test_the_loss_is_the_baseline_minus_what_is_left() -> None:
    assert budget_loss(
        baseline_usd=Decimal("1000"),
        deposits_usd=Decimal("0"),
        equity_usd=Decimal("640"),
    ) == Decimal("360")


def test_a_sub_in_profit_reports_a_negative_loss_rather_than_zero() -> None:
    # The figure is read by the operator beside their budget, and "spent
    # -$120" is the honest reading of a leader who is up. Flooring at zero
    # would make a winning run indistinguishable from a flat one.
    assert budget_loss(
        baseline_usd=Decimal("1000"),
        deposits_usd=Decimal("0"),
        equity_usd=Decimal("1120"),
    ) == Decimal("-120")


def test_money_epigone_put_in_is_never_mistaken_for_profit() -> None:
    """The btcgod lesson, as arithmetic. A sub topped up by $500 and worth $500
    more has not made a cent, and a budget that read it as profit would hand
    the leader $500 of runway they never earned."""
    assert budget_loss(
        baseline_usd=Decimal("1000"),
        deposits_usd=Decimal("500"),
        equity_usd=Decimal("1500"),
    ) == Decimal("0")


def test_a_top_up_does_not_forgive_losses_already_booked() -> None:
    # Down $300, then topped up $500: still down $300.
    assert budget_loss(
        baseline_usd=Decimal("1000"),
        deposits_usd=Decimal("500"),
        equity_usd=Decimal("1200"),
    ) == Decimal("300")


def test_the_budget_bites_exactly_ON_the_number_not_a_cent_past_it() -> None:
    budget = Decimal("500")
    assert budget_stage(loss_usd=Decimal("399.99"), budget_usd=budget) == BUDGET_WITHIN
    assert budget_stage(loss_usd=Decimal("400"), budget_usd=budget) == BUDGET_WARNING
    assert budget_stage(loss_usd=Decimal("499.99"), budget_usd=budget) == BUDGET_WARNING
    assert budget_stage(loss_usd=Decimal("500"), budget_usd=budget) == BUDGET_BREACHED
    assert budget_stage(loss_usd=Decimal("612"), budget_usd=budget) == BUDGET_BREACHED


def test_a_profitable_sub_is_nowhere_near_its_budget() -> None:
    assert budget_stage(loss_usd=Decimal("-120"), budget_usd=Decimal("500")) == BUDGET_WITHIN


def test_the_wind_down_verdict_never_claims_it_stopped_an_exit() -> None:
    """The one gate an operator might mistake for a stop-loss. It is not: it
    refuses new risk and lets the copy follow the leader out."""
    verdict = RiskPolicy().judge_wind_down(
        coin="ETH", loss_usd=Decimal("612"), budget_usd=Decimal("500")
    )
    assert verdict.allowed is False
    assert "$612.00" in verdict.decision and "$500" in verdict.decision
    assert "did not enter" in verdict.decision
    assert "Exits keep copying" in verdict.decision
    assert "did not exit" not in verdict.decision


async def test_only_transfers_that_actually_landed_are_counted_as_deposits(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """An attempt row with no outcome is a transfer that MAY never have
    landed. Counting it would credit the sub with equity it does not have and
    slacken the budget by that much — so the join demands the `ok`."""
    audit = ExecutionAudit(pool, clock)
    armed = clock.now()
    clock.advance(60)
    landed = await _transfer_attempt(audit, SUB, "300")
    await audit.record_outcome(landed, outcome=OK, detail={})
    await _transfer_attempt(audit, SUB, "700")  # no outcome: unconfirmed

    assert await deposits_since(pool, sub_address=SUB, since=armed) == Decimal("300")


async def test_the_transfer_that_funded_the_sub_is_not_counted_against_it(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    """The funding transfer is already INSIDE the baseline — the executor
    baselines a sub it has already funded. Counting it again would show the
    whole allocation as lost on the very first cycle."""
    audit = ExecutionAudit(pool, clock)
    funding = await _transfer_attempt(audit, SUB, "1000")
    await audit.record_outcome(funding, outcome=OK, detail={})
    clock.advance(60)
    armed = clock.now()
    clock.advance(60)
    topup = await _transfer_attempt(audit, SUB, "250")
    await audit.record_outcome(topup, outcome=OK, detail={})

    assert await deposits_since(pool, sub_address=SUB, since=armed) == Decimal("250")


async def test_another_subs_funding_is_not_this_subs_deposit(
    pool: asyncpg.Pool, clock: FakeClock
) -> None:
    audit = ExecutionAudit(pool, clock)
    armed = clock.now()
    clock.advance(60)
    other = await _transfer_attempt(audit, "0x00000000000000000000000000000000000000099", "800")
    await audit.record_outcome(other, outcome=OK, detail={})

    assert await deposits_since(pool, sub_address=SUB, since=armed) == Decimal("0")


async def _transfer_attempt(audit: ExecutionAudit, sub_address: str, usd: str):
    """One provisioning transfer, written the way the executor writes it —
    through the audited provisioning wrapper's own row shape, so this test
    cannot pass against a request payload the real path never produces."""
    return await audit.record_attempt(
        actor=EXECUTOR_ACTOR,
        action="subAccountTransfer",
        request={
            "sub_account_user": sub_address,
            "is_deposit": True,
            "usd_micro": int(Decimal(usd) * USD_MICRO),
        },
        risk_decision="test transfer",
    )


async def test_the_equity_the_budget_measures_sums_every_covered_venue() -> None:
    """The other half of the arithmetic, and the one a core-only read gets
    silently wrong: each venue collateralises itself, so a sub holding $400 on
    core and $300 on the builder DEX is worth $700 — not $400, which would read
    as a $300 loss the sub never took."""
    gateway = FakeHyperliquidGateway()
    gateway.set_account_value(SUB, Decimal("400"))  # core
    gateway.set_account_value(SUB, Decimal("300"), dex="xyz")

    state = await fetch_account_state(gateway, SUB)

    assert state.account_value == Decimal("700")
    assert budget_loss(
        baseline_usd=Decimal("1000"),
        deposits_usd=Decimal("0"),
        equity_usd=state.account_value,
    ) == Decimal("300")


async def test_a_venue_that_cannot_be_read_raises_rather_than_summing_short() -> None:
    """A venue contributing zero because it failed to answer is
    indistinguishable from a sub that moved that balance out — and the budget
    would read the difference as a loss. Raising leaves the sub unjudged for a
    cycle, which is the cheap failure."""
    gateway = FakeHyperliquidGateway()
    gateway.set_account_value(SUB, Decimal("400"))
    gateway.positions_errors_by_dex[(SUB, "xyz")] = GatewayError("xyz unreachable")

    with pytest.raises(GatewayError):
        await fetch_account_state(gateway, SUB)
