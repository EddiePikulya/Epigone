from datetime import datetime
from decimal import Decimal

from epigone.gateway import (
    AccountState,
    ExtraAgent,
    Fill,
    LeaderboardEntry,
    MarketStats,
    OpenOrder,
    PerpAsset,
    Position,
)

# What an unconfigured coin's size precision answers. A fake needs SOME
# default, and 3 is the mid of the live core range (BTC 5, most majors 2–4) —
# but it is a fiction, so any test whose subject is rounding sets
# `sz_decimals` explicitly rather than leaning on it.
DEFAULT_FAKE_SZ_DECIMALS = 3

# What an unconfigured coin's market health answers (issue #137): a market
# comfortably clear of any sane Liquidity Floor, and the venue's real BTC
# leverage ceiling. Same convention and the same warning as the precision
# default above — it exists so a test about copying does not have to describe
# a market, and any test whose subject IS the floor or the leverage cap
# configures `market_stats` explicitly.
DEFAULT_FAKE_DAY_VOLUME = Decimal("50000000")
DEFAULT_FAKE_OPEN_INTEREST = Decimal("1000000")
DEFAULT_FAKE_MAX_LEVERAGE = 40


class FakeHyperliquidGateway:
    """In-memory gateway for tests: set data per address, no network.

    Configure failures by assigning `leaderboard_error` / `fills_errors`;
    `fills_calls` records every full-history request (lowercased) in order;
    `fills_since_calls` records incremental ones as (address, start).
    `positions_calls`
    records each get_open_positions as an (address, dex) pair — dex is None for
    the core venue, "xyz" etc. for a builder DEX (issue #21).
    `open_orders_calls` records each get_open_orders the same way — orders are
    per-venue exactly like positions (issue #115).
    """

    def __init__(self) -> None:
        self.positions: dict[tuple[str, str | None], list[Position]] = {}
        self.positions_errors: dict[str, Exception] = {}
        # Fail one venue while another succeeds, keyed by (address, dex).
        self.positions_errors_by_dex: dict[tuple[str, str | None], Exception] = {}
        self.positions_calls: list[tuple[str, str | None]] = []
        # A venue's account equity (issue #170), keyed like the positions it
        # rides with. Unset answers zero, which is what the live endpoint says
        # about a venue a wallet holds nothing on (verified 2026-08-04:
        # accountValue "0.0", never an absent field) — so a test that says
        # nothing about equity still gets a shape the real gateway produces.
        self.account_values: dict[tuple[str, str | None], Decimal] = {}
        self.open_orders: dict[tuple[str, str | None], list[OpenOrder]] = {}
        self.open_orders_errors: dict[str, Exception] = {}
        self.open_orders_errors_by_dex: dict[tuple[str, str | None], Exception] = {}
        self.open_orders_calls: list[tuple[str, str | None]] = []
        self.leaderboard: list[LeaderboardEntry] = []
        self.leaderboard_error: Exception | None = None
        self.leaderboard_calls = 0
        # Asset-id mapping sources (issue #135): per-venue universes (coin
        # names in asset-index order, builder-DEX names namespaced like the
        # real meta parser keeps them) and the builder-dex listing whose order
        # fixes the offsets.
        self.perp_universes: dict[str | None, list[str]] = {}
        self.perp_dex_listing: list[str] = []
        self.perp_dex_error: Exception | None = None
        # Order-placement precision, keyed by the same namespaced coin names
        # `perp_universes` lists (issue #136). Deliberately a SIDE table over
        # that one list of names rather than a second universe source: the
        # fake must not be able to say a coin exists for placing and not for
        # cancelling. Unset coins answer DEFAULT_FAKE_SZ_DECIMALS.
        self.sz_decimals: dict[str, int] = {}
        # Per-venue mid prices (allMids, issue #136) — what an IOC entry is
        # priced against. Account equity rides `account_values` above.
        self.mid_prices: dict[str | None, dict[str, Decimal]] = {}
        self.mid_price_errors: dict[str | None, Exception] = {}
        # Per-coin market health (metaAndAssetCtxs, issue #137) — what the
        # Liquidity Floor is judged against. Keyed by namespaced coin like
        # `sz_decimals` is, and for the same reason: the fake must not be able
        # to say a coin exists for ordering and not for judging. Unset coins
        # answer the healthy default above; `market_stats_errors` fails one
        # venue's read the way `mid_price_errors` fails one venue's prices.
        self.market_stats: dict[str, MarketStats] = {}
        self.market_stats_errors: dict[str | None, Exception] = {}
        self.market_stats_calls: list[str | None] = []
        # Sub-accounts per master (the subAccounts readback, issue #136):
        # what the watchdog's per-sub sweep enumerates, and the one source
        # of that list the cold-start blind path can reach.
        self.sub_accounts: dict[str, list[str]] = {}
        self.sub_account_errors: dict[str, Exception] = {}
        self.sub_account_calls: list[str] = []
        # Approved agents by master address (the extraAgents readback, issue
        # #135): what the watchdog's capability probe sees on-chain.
        # `extra_agents_calls` records each read so tests can pin the probe's
        # cadence (checks are hours apart, never per-cycle).
        self.extra_agents: dict[str, list[ExtraAgent]] = {}
        self.extra_agents_errors: dict[str, Exception] = {}
        self.extra_agents_calls: list[str] = []
        self.fills: dict[str, list[Fill]] = {}
        self.fills_errors: dict[str, Exception] = {}
        self.fills_calls: list[str] = []
        self.fills_since_calls: list[tuple[str, datetime]] = []

    async def get_open_positions(self, address: str, dex: str | None = None) -> list[Position]:
        key = address.lower()
        self.positions_calls.append((key, dex))
        error = self.positions_errors_by_dex.get((key, dex)) or self.positions_errors.get(key)
        if error is not None:
            raise error
        return self.positions.get((key, dex), [])

    async def get_account_state(self, address: str, dex: str | None = None) -> AccountState:
        # Routed through get_open_positions so the recorded call, the error
        # injection and any subclass override apply to both reads — the live
        # gateway serves them from one clearinghouseState response, and a fake
        # where one read fails and the other doesn't would be modelling a
        # transport that does not exist.
        positions = await self.get_open_positions(address, dex)
        return AccountState(
            positions=positions,
            account_value=self.account_values.get((address.lower(), dex), Decimal(0)),
        )

    async def get_open_orders(self, address: str, dex: str | None = None) -> list[OpenOrder]:
        key = address.lower()
        self.open_orders_calls.append((key, dex))
        error = self.open_orders_errors_by_dex.get((key, dex)) or self.open_orders_errors.get(key)
        if error is not None:
            raise error
        return self.open_orders.get((key, dex), [])

    async def get_perp_universe(self, dex: str | None = None) -> list[str]:
        return list(self.perp_universes.get(dex, []))

    async def get_sub_accounts(self, address: str) -> list[str]:
        key = address.lower()
        self.sub_account_calls.append(key)
        error = self.sub_account_errors.get(key)
        if error is not None:
            raise error
        return list(self.sub_accounts.get(key, []))

    async def get_perp_assets(self, dex: str | None = None) -> list[PerpAsset]:
        return [
            PerpAsset(name=coin, sz_decimals=self.sz_decimals.get(coin, DEFAULT_FAKE_SZ_DECIMALS))
            for coin in self.perp_universes.get(dex, [])
        ]

    async def get_market_stats(self, dex: str | None = None) -> dict[str, MarketStats]:
        self.market_stats_calls.append(dex)
        error = self.market_stats_errors.get(dex)
        if error is not None:
            raise error
        mids = self.mid_prices.get(dex, {})
        return {
            coin: self.market_stats.get(
                coin,
                MarketStats(
                    coin=coin,
                    day_notional_volume=DEFAULT_FAKE_DAY_VOLUME,
                    open_interest=DEFAULT_FAKE_OPEN_INTEREST,
                    mark_price=mids.get(coin, Decimal(1)),
                    max_leverage=DEFAULT_FAKE_MAX_LEVERAGE,
                ),
            )
            for coin in self.perp_universes.get(dex, [])
        }

    async def get_mid_prices(self, dex: str | None = None) -> dict[str, Decimal]:
        error = self.mid_price_errors.get(dex)
        if error is not None:
            raise error
        return dict(self.mid_prices.get(dex, {}))

    async def get_perp_dexs(self) -> list[str]:
        if self.perp_dex_error is not None:
            raise self.perp_dex_error
        return list(self.perp_dex_listing)

    async def get_extra_agents(self, address: str) -> list[ExtraAgent]:
        key = address.lower()
        self.extra_agents_calls.append(key)
        error = self.extra_agents_errors.get(key)
        if error is not None:
            raise error
        return list(self.extra_agents.get(key, []))

    async def get_leaderboard(self) -> list[LeaderboardEntry]:
        self.leaderboard_calls += 1
        if self.leaderboard_error is not None:
            raise self.leaderboard_error
        return list(self.leaderboard)

    async def get_fills(self, address: str) -> list[Fill]:
        key = address.lower()
        self.fills_calls.append(key)
        error = self.fills_errors.get(key)
        if error is not None:
            raise error
        return list(self.fills.get(key, []))

    async def get_fills_since(self, address: str, start: datetime) -> list[Fill]:
        key = address.lower()
        self.fills_since_calls.append((key, start))
        error = self.fills_errors.get(key)
        if error is not None:
            raise error
        # Mirror userFillsByTime's inclusive startTime against the same store the
        # full pull reads, so a test sets one fill list and both paths agree.
        return [f for f in self.fills.get(key, []) if f.time >= start]

    def set_positions(
        self, address: str, positions: list[Position], dex: str | None = None
    ) -> None:
        self.positions[(address.lower(), dex)] = positions

    def set_account_value(
        self, address: str, account_value: Decimal, dex: str | None = None
    ) -> None:
        """Provide ONE venue's account equity (issue #170), as the real gateway
        reads it: per-dex, beside that venue's positions. A test modelling a
        Trader's total covered-venue equity sets each venue it wants non-zero."""
        self.account_values[(address.lower(), dex)] = account_value

    def set_open_orders(
        self, address: str, orders: list[OpenOrder], dex: str | None = None
    ) -> None:
        """Provide one venue's resting orders, as the real gateway returns them
        (issue #115): per-dex — dex=None is the core venue, "xyz" etc. a
        builder DEX — with builder-DEX coins already namespaced (`xyz:BB`),
        since frontendOpenOrders serves them namespaced and the parser keeps
        them so. A test modelling full coverage sets each venue it wants
        non-empty; unset venues answer [], exactly like the live endpoint for
        a wallet with no ladder there."""
        self.open_orders[(address.lower(), dex)] = list(orders)

    def set_leaderboard(self, entries: list[LeaderboardEntry]) -> None:
        self.leaderboard = list(entries)

    def set_fills(self, address: str, fills: list[Fill]) -> None:
        """Provide fills in **execution order** — oldest first, same-millisecond
        fills in the sequence they executed — the protocol contract the real
        gateway normalizes to (get_fills reverses the newest-first userFills
        and userTwapSliceFills responses; get_fills_since keeps the ByTime
        endpoints' oldest-first). The list is the already-merged union of
        regular and TWAP slice fills (issue #63) — the real gateway merges the
        two endpoints into one stream, so the fake takes one list and a test
        interleaves TWAP slices exactly where they executed. The round-trip
        engine (#58) depends on within-ms order, so a newest-first list here
        would silently exercise the wrong order."""
        self.fills[address.lower()] = list(fills)
