from datetime import datetime

from epigone.gateway import Fill, LeaderboardEntry, OpenOrder, Position


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
        self.open_orders: dict[tuple[str, str | None], list[OpenOrder]] = {}
        self.open_orders_errors: dict[str, Exception] = {}
        self.open_orders_errors_by_dex: dict[tuple[str, str | None], Exception] = {}
        self.open_orders_calls: list[tuple[str, str | None]] = []
        self.leaderboard: list[LeaderboardEntry] = []
        self.leaderboard_error: Exception | None = None
        self.leaderboard_calls = 0
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

    async def get_open_orders(self, address: str, dex: str | None = None) -> list[OpenOrder]:
        key = address.lower()
        self.open_orders_calls.append((key, dex))
        error = self.open_orders_errors_by_dex.get((key, dex)) or self.open_orders_errors.get(key)
        if error is not None:
            raise error
        return self.open_orders.get((key, dex), [])

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
