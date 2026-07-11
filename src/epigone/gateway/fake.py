from datetime import datetime

from epigone.gateway import Fill, LeaderboardEntry, Position


class FakeHyperliquidGateway:
    """In-memory gateway for tests: set data per address, no network.

    Configure failures by assigning `leaderboard_error` / `fills_errors`;
    `fills_calls` records every full-history request (lowercased) in order;
    `fills_since_calls` records incremental ones as (address, start).
    `positions_calls`
    records each get_open_positions as an (address, dex) pair — dex is None for
    the core venue, "xyz" etc. for a builder DEX (issue #21).
    """

    def __init__(self) -> None:
        self.positions: dict[tuple[str, str | None], list[Position]] = {}
        self.positions_errors: dict[str, Exception] = {}
        # Fail one venue while another succeeds, keyed by (address, dex).
        self.positions_errors_by_dex: dict[tuple[str, str | None], Exception] = {}
        self.positions_calls: list[tuple[str, str | None]] = []
        self.leaderboard: list[LeaderboardEntry] = []
        self.leaderboard_error: Exception | None = None
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

    async def get_leaderboard(self) -> list[LeaderboardEntry]:
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

    def set_leaderboard(self, entries: list[LeaderboardEntry]) -> None:
        self.leaderboard = list(entries)

    def set_fills(self, address: str, fills: list[Fill]) -> None:
        self.fills[address.lower()] = list(fills)
