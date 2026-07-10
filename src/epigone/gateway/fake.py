from epigone.gateway import Fill, LeaderboardEntry, PortfolioWindow, Position, Window


class FakeHyperliquidGateway:
    """In-memory gateway for tests: set data per address, no network.

    Configure failures by assigning `leaderboard_error` / `portfolio_errors` /
    `fills_errors`; `positions_calls`, `portfolio_calls` and `fills_calls`
    record every request (lowercased) in order.
    """

    def __init__(self) -> None:
        self.positions: dict[str, list[Position]] = {}
        self.positions_errors: dict[str, Exception] = {}
        self.positions_calls: list[str] = []
        self.leaderboard: list[LeaderboardEntry] = []
        self.leaderboard_error: Exception | None = None
        self.portfolios: dict[str, dict[Window, PortfolioWindow]] = {}
        self.portfolio_errors: dict[str, Exception] = {}
        self.portfolio_calls: list[str] = []
        self.fills: dict[str, list[Fill]] = {}
        self.fills_errors: dict[str, Exception] = {}
        self.fills_calls: list[str] = []

    async def get_open_positions(self, address: str) -> list[Position]:
        key = address.lower()
        self.positions_calls.append(key)
        error = self.positions_errors.get(key)
        if error is not None:
            raise error
        return self.positions.get(key, [])

    async def get_leaderboard(self) -> list[LeaderboardEntry]:
        if self.leaderboard_error is not None:
            raise self.leaderboard_error
        return list(self.leaderboard)

    async def get_portfolio(self, address: str) -> dict[Window, PortfolioWindow]:
        key = address.lower()
        self.portfolio_calls.append(key)
        error = self.portfolio_errors.get(key)
        if error is not None:
            raise error
        return dict(self.portfolios.get(key, {}))

    async def get_fills(self, address: str) -> list[Fill]:
        key = address.lower()
        self.fills_calls.append(key)
        error = self.fills_errors.get(key)
        if error is not None:
            raise error
        return list(self.fills.get(key, []))

    def set_positions(self, address: str, positions: list[Position]) -> None:
        self.positions[address.lower()] = positions

    def set_leaderboard(self, entries: list[LeaderboardEntry]) -> None:
        self.leaderboard = list(entries)

    def set_portfolio(self, address: str, windows: dict[Window, PortfolioWindow]) -> None:
        self.portfolios[address.lower()] = dict(windows)

    def set_fills(self, address: str, fills: list[Fill]) -> None:
        self.fills[address.lower()] = list(fills)
