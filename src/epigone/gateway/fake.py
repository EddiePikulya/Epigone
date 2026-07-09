from epigone.gateway import LeaderboardEntry, PortfolioWindow, Position, Window


class FakeHyperliquidGateway:
    """In-memory gateway for tests: set data per address, no network.

    Configure failures by assigning `leaderboard_error` / `portfolio_errors`;
    `portfolio_calls` records every portfolio request (lowercased) in order.
    """

    def __init__(self) -> None:
        self.positions: dict[str, list[Position]] = {}
        self.leaderboard: list[LeaderboardEntry] = []
        self.leaderboard_error: Exception | None = None
        self.portfolios: dict[str, dict[Window, PortfolioWindow]] = {}
        self.portfolio_errors: dict[str, Exception] = {}
        self.portfolio_calls: list[str] = []

    async def get_open_positions(self, address: str) -> list[Position]:
        return self.positions.get(address.lower(), [])

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

    def set_positions(self, address: str, positions: list[Position]) -> None:
        self.positions[address.lower()] = positions

    def set_leaderboard(self, entries: list[LeaderboardEntry]) -> None:
        self.leaderboard = list(entries)

    def set_portfolio(self, address: str, windows: dict[Window, PortfolioWindow]) -> None:
        self.portfolios[address.lower()] = dict(windows)
