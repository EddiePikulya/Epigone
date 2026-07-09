from epigone.gateway import Position


class FakeHyperliquidGateway:
    """In-memory gateway for tests: set positions per address, no network."""

    def __init__(self) -> None:
        self.positions: dict[str, list[Position]] = {}

    async def get_open_positions(self, address: str) -> list[Position]:
        return self.positions.get(address.lower(), [])

    def set_positions(self, address: str, positions: list[Position]) -> None:
        self.positions[address.lower()] = positions
