from datetime import UTC, datetime, timedelta


class FakeClock:
    """Injected clock for tests: sleeping advances fake time instantly."""

    def __init__(self, start: datetime | None = None) -> None:
        self.current = start or datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
        self.slept: list[float] = []

    def now(self) -> datetime:
        return self.current

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.current += timedelta(seconds=seconds)

    def advance(self, seconds: float) -> None:
        self.current += timedelta(seconds=seconds)
