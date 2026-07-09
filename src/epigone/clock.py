"""Injected clock — the house convention (V1 spec "Testing Decisions"): metric
windows, poll cadence, and budget pacing never read the wall clock directly."""

import asyncio
from datetime import UTC, datetime
from typing import Protocol


class Clock(Protocol):
    def now(self) -> datetime: ...

    async def sleep(self, seconds: float) -> None: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(tz=UTC)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)
