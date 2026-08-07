"""A staged Hyperliquid websocket for the shadow lane's tests (issue #157).

Everything the lane has to get right is a property of TIME and SILENCE — a
connection that stops delivering, a gap during which the world moved, a feed
that is quiet because nobody is trading. None of that is reachable through a
real socket in a test, so the seam takes a connection factory and this is what
the tests hand it.

Two details make the staging faithful rather than merely convenient:

- A callable in the script is the WORLD changing mid-connection: it is awaited
  where it sits and the stream carries on. Ownership moving to the poller, an
  operator command, a new follow — things a lane has to react to in the middle
  of a connection, which a test could otherwise only stage before or after one.
- A `None` in the script is a QUIET TICK: `receive` returns None, exactly as
  the real transport does when its timeout expires with no message. Quiet ticks
  advance the fake clock by the timeout, which is what a real one costs, so a
  run of them is how a test buys the seconds that trip a ping, a refresh, or
  the liveness deadline.
- Running out of script raises WebsocketClosed — the peer going away. That is
  how a disconnect is staged: not by a flag, but by the stream simply ending,
  which is the only thing the lane ever actually observes.
"""

from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from epigone.ws import WebsocketClosed
from tests.support.clock import FakeClock

# One entry in a staged stream: a message, a quiet tick, or something happening
# in the world at that moment (see `receive`).
Staged = dict[str, Any] | None | Callable[[], Awaitable[None]]


class FakeWebsocket:
    """One connection, playing a fixed script of messages and quiet ticks."""

    def __init__(self, clock: FakeClock, script: list[Staged]) -> None:
        self._clock = clock
        self._script = list(script)
        self.sent: list[dict[str, Any]] = []
        self.closed = False

    async def send(self, message: Mapping[str, Any]) -> None:
        if self.closed:
            raise WebsocketClosed("sent on a closed connection")
        self.sent.append(dict(message))

    async def receive(self, timeout: float) -> dict[str, Any] | None:
        while True:
            if not self._script:
                raise WebsocketClosed("staged stream ended")
            message = self._script.pop(0)
            if callable(message):
                # Something happening in the WORLD at this point in the stream,
                # rather than on the socket — the poller escalating, an operator
                # command, a follow landing. Staged inline because a lane's
                # reaction to it is a reaction to a moment, and a test that can
                # only change the world before or after a connection cannot
                # reach the middle of one (issue #158).
                await message()
                continue
            if message is None:
                self._clock.advance(timeout)
            return message

    async def close(self) -> None:
        self.closed = True

    # -- assertions the tests read the recording through ---------------------

    def subscriptions(self, channel: str) -> list[str]:
        """Addresses subscribed to `channel`, in the order they were sent. The
        liveness feed has no user, so it reports as an empty address."""
        return [
            str(message["subscription"].get("user", ""))
            for message in self.sent
            if message.get("method") == "subscribe"
            and message["subscription"]["type"] == channel
        ]

    def unsubscriptions(self, channel: str) -> list[str]:
        return [
            str(message["subscription"].get("user", ""))
            for message in self.sent
            if message.get("method") == "unsubscribe"
            and message["subscription"]["type"] == channel
        ]

    @property
    def subscription_count(self) -> int:
        """Subscriptions currently held on this connection — what the per-IP
        cap counts."""
        opened = sum(1 for message in self.sent if message.get("method") == "subscribe")
        closed = sum(1 for message in self.sent if message.get("method") == "unsubscribe")
        return opened - closed

    @property
    def pings(self) -> int:
        return sum(1 for message in self.sent if message.get("method") == "ping")


def connecting(*connections: FakeWebsocket) -> Any:
    """A connect factory handing out the given connections in order, then
    refusing — so a test that reconnects gets a genuinely NEW connection with
    no subscriptions, which is the state a reconnect actually starts from."""
    remaining = list(connections)

    async def connect() -> FakeWebsocket:
        if not remaining:
            raise WebsocketClosed("no more staged connections")
        return remaining.pop(0)

    return connect
