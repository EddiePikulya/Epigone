"""The production WebsocketConnection: an aiohttp websocket to Hyperliquid.

Thin on purpose (see epigone.ws): JSON in, JSON out, and one honest signal for
"the peer is gone". Every way aiohttp can report that — a CLOSE/CLOSING/CLOSED
frame, an ERROR frame, a transport exception, a send after the socket died —
arrives at the lane as WebsocketClosed, so the lane has exactly one path to
write for it. That is the same discipline the REST gateway settled on after a
missed timeout class killed the stream process (issue #163): one degradation
path, named, with nothing leaking around it.

A note on what this module does NOT do: it does not decide that a quiet socket
is a dead socket. `receive` returning None means "nothing arrived in this
tick", and only the lane, which knows it is subscribed to a feed that always
emits, can turn a run of those into a verdict.
"""

import logging
from collections.abc import Mapping
from typing import Any

import aiohttp

from epigone.ws import WS_URL, WebsocketClosed

log = logging.getLogger(__name__)

# aiohttp's own heartbeat is left off: Hyperliquid closes a connection that has
# been idle in BOTH directions for 60s (measured on testnet 2026-08-02 — three
# connections, all cut at 60.6s), and the lane sends its own application-level
# {"method": "ping"} well inside that. Two keepalive mechanisms would only make
# it harder to reason about which one is holding the socket open.
_CLOSING_TYPES = frozenset(
    {
        aiohttp.WSMsgType.CLOSE,
        aiohttp.WSMsgType.CLOSING,
        aiohttp.WSMsgType.CLOSED,
        aiohttp.WSMsgType.ERROR,
    }
)


class AiohttpWebsocketConnection:
    def __init__(self, socket: aiohttp.ClientWebSocketResponse) -> None:
        self._socket = socket

    async def send(self, message: Mapping[str, Any]) -> None:
        try:
            await self._socket.send_json(dict(message))
        except (TimeoutError, aiohttp.ClientError, ConnectionResetError, RuntimeError) as exc:
            raise WebsocketClosed(f"send failed: {exc}") from exc

    async def receive(self, timeout: float) -> dict[str, Any] | None:
        try:
            message = await self._socket.receive(timeout=timeout)
        except TimeoutError:
            return None
        except (aiohttp.ClientError, ConnectionResetError) as exc:
            raise WebsocketClosed(f"receive failed: {exc}") from exc
        if message.type in _CLOSING_TYPES:
            raise WebsocketClosed(
                f"peer closed: type={message.type} code={self._socket.close_code}"
            )
        if message.type is not aiohttp.WSMsgType.TEXT:
            # Binary/continuation frames are not part of this protocol; skip the
            # tick rather than fail the connection over one odd frame.
            log.debug("ignoring non-text websocket frame: %s", message.type)
            return None
        parsed: dict[str, Any] = message.json()
        return parsed

    async def close(self) -> None:
        try:
            await self._socket.close()
        except Exception:  # noqa: BLE001 — closing must never raise into cleanup
            log.debug("websocket close failed", exc_info=True)


async def connect(
    session: aiohttp.ClientSession, url: str = WS_URL
) -> AiohttpWebsocketConnection:
    """Open one websocket. Any failure to get connected is WebsocketClosed, so
    a connect failure and a mid-life disconnect take the lane down the same
    reconnect path."""
    try:
        socket = await session.ws_connect(url, heartbeat=None)
    except (TimeoutError, aiohttp.ClientError) as exc:
        raise WebsocketClosed(f"connect to {url} failed: {exc}") from exc
    return AiohttpWebsocketConnection(socket)
