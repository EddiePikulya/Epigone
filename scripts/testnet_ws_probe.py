"""Probe Hyperliquid's websocket allowances (issue #157).

The two unknowns this settled, and the numbers it produced on 2026-08-02, are
recorded in docs/research/ecosystem-survey.md. This script is kept so the
findings are reproducible rather than folklore — the allowances are
undocumented and can change, and the lane's constants (PING_INTERVAL_SECONDS,
LIVENESS_TIMEOUT_SECONDS, MAX_SUBSCRIBED_TRADERS) are sized off them.

    uv run python scripts/testnet_ws_probe.py idle      # ping/pong + idle timeout
    uv run python scripts/testnet_ws_probe.py inbound   # does inbound count?
    uv run python scripts/testnet_ws_probe.py names     # which subscriptions exist

Read-only throughout: market data and public account state, no keys, no signed
actions. `inbound` runs against MAINNET because testnet is far too quiet to
reach the message volumes in question — the allowances are per-IP and
account-independent, so that is the right place to ask.
"""

import asyncio
import json
import sys
import time
from typing import Any

import aiohttp

MAINNET_WS = "wss://api.hyperliquid.xyz/ws"
TESTNET_WS = "wss://api.hyperliquid-testnet.xyz/ws"
MAINNET_INFO = "https://api.hyperliquid.xyz/info"

# The funded testnet harness's master account (issue #133). Public address
# only — the keys live in ~/.epigone/testnet-keys.json and are never read here.
HARNESS_ADDRESS = "0x971cb6b3f22bc5ea90d822306a07145628ee9be6"

PING_EVERY = 25.0


async def _drain(
    ws: aiohttp.ClientWebSocketResponse, seconds: float, *, ping: bool
) -> tuple[int, int, str]:
    """Receive for `seconds`, optionally pinging. Returns (inbound, outbound,
    outcome)."""
    started = time.monotonic()
    inbound = outbound = 0
    last_ping = started
    while time.monotonic() - started < seconds:
        if ping and time.monotonic() - last_ping >= PING_EVERY:
            await ws.send_json({"method": "ping"})
            outbound += 1
            last_ping = time.monotonic()
        try:
            message = await ws.receive(timeout=1.0)
        except TimeoutError:
            continue
        if message.type is not aiohttp.WSMsgType.TEXT:
            elapsed = time.monotonic() - started
            return inbound, outbound, f"CLOSED at {elapsed:.1f}s (code {ws.close_code})"
        inbound += 1
    return inbound, outbound, f"SURVIVED {seconds:.0f}s"


async def probe_idle() -> None:
    """Is there a ping/pong, and is there an idle timeout?

    Three connections, none of them receiving anything to speak of: bare (no
    subscriptions, never speaks), quiet (one subscription an idle account never
    emits on), and ping (pings once at the start). 2026-08-02: all three cut at
    60.6s with close code 1006, and the ping got a {"channel": "pong"} back."""

    async def one(name: str, subs: list[dict[str, Any]], ping_once: bool) -> None:
        async with aiohttp.ClientSession() as session, session.ws_connect(
            TESTNET_WS, heartbeat=None
        ) as ws:
            for sub in subs:
                await ws.send_json({"method": "subscribe", "subscription": sub})
            if ping_once:
                await ws.send_json({"method": "ping"})
            inbound, _, outcome = await _drain(ws, 180, ping=False)
            print(f"{name:6}: {outcome}, {inbound} inbound", flush=True)

    await asyncio.gather(
        one("bare", [], False),
        one("quiet", [{"type": "orderUpdates", "user": HARNESS_ADDRESS}], False),
        one("ping", [], True),
    )


async def probe_inbound() -> None:
    """Does the 2000 messages/min allowance count inbound pushes?

    Drive inbound far past 2000/min on one mainnet connection while sending
    only a keepalive. 2026-08-02: 6702/min sustained over 200s (peak minute
    7497), 0 error frames, never cut — so the allowance is outbound-only."""
    async with aiohttp.ClientSession() as session:
        async with session.post(MAINNET_INFO, json={"type": "meta"}) as response:
            universe = (await response.json())["universe"]
        coins = [entry["name"] for entry in universe if not entry.get("isDelisted")][:70]
        subs: list[dict[str, Any]] = [{"type": "allMids"}]
        for coin in coins:
            subs += [
                {"type": "l2Book", "coin": coin},
                {"type": "bbo", "coin": coin},
                {"type": "trades", "coin": coin},
            ]
        async with session.ws_connect(MAINNET_WS, heartbeat=None) as ws:
            for sub in subs:
                await ws.send_json({"method": "subscribe", "subscription": sub})
                await asyncio.sleep(0.02)
            print(f"{len(subs)} subscriptions; keepalive pings only", flush=True)
            inbound, outbound, outcome = await _drain(ws, 200, ping=True)
            print(
                f"{outcome}: {inbound} inbound ({inbound / 200 * 60:.0f}/min), "
                f"{len(subs) + outbound} outbound",
                flush=True,
            )


async def probe_names() -> None:
    """Which user subscriptions actually exist, and what do they push?

    2026-08-02: allDexsClearinghouseState works and carries every dex as
    [name, state] pairs (core under ""); allDexsOrderUpdates is REJECTED as
    unparseable; orderUpdates takes no dex. The positions feed pushed every
    ~5s on an account holding nothing at all."""
    candidates = [
        {"type": "allDexsClearinghouseState", "user": HARNESS_ADDRESS},
        {"type": "allDexsOrderUpdates", "user": HARNESS_ADDRESS},
        {"type": "orderUpdates", "user": HARNESS_ADDRESS},
    ]
    async with aiohttp.ClientSession() as session, session.ws_connect(
        TESTNET_WS, heartbeat=None
    ) as ws:
        for sub in candidates:
            await ws.send_json({"method": "subscribe", "subscription": sub})
        started = time.monotonic()
        pushes: list[float] = []
        while time.monotonic() - started < 60:
            try:
                message = await ws.receive(timeout=1.0)
            except TimeoutError:
                continue
            if message.type is not aiohttp.WSMsgType.TEXT:
                break
            body = json.loads(message.data)
            channel = body.get("channel")
            if channel == "allDexsClearinghouseState":
                pushes.append(round(time.monotonic() - started, 1))
            elif channel in ("error", "subscriptionResponse"):
                print(f"{channel}: {json.dumps(body)[:220]}", flush=True)
        print(f"allDexsClearinghouseState pushes on an idle account: t={pushes}", flush=True)


PROBES = {"idle": probe_idle, "inbound": probe_inbound, "names": probe_names}


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else ""
    if which not in PROBES:
        raise SystemExit(f"usage: {sys.argv[0]} {{{'|'.join(PROBES)}}}")
    asyncio.run(PROBES[which]())
