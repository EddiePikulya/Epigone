"""Probe Hyperliquid's websocket allowances (issue #157).

The two unknowns this settled, and the numbers it produced on 2026-08-02, are
recorded in docs/research/ecosystem-survey.md. This script is kept so the
findings are reproducible rather than folklore — the allowances are
undocumented and can change, and the lane's constants (PING_INTERVAL_SECONDS,
LIVENESS_TIMEOUT_SECONDS, MAX_SUBSCRIBED_TRADERS) are sized off them.

    uv run python scripts/testnet_ws_probe.py idle      # ping/pong + idle timeout
    uv run python scripts/testnet_ws_probe.py inbound   # does inbound count?
    uv run python scripts/testnet_ws_probe.py names     # which subscriptions exist
    uv run python scripts/testnet_ws_probe.py users     # the per-IP unique-user cap
    uv run python scripts/testnet_ws_probe.py orders    # what orderUpdates carries

The last two are issue #168's, and they are the load-bearing ones for the order
seam (ADR-0007): `users` establishes that the real ceiling on any lane is 15
unique addresses per IP, and `orders` establishes that an order update never
names the user it belongs to.

Read-only throughout: market data and public account state, no keys, no signed
actions. `inbound`, `users` and `orders` run against MAINNET because testnet is
far too quiet to reach the volumes in question — the allowances are per-IP and
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


async def _harvest_active(session: aiohttp.ClientSession, wanted: int) -> list[str]:
    """Addresses that are actually trading right now, off the public trades
    feed (it carries both counterparties). Busiest-first, which is deliberate:
    these are the market makers, i.e. the worst case any order-persistence seam
    has to survive."""
    seen: dict[str, int] = {}
    async with session.ws_connect(MAINNET_WS, heartbeat=None) as ws:
        for coin in ("BTC", "ETH", "SOL"):
            await ws.send_json(
                {"method": "subscribe", "subscription": {"type": "trades", "coin": coin}}
            )
        started = time.monotonic()
        while time.monotonic() - started < 20:
            try:
                message = await ws.receive(timeout=1.0)
            except TimeoutError:
                continue
            if message.type is not aiohttp.WSMsgType.TEXT:
                break
            body = json.loads(message.data)
            if body.get("channel") != "trades":
                continue
            for trade in body["data"]:
                for user in trade.get("users", []):
                    address = str(user).lower()
                    seen[address] = seen.get(address, 0) + 1
    return sorted(seen, key=lambda address: -seen[address])[:wanted]


async def _ask(ws: aiohttp.ClientWebSocketResponse, method: str, sub: dict[str, Any]) -> str:
    """One subscribe/unsubscribe and the server's verdict on it. Sent one at a
    time so the answer is unambiguously about THIS subscription — an error frame
    names no subscription, so a batch would be uncorrelatable."""
    await ws.send_json({"method": method, "subscription": sub})
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline:
        try:
            message = await ws.receive(timeout=0.5)
        except TimeoutError:
            continue
        if message.type is not aiohttp.WSMsgType.TEXT:
            return f"CLOSED({ws.close_code})"
        body = json.loads(message.data)
        if body.get("channel") == "subscriptionResponse":
            return "ok"
        if body.get("channel") == "error":
            return f"refused({body['data'][:70]})"
    return "no answer"


async def probe_users() -> None:
    """How many unique users may one IP track, and is the allowance per IP or
    per connection?

    2026-08-03: **15 unique users, per IP** — a fresh second connection is
    refused a brand-new address while the first holds its 15
    ("Cannot track more than 15 total users."). Unsubscribing frees a slot at
    once; closing a connection frees all of its slots within ~2s; and the same
    address on a second connection is free, so splitting one Trader's feeds
    across connections costs no allowance. This retires ADR-0006's "499
    Traders on one connection"."""
    orders = lambda address: {"type": "orderUpdates", "user": address}  # noqa: E731
    async with aiohttp.ClientSession() as session:
        addresses = await _harvest_active(session, 40)
        print(f"harvested {len(addresses)} active addresses; settling 30s", flush=True)
        await asyncio.sleep(30)

        first = await session.ws_connect(MAINNET_WS, heartbeat=None)
        held: list[str] = []
        for address in addresses:
            if await _ask(first, "subscribe", orders(address)) != "ok":
                break
            held.append(address)
        print(f"one connection accepted {len(held)} unique users", flush=True)

        second = await session.ws_connect(MAINNET_WS, heartbeat=None)
        fresh = [address for address in addresses if address not in held]
        verdict = await _ask(second, "subscribe", orders(fresh[0]))
        print(f"brand-new user on a SECOND connection -> {verdict}", flush=True)
        print(f"=> allowance is {'PER CONNECTION' if verdict == 'ok' else 'PER IP'}", flush=True)
        again = await _ask(second, "subscribe", orders(held[0]))
        print(f"already-held user on the second      -> {again}", flush=True)

        await first.close()
        await asyncio.sleep(2)
        freed = await _ask(second, "subscribe", orders(fresh[0]))
        print(f"2s after closing the first, new user -> {freed}", flush=True)
        await second.close()


async def probe_orders() -> None:
    """What does an orderUpdates frame carry, and how much of it is there?

    2026-08-03: the frame carries `{"order": {coin, side, limitPx, sz, oid,
    timestamp, origSz, cloid}, "status", "statusTimestamp"}` and **no user
    field at any level** — so on a connection subscribed to several users the
    frames cannot be attributed, which is the constraint ADR-0007 is built
    around. One market-making address alone: 442 frames / 1471 updates in 60s.
    """
    async with aiohttp.ClientSession() as session:
        addresses = await _harvest_active(session, 1)
        async with session.ws_connect(MAINNET_WS, heartbeat=None) as ws:
            target = addresses[0]
            verdict = await _ask(ws, "subscribe", {"type": "orderUpdates", "user": target})
            print(f"watching {target} alone for 60s -> {verdict}", flush=True)
            frames = updates = 0
            statuses: dict[str, int] = {}
            started = time.monotonic()
            while time.monotonic() - started < 60:
                try:
                    message = await ws.receive(timeout=1.0)
                except TimeoutError:
                    continue
                if message.type is not aiohttp.WSMsgType.TEXT:
                    break
                body = json.loads(message.data)
                if body.get("channel") != "orderUpdates":
                    continue
                if not frames:
                    print(f"RAW: {json.dumps(body)[:400]}", flush=True)
                frames += 1
                for update in body["data"]:
                    updates += 1
                    statuses[update["status"]] = statuses.get(update["status"], 0) + 1
            print(f"{frames} frames, {updates} order updates in 60s", flush=True)
            print(f"statuses: {json.dumps(statuses)}", flush=True)


PROBES = {
    "idle": probe_idle,
    "inbound": probe_inbound,
    "names": probe_names,
    "users": probe_users,
    "orders": probe_orders,
}


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else ""
    if which not in PROBES:
        raise SystemExit(f"usage: {sys.argv[0]} {{{'|'.join(PROBES)}}}")
    asyncio.run(PROBES[which]())
