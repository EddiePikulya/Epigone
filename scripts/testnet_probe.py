"""TESTNET harness for the ExecutionGateway (issue #133) — run explicitly:

    uv run python scripts/testnet_probe.py

Never part of the unit suite; the suite must stay network-free. Everything
here targets api.hyperliquid-testnet.xyz ONLY, with throwaway keys generated
locally into .testnet-keys.json (gitignored, 0600). Mainnet keys are never
generated, held, or requested (issue #133 key-safety scope).

What it does, and why each probe is shaped the way it is:

E2E  Signing sanity through OUR gateway: submit a signed action from the
     throwaway agent and read the exchange's error back. The load-bearing
     detail: "User or API Wallet 0x… does not exist" names the address the
     EXCHANGE recovered from our signature — if our wire construction (key
     order, msgpack, phantom agent) were wrong in any way, the recovered
     address would be garbage, not our agent. The error string is therefore
     a signature oracle that needs no funded account.

Q1   Agent-count cap (research §11 open Q1): the docs' 1-unnamed+3-named
     formula contradicts live mainnet probes (26–103 agents). Approve named
     agents from the throwaway master until the exchange refuses; report
     the count and the refusal string.

Q2   scheduleCancel volume gate (open Q2 — A3's contingency hangs on this):
     secondary sources claim a $1M cumulative-volume eligibility gate the
     official docs don't mention. A zero-volume account submitting
     scheduleCancel gets an answer either way: accepted → no gate; refused
     naming volume → gate confirmed, string recorded.

Q3   Gray-zone L1 actions from an agent signer (open Q3): createSubAccount /
     subAccountTransfer / vaultTransfer are L1-signed in the SDK but the
     docs don't say whether the chain accepts them from an agent. Method:
     submit each twice — once signed by the MASTER (control), once by the
     AGENT (probe) — and compare the error strings; a signer-specific
     refusal on the agent run that the master run doesn't show is the
     answer. (The master signing here is the harness acting as the USER's
     side of the ceremony on a throwaway testnet wallet — the thing ADR-0005
     forbids is master keys on Epigone servers, not in this local probe.)

Findings land in the ExecutionGateway protocol docstring (the #63
empirical-contract convention) — this transcript is the evidence.
"""

import asyncio
import json
import os
import sys
import time
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import aiohttp
from eth_account import Account
from hyperliquid.exchange import Exchange

from epigone.budget import WeightBudget
from epigone.clock import SystemClock
from epigone.gateway.execution import ExecutionError, OrderSpec
from epigone.gateway.execution_http import TESTNET_EXCHANGE_URL, HttpExecutionGateway

TESTNET_BASE = "https://api.hyperliquid-testnet.xyz"
KEYS_PATH = Path(__file__).resolve().parent.parent / ".testnet-keys.json"

# How many named-agent approvals Q1 attempts before concluding "no cap hit".
Q1_MAX_APPROVALS = 30


def load_or_create_keys() -> dict[str, Any]:
    if KEYS_PATH.exists():
        return json.loads(KEYS_PATH.read_text())
    keys = {
        "comment": "THROWAWAY TESTNET keys (issue #133 harness). Never funded on mainnet.",
        "master": Account.create().key.hex(),
        "agent": Account.create().key.hex(),
    }
    KEYS_PATH.write_text(json.dumps(keys, indent=2))
    os.chmod(KEYS_PATH, 0o600)
    return keys


def show(label: str, outcome: Any) -> None:
    print(f"  {label}: {outcome!r}", flush=True)


def attempt(label: str, call: Any) -> Any:
    """Run a probe call, print and return its outcome (result or exception)."""
    try:
        result = call()
    except Exception as exc:  # noqa: BLE001 - a harness records, never crashes
        show(label, exc)
        return exc
    show(label, result)
    return result


async def probe_e2e_and_q2(master_addr: str, agent: Any) -> None:
    print("\n== E2E: our gateway signs; the exchange's error is the signature oracle ==")
    async with aiohttp.ClientSession() as session:
        clock = SystemClock()
        gateway = HttpExecutionGateway(
            session,
            clock,
            WeightBudget(1200, clock),
            signer=agent,
            master_address=master_addr,
            exchange_url=TESTNET_EXCHANGE_URL,
        )
        try:
            results = await gateway.place_orders(
                # BTC (asset 3 on testnet meta) far-below-market Gtc — could
                # never fill even if the account were somehow funded.
                [
                    OrderSpec(
                        asset=3,
                        is_buy=True,
                        size=Decimal("0.001"),
                        limit_price=Decimal("1000"),
                    )
                ]
            )
            show("place_orders (expect typed results or reject)", results)
        except ExecutionError as exc:
            show("place_orders (expect the error to NAME our agent address)", exc)
            print(f"  agent address for comparison: {agent.address}")

        print("\n== Q2: scheduleCancel from a zero-volume account ==")
        at = datetime.now(tz=UTC) + timedelta(seconds=70)
        try:
            await gateway.schedule_cancel(at)
            show("scheduleCancel(+70s)", "ACCEPTED — no volume gate for this account")
        except ExecutionError as exc:
            show("scheduleCancel(+70s)", exc)
        try:
            await gateway.schedule_cancel(None)
            show("scheduleCancel(None)", "ACCEPTED (schedule removed)")
        except ExecutionError as exc:
            show("scheduleCancel(None)", exc)


def probe_q1_agent_cap(master: Any) -> None:
    print("\n== Q1: approve named agents until the exchange refuses ==")
    exchange = Exchange(master, TESTNET_BASE)
    approved = 0
    for i in range(Q1_MAX_APPROVALS):
        name = f"probe_{i:03d}"
        result = attempt(f"approveAgent name={name}", lambda n=name: exchange.approve_agent(n)[0])
        if isinstance(result, Exception):
            break
        if isinstance(result, dict) and result.get("status") != "ok":
            print(f"  -> refused after {approved} approvals")
            break
        approved += 1
        time.sleep(0.3)
    else:
        print(f"  -> {approved} named agents approved without hitting a cap")
    print(f"  total approved this run: {approved}")


def probe_q3_gray_zone(master: Any, agent: Any, master_addr: str) -> None:
    print("\n== Q3: gray-zone L1 actions — master (control) vs agent (probe) ==")
    # EMPIRICAL (first run, 2026-07-27): address fields INSIDE an L1 action
    # must be lowercase — the server canonicalizes before re-hashing, so a
    # checksummed address makes the signature recover to a garbage address
    # server-side ("User or API Wallet 0x<garbage> does not exist").
    address = master_addr.lower()
    as_master = Exchange(master, TESTNET_BASE)
    as_agent = Exchange(agent, TESTNET_BASE, account_address=address)
    for label, exchange in (("master", as_master), ("agent", as_agent)):
        attempt(
            f"createSubAccount ({label})",
            lambda e=exchange, n=f"pr{label[:2]}": e.create_sub_account(n),
        )
        attempt(
            f"subAccountTransfer ({label})",
            # A self-referencing dummy sub-account address: if the signer is
            # refused, the error says so before the address is even resolved.
            lambda e=exchange: e.sub_account_transfer(address, True, 1),
        )
        attempt(
            f"vaultTransfer ({label})",
            lambda e=exchange: e.vault_usd_transfer(address, True, 1),
        )


async def check_funding(master_addr: str) -> bool:
    """Whether the throwaway master exists/has equity on testnet. The drip
    faucet is an on-chain claimDrip() on Arbitrum Sepolia (needs gas there),
    so funding is a one-time manual step — the probes that need an existing
    account report the wall honestly until it happens."""
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{TESTNET_BASE}/info",
            json={"type": "clearinghouseState", "user": master_addr.lower()},
        ) as response:
            state = await response.json()
    value = Decimal(state.get("marginSummary", {}).get("accountValue", "0"))
    print(f"testnet accountValue for throwaway master: {value}")
    return value > 0


async def main() -> None:
    print(f"Hyperliquid TESTNET probe — {datetime.now(tz=UTC).isoformat()}")
    print(f"target: {TESTNET_BASE} (testnet ONLY)")
    keys = load_or_create_keys()
    master = Account.from_key(keys["master"])
    agent = Account.from_key(keys["agent"])
    print(f"throwaway master: {master.address}")
    print(f"throwaway agent:  {agent.address}")
    funded = await check_funding(master.address)
    if not funded:
        print(
            "\nNOT FUNDED: the exchange answers 'Must deposit before performing"
            " actions' / 'does not exist' to every account-bound probe. To"
            " complete Q1-Q3, send this throwaway master mock USDC on TESTNET"
            " (app.hyperliquid-testnet.xyz faucet from a wallet with mainnet"
            " history, then usdSend to the address above) and re-run. The"
            " signature-oracle E2E check below is meaningful regardless."
        )

    await probe_e2e_and_q2(master.address, agent)
    probe_q1_agent_cap(master)
    probe_q3_gray_zone(master, agent, master.address)
    print("\ndone — paste the relevant lines into the protocol docstring findings")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
