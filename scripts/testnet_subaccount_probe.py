"""TESTNET sub-account semantics probe (issue #142) — run explicitly:

    uv run python scripts/testnet_subaccount_probe.py

Never part of the unit suite; the suite must stay network-free. Targets
api.hyperliquid-testnet.xyz ONLY, throwaway keys from
~/.epigone/testnet-keys.json (worktree .testnet-keys.json fallback).
Requires cumVlm >= $100k (the createSubAccount gate) — farm first with
scripts/testnet_farm_volume.py.

The four questions this answers (they decide whether A4 ring-fences copy
capital in a sub-account or in a dedicated wallet):

Q-A  Does createSubAccount succeed past the $100k volume gate?
Q-B  Can the master approve an agent ON the sub-account, or are agents
     master-scoped only? Probed by submitting approveAgent with
     vaultAddress=<sub> and then reading WHERE the approval landed
     (extraAgents of sub vs master) — the readback is the decisive
     observable, not the ack. A fresh throwaway address is used so the
     outcome is distinguishable from the main agent in listings.
Q-C  Can an approved agent signer place an order FOR the sub-account (the
     SDK's vault/sub-account address mechanism)? Master-signed control run
     first, then the agent probe — same action, only the signer differs.
Q-D  Does subAccountTransfer move funds master<->sub, and is it master-only?
     (The zero-volume probe said agent signers get "does not exist" — the
     docstring warns to re-probe on a high-volume account before relying on
     the negative; this is that re-probe.)

Prep step: the throwaway agent key must be an APPROVED agent of the master
for Q-C/Q-D to mean anything. The earlier Q1 cap probe filled all 3 named
slots with generated-and-discarded keys, so this script first tries a NEW
name (bonus finding: the agent cap at $100k volume) and falls back to the
verified name-swap rotation primitive on slot probe_000.

Address fields inside L1 actions (subAccountUser, vaultAddress) are LOWERCASE
throughout — the verified first-run gotcha: the server canonicalizes before
re-hashing, so a checksummed address breaks signature recovery.

Findings land in the ExecutionGateway protocol docstring (the #63
empirical-contract convention) — this transcript is the evidence.
"""

import json
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

from eth_account import Account
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils.constants import TESTNET_API_URL
from hyperliquid.utils.signing import get_timestamp_ms, sign_agent

DURABLE_KEYS_PATH = Path.home() / ".epigone" / "testnet-keys.json"
WORKTREE_KEYS_PATH = Path(__file__).resolve().parent.parent / ".testnet-keys.json"

SUB_NAME = "epicopy"
DEPOSIT_USD_MICRO = 50_000_000  # nominally $50 if usd is micro-units — Q-D verifies
PROBE_ORDER_SIZE = 0.0003  # BTC, ~$19 at ~63.5k — clears the $10 min order value


def load_keys() -> dict[str, Any]:
    for path in (DURABLE_KEYS_PATH, WORKTREE_KEYS_PATH):
        if path.exists():
            return json.loads(path.read_text())
    sys.exit(f"no testnet keys at {DURABLE_KEYS_PATH} or {WORKTREE_KEYS_PATH}")


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


def approve_agent_address(
    exchange: Exchange, agent_address: str, name: str
) -> Any:
    """approveAgent for an EXTERNALLY-held agent address (the ceremony
    shape (a) from scripts/rehearse_agent_ceremony.py — the stock SDK
    approve_agent always generates the key itself)."""
    timestamp = get_timestamp_ms()
    action = {
        "type": "approveAgent",
        "agentAddress": agent_address,
        "agentName": name,
        "nonce": timestamp,
    }
    signature = sign_agent(exchange.wallet, action, is_mainnet=False)
    return exchange._post_action(action, signature, timestamp)


def ensure_agent_approved(info: Info, master_exchange: Exchange, agent_addr: str) -> bool:
    master_addr = master_exchange.wallet.address.lower()
    agents = info.post("/info", {"type": "extraAgents", "user": master_addr})
    if any(a["address"].lower() == agent_addr.lower() for a in agents):
        print(f"  agent {agent_addr} already approved", flush=True)
        return True
    # A new name first — doubles as the agent-cap-at-$100k data point.
    result = attempt(
        "approveAgent NEW name 'epicopy-agent' (cap probe at current volume)",
        lambda: approve_agent_address(master_exchange, agent_addr, "epicopy-agent"),
    )
    if not (isinstance(result, dict) and result.get("status") == "ok"):
        # Fall back to the verified rotation primitive: re-approving an
        # existing NAME with a new address atomically swaps that slot.
        attempt(
            "approveAgent name-swap onto slot 'probe_000'",
            lambda: approve_agent_address(master_exchange, agent_addr, "probe_000"),
        )
    time.sleep(2)
    agents = info.post("/info", {"type": "extraAgents", "user": master_addr})
    show("extraAgents(master) after approval", agents)
    return any(a["address"].lower() == agent_addr.lower() for a in agents)


def find_sub_account(info: Info, master_addr: str) -> dict[str, Any] | None:
    subs = info.post("/info", {"type": "subAccounts", "user": master_addr}) or []
    for sub in subs:
        if sub.get("name") == SUB_NAME:
            return sub
    return None


def sub_equity(info: Info, master_addr: str) -> Decimal:
    sub = find_sub_account(info, master_addr)
    if sub is None:
        return Decimal("0")
    return Decimal(sub["clearinghouseState"]["marginSummary"]["accountValue"])


def flatten_sub(label: str, exchange: Exchange) -> None:
    """Close any BTC position on the exchange's vault (sub-account)."""
    attempt(f"market_close BTC on sub ({label})", lambda: exchange.market_close("BTC"))


def main() -> None:
    assert "testnet" in TESTNET_API_URL, "this probe is testnet-ONLY"
    print(f"Hyperliquid TESTNET sub-account probe — target {TESTNET_API_URL}")
    keys = load_keys()
    master = Account.from_key(keys["master"])
    agent = Account.from_key(keys["agent"])
    master_addr = master.address.lower()
    print(f"throwaway master: {master_addr}")
    print(f"throwaway agent:  {agent.address}")

    info = Info(TESTNET_API_URL, skip_ws=True)
    rate = info.post("/info", {"type": "userRateLimit", "user": master_addr})
    print(f"cumVlm: {rate['cumVlm']} (createSubAccount gate is $100k)")

    as_master = Exchange(master, TESTNET_API_URL)

    print("\n== prep: ensure the throwaway agent is an approved agent of the master ==")
    if not ensure_agent_approved(info, as_master, agent.address):
        sys.exit("agent could not be approved — Q-C/Q-D would be meaningless, aborting")

    print("\n== Q-A: createSubAccount past the $100k gate ==")
    existing = find_sub_account(info, master_addr)
    if existing:
        show(f"sub-account '{SUB_NAME}' already exists (prior run)", existing["subAccountUser"])
    else:
        attempt(
            f"createSubAccount('{SUB_NAME}') as master",
            lambda: as_master.create_sub_account(SUB_NAME),
        )
        time.sleep(2)
        existing = find_sub_account(info, master_addr)
    if not existing:
        sys.exit("no sub-account after createSubAccount — remaining probes impossible")
    sub_addr = existing["subAccountUser"].lower()
    print(f"  sub-account address: {sub_addr}")

    print("\n== Q-D (deposit leg): subAccountTransfer master->sub, then the agent attempt ==")
    attempt(
        f"subAccountTransfer master->sub usd={DEPOSIT_USD_MICRO}",
        lambda: as_master.sub_account_transfer(sub_addr, True, DEPOSIT_USD_MICRO),
    )
    time.sleep(2)
    equity = sub_equity(info, master_addr)
    show("sub accountValue after deposit (verifies the usd units)", str(equity))
    as_agent_for_master = Exchange(agent, TESTNET_API_URL, account_address=master_addr)
    attempt(
        "subAccountTransfer signed by AGENT (re-probe of the zero-volume negative)",
        lambda: as_agent_for_master.sub_account_transfer(sub_addr, True, 1_000_000),
    )

    print("\n== Q-B: can the master approve an agent ON the sub-account? ==")
    throwaway = Account.create()
    print(f"  fresh throwaway agent for this probe: {throwaway.address}")
    as_master_vault_sub = Exchange(master, TESTNET_API_URL, vault_address=sub_addr)
    attempt(
        "approveAgent with vaultAddress=<sub> (signed by master)",
        lambda: approve_agent_address(as_master_vault_sub, throwaway.address, "subscope"),
    )
    time.sleep(2)
    attempt(
        "extraAgents(sub) — did it land on the sub?",
        lambda: info.post("/info", {"type": "extraAgents", "user": sub_addr}),
    )
    attempt(
        "extraAgents(master) — or on the master?",
        lambda: info.post("/info", {"type": "extraAgents", "user": master_addr}),
    )
    attempt("userRole(sub)", lambda: info.post("/info", {"type": "userRole", "user": sub_addr}))

    print("\n== Q-C: orders FOR the sub-account — master control, then the agent probe ==")
    attempt(
        f"market_open BTC {PROBE_ORDER_SIZE} for sub, signed by MASTER (control)",
        lambda: as_master_vault_sub.market_open("BTC", True, PROBE_ORDER_SIZE),
    )
    flatten_sub("master control", as_master_vault_sub)
    as_agent_vault_sub = Exchange(
        agent, TESTNET_API_URL, vault_address=sub_addr, account_address=master_addr
    )
    probe = attempt(
        f"market_open BTC {PROBE_ORDER_SIZE} for sub, signed by AGENT (THE probe)",
        lambda: as_agent_vault_sub.market_open("BTC", True, PROBE_ORDER_SIZE),
    )
    if isinstance(probe, dict) and probe.get("status") == "ok":
        flatten_sub("agent (close leg also via agent)", as_agent_vault_sub)
    flatten_sub("master safety-net", as_master_vault_sub)

    print("\n== Q-D (withdraw leg): move everything back master<-sub ==")
    time.sleep(2)
    remaining = sub_equity(info, master_addr)
    micro = int(remaining * 1_000_000)
    if micro > 0:
        attempt(
            f"subAccountTransfer sub->master usd={micro}",
            lambda: as_master.sub_account_transfer(sub_addr, False, micro),
        )
        time.sleep(2)
    show("sub accountValue after withdraw", str(sub_equity(info, master_addr)))

    print("\n== bonus: scheduleCancel at current volume (gate string should name Traded) ==")
    attempt(
        "scheduleCancel(+70s) as master",
        lambda: as_master.schedule_cancel(get_timestamp_ms() + 70_000),
    )
    attempt("scheduleCancel(None) as master", lambda: as_master.schedule_cancel(None))

    print("\ndone — paste the relevant lines into the protocol docstring findings")


if __name__ == "__main__":
    main()
