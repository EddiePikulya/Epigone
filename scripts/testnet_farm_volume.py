"""TESTNET volume farmer (issue #142) — run explicitly:

    uv run python scripts/testnet_farm_volume.py --target 100000

Never part of the unit suite; the suite must stay network-free. Everything
here targets api.hyperliquid-testnet.xyz ONLY (asserted at startup), with the
throwaway keys from ~/.epigone/testnet-keys.json (gitignored, 0600; worktree
.testnet-keys.json as fallback). Mainnet keys are never generated, held, or
requested.

Why this exists: two Hyperliquid gates are volume-scaled — createSubAccount
at $100k cumulative volume and scheduleCancel at $1M (both verified, see the
TESTNET FINDINGS block in src/epigone/gateway/execution.py). The sub-account
probes that feed A4's blast-radius design are unreachable at zero volume, so
this script buys volume with mock USDC: open a position, close it
reduce-only, repeat, until `userRateLimit.cumVlm` crosses --target.

This is operational tooling signed by the throwaway MASTER key — it must NOT
go through ExecutionGateway, which is agent-key-only by construction
(ADR-0005) and refuses a master signer at the door. The SDK is used directly,
exactly like scripts/testnet_probe.py.

Safety posture (all hard stops, any one aborts the run after flattening):
  - testnet URL asserted, mainnet structurally unreachable
  - equity floor (--equity-floor): stop before the account drains
  - consecutive-failure abort (--max-failures)
  - max iterations (--max-cycles)
  - positions are sized to the VISIBLE top-of-book depth re-read every cycle
    (the testnet book is thin — ~$1-5k in the top 3 levels; oversizing just
    burns equity as slippage), and closes are reduce-only
  - the account is verified FLAT after every cycle; a position that survives
    3 close attempts aborts the run
"""

import argparse
import json
import sys
import time
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal
from pathlib import Path
from typing import Any

from eth_account import Account
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils.constants import TESTNET_API_URL

DURABLE_KEYS_PATH = Path.home() / ".epigone" / "testnet-keys.json"
WORKTREE_KEYS_PATH = Path(__file__).resolve().parent.parent / ".testnet-keys.json"

# Sizing: take at most this fraction of the visible top-3 depth on the side
# we are about to cross — the rest is headroom for the book moving between
# the snapshot and the order landing.
DEPTH_FRACTION = Decimal("0.5")
TOP_LEVELS = 3
# Below this notional the book is effectively empty — skip the cycle and let
# the testnet market makers refill instead of sweeping dust levels.
MIN_CYCLE_NOTIONAL = Decimal("25")
CLOSE_RETRIES = 5
# Pause after a transient API failure (the testnet 500s/502s in bursts) —
# long enough to ride out a blip, short enough that --max-failures still
# aborts a real outage in under a minute.
FAILURE_BACKOFF_S = 5.0


def load_keys() -> dict[str, Any]:
    for path in (DURABLE_KEYS_PATH, WORKTREE_KEYS_PATH):
        if path.exists():
            return json.loads(path.read_text())
    sys.exit(
        f"no testnet keys found at {DURABLE_KEYS_PATH} or {WORKTREE_KEYS_PATH} — "
        "this farmer needs the already-funded throwaway master (issue #142), "
        "it never generates keys itself"
    )


@dataclass
class AccountSnapshot:
    equity: Decimal
    cum_vlm: Decimal
    n_requests_used: int
    position_size: Decimal  # signed szi for --coin; 0 when flat


class Farmer:
    def __init__(self, args: argparse.Namespace) -> None:
        assert "testnet" in TESTNET_API_URL, "volume farming is testnet-ONLY"
        keys = load_keys()
        master = Account.from_key(keys["master"])
        self.address = master.address.lower()
        self.coin = args.coin
        self.args = args
        self.info = Info(TESTNET_API_URL, skip_ws=True)
        self.exchange = Exchange(master, TESTNET_API_URL)
        meta = self.info.meta()
        self.sz_decimals = {
            entry["name"]: int(entry["szDecimals"]) for entry in meta["universe"]
        }[self.coin]

    def snapshot(self) -> AccountSnapshot:
        state = self.info.user_state(self.address)
        rate = self.info.post("/info", {"type": "userRateLimit", "user": self.address})
        size = Decimal("0")
        for position in state["assetPositions"]:
            if position["position"]["coin"] == self.coin:
                size = Decimal(position["position"]["szi"])
        return AccountSnapshot(
            equity=Decimal(state["marginSummary"]["accountValue"]),
            cum_vlm=Decimal(rate["cumVlm"]),
            n_requests_used=int(rate["nRequestsUsed"]),
            position_size=size,
        )

    def crossable_notional(self, is_buy: bool) -> tuple[Decimal, Decimal]:
        """(top-3 visible notional on the side we'd cross, best price there)."""
        bids, asks = self.info.l2_snapshot(self.coin)["levels"]
        levels = (asks if is_buy else bids)[:TOP_LEVELS]
        if not levels:
            return Decimal("0"), Decimal("0")
        notional = sum(
            (Decimal(level["px"]) * Decimal(level["sz"]) for level in levels),
            Decimal("0"),
        )
        return notional, Decimal(levels[0]["px"])

    def cycle_size(self, is_buy: bool) -> Decimal:
        """Depth-bounded position size in coin units; 0 means 'book too thin'."""
        depth, best_px = self.crossable_notional(is_buy)
        notional = min(Decimal(self.args.max_notional), depth * DEPTH_FRACTION)
        if notional < MIN_CYCLE_NOTIONAL or best_px == 0:
            return Decimal("0")
        quantum = Decimal(1).scaleb(-self.sz_decimals)
        return (notional / best_px).quantize(quantum, rounding=ROUND_DOWN)

    @staticmethod
    def filled_notional(response: Any) -> tuple[Decimal, str]:
        """Sum the filled notional out of an order response; '' error if ok."""
        if not isinstance(response, dict) or response.get("status") != "ok":
            return Decimal("0"), f"rejected: {response!r}"
        total = Decimal("0")
        errors = []
        for status in response["response"]["data"]["statuses"]:
            if "filled" in status:
                fill = status["filled"]
                total += Decimal(fill["totalSz"]) * Decimal(fill["avgPx"])
            elif "error" in status:
                errors.append(status["error"])
        return total, "; ".join(errors)

    def ensure_flat(self) -> bool:
        """Reduce-only-close until flat; True if flat, False after retries.

        Exception-tolerant on purpose: this runs in the abort/finally paths,
        where a flapping testnet API must not be able to leave a position
        open just because one status poll 502'd."""
        for attempt in range(CLOSE_RETRIES):
            try:
                if self.snapshot().position_size == 0:
                    return True
                response = self.exchange.market_close(self.coin, slippage=self.args.slippage)
                if response is not None:
                    _, error = self.filled_notional(response)
                    if error:
                        print(f"  close attempt {attempt + 1}: {error}", flush=True)
            except Exception as exc:  # noqa: BLE001 - transient API failure
                print(f"  close attempt {attempt + 1}: API error {exc!r}", flush=True)
            time.sleep(1.0)
        try:
            return self.snapshot().position_size == 0
        except Exception:  # noqa: BLE001
            return False

    def run(self) -> int:
        start = self.snapshot()
        target = Decimal(self.args.target)
        print(
            f"farming {self.coin} on {TESTNET_API_URL} as {self.address}\n"
            f"start: cumVlm ${start.cum_vlm:,.0f} target ${target:,.0f} | "
            f"equity ${start.equity:,.2f} floor ${self.args.equity_floor:,.2f}",
            flush=True,
        )
        if start.cum_vlm >= target:
            print("target already reached — nothing to farm", flush=True)
            return 0
        if not self.ensure_flat():
            print("ABORT: could not flatten pre-existing position", flush=True)
            return 1

        failures = 0
        outcome = 1
        try:
            for cycle in range(1, self.args.max_cycles + 1):
                try:
                    is_buy = cycle % 2 == 1  # alternate long/short: drift-neutral
                    size = self.cycle_size(is_buy)
                    if size == 0:
                        failures += 1
                        print(f"cycle {cycle}: book too thin, waiting", flush=True)
                    else:
                        response = self.exchange.market_open(
                            self.coin, is_buy, float(size), None, self.args.slippage
                        )
                        opened, error = self.filled_notional(response)
                        if not self.ensure_flat():
                            print("ABORT: position would not close reduce-only", flush=True)
                            return 1
                        if opened == 0:
                            failures += 1
                            print(f"cycle {cycle}: no fill ({error or 'IOC missed'})", flush=True)
                        else:
                            failures = 0

                    now = self.snapshot()
                    burned = start.equity - now.equity
                    gained = now.cum_vlm - start.cum_vlm
                    rate_bp = (burned / gained * 10_000) if gained else Decimal("0")
                    print(
                        f"cycle {cycle}: cumVlm ${now.cum_vlm:,.0f} / ${target:,.0f} | "
                        f"equity ${now.equity:,.2f} | burned ${burned:,.2f} "
                        f"({rate_bp:.1f} bp/$) | requests {now.n_requests_used}",
                        flush=True,
                    )
                    if now.cum_vlm >= target:
                        print(f"TARGET REACHED: cumVlm ${now.cum_vlm:,.2f}", flush=True)
                        outcome = 0
                        break
                    if now.equity < self.args.equity_floor:
                        print(f"STOP: equity ${now.equity:,.2f} under floor", flush=True)
                        break
                except Exception as exc:  # noqa: BLE001 - the testnet 500s in bursts
                    failures += 1
                    print(
                        f"cycle {cycle}: transient API error "
                        f"({failures}/{self.args.max_failures}): {exc!r}",
                        flush=True,
                    )
                    self.ensure_flat()  # exception-tolerant; never trust an open leg
                    time.sleep(FAILURE_BACKOFF_S)
                if failures >= self.args.max_failures:
                    print(f"STOP: {failures} consecutive failures", flush=True)
                    break
                time.sleep(self.args.sleep)
            else:
                print(f"STOP: max cycles ({self.args.max_cycles}) reached", flush=True)
        finally:
            flat = self.ensure_flat()
            try:
                end = self.snapshot()
                print(
                    f"\ndone: flat={flat} | cumVlm ${end.cum_vlm:,.2f} | "
                    f"equity ${end.equity:,.2f} (total burn "
                    f"${start.equity - end.equity:,.2f} for "
                    f"${end.cum_vlm - start.cum_vlm:,.0f} of volume)",
                    flush=True,
                )
            except Exception as exc:  # noqa: BLE001
                print(f"\ndone: flat={flat} | final snapshot unavailable: {exc!r}", flush=True)
        return outcome


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", type=float, required=True, help="cumVlm target in USD")
    parser.add_argument("--coin", default="BTC")
    parser.add_argument("--max-notional", type=float, default=3000.0)
    parser.add_argument("--equity-floor", type=float, default=500.0)
    parser.add_argument("--max-cycles", type=int, default=200)
    parser.add_argument("--max-failures", type=int, default=5)
    parser.add_argument("--slippage", type=float, default=0.01)
    parser.add_argument("--sleep", type=float, default=0.5, help="pause between cycles (s)")
    return parser.parse_args()


if __name__ == "__main__":
    sys.exit(Farmer(parse_args()).run())
