"""The ExecutionGateway seam: the write-side twin of the read gateway.

ALL Hyperliquid exchange writes — orders, cancels, modifies, TP/SL legs,
leverage, scheduleCancel — go through this interface and nowhere else
(ADR-0005; the ADR-0001 shape extended to writes, issue #133). Tests inject
FakeExecutionGateway; production wires HttpExecutionGateway.

Agent-key-only (the ADR-0005 invariant), enforced in layers — and stated
precisely, because the layers guarantee different things:

- IMPOSSIBLE BY CONSTRUCTION: moving funds or granting authority. The
  surface below contains ONLY L1 trading actions; every fund-moving or
  authority-granting action (`withdraw3`, `usdSend`, `approveAgent`,
  `approveBuilderFee`, …) is a user-signed action — a different signature
  scheme this module cannot express — so no code path through Epigone's
  execution seam can move funds out or mint signing authority even if handed
  a master key by mistake.
- STRUCTURALLY CHECKED: signing with the traded account's own key.
  Implementations take the signer and the master account as separate values
  and MUST refuse a signer whose address equals the master's
  (HttpExecutionGateway raises MasterKeySignerError at construction). This
  check cannot recognize an arbitrary master key presented with a mismatched
  master_address — keeping master keys off Epigone entirely is the
  keystore's invariant (issue #134, ADR-0005), of which this check is the
  gateway-side defense in depth. Any non-master key is only useful once the
  master has approved it as an agent on Hyperliquid — the chain enforces
  that, we don't have to.

The Signer seam: the gateway accepts any eth_account LocalAccount-compatible
signer (what hyperliquid-python-sdk's signing helpers take). Loading,
decrypting, or generating keys is the keystore's concern (issue #134), never
the gateway's — a signer arrives ready to sign.

Nonce contract (research §2 "Nonces"): Hyperliquid tracks nonces PER SIGNER —
the 100 highest are stored; a new nonce must exceed the smallest stored one,
be unused, and sit inside (T − 2 days, T + 1 day). NonceSource implements the
docs' own advice — an atomic per-signer ms-timestamp counter — and each
HttpExecutionGateway instance owns one, so the rule is: ONE gateway instance
per signer per process (matching "one API wallet per trading process").
A nonce is single-use on-chain, which bounds the HTTP impl's 429 retry:
re-posting the same signed payload can never execute TWICE (chain-enforced).
Whether it executed ONCE before the 429 is the part the chain cannot tell us
— see AmbiguousExecutionError and ExecutionRateLimitedError.

== TESTNET FINDINGS (2026-07-27 unfunded; 2026-07-28 funded run — the open
   questions below are ANSWERED; same day, volume-farmed run (issue #142) —
   the $100k gate crossed, finding 3 CORRECTED, sub-account semantics in
   findings 6-8, api.hyperliquid-testnet.xyz) ==

Probed by scripts/testnet_probe.py, scripts/testnet_farm_volume.py and
scripts/testnet_subaccount_probe.py (throwaway keys, testnet only — the #63
empirical-contract convention). What the runs established:

- SIGNING CONTRACT VERIFIED END-TO-END: signed `order` and `scheduleCancel`
  actions submitted by HttpExecutionGateway recover SERVER-side to exactly
  the signer's address — the exchange's reject "User or API Wallet 0x…
  does not exist" names the address it recovered from the signature, so it
  doubles as a signature oracle needing no funded account. Our wire
  construction (msgpack key order, phantom-agent EIP-712, decimal wire
  strings) is byte-compatible with the chain.
- ADDRESS FIELDS INSIDE L1 ACTIONS MUST BE LOWERCASE: the server
  canonicalizes the JSON before re-hashing, so a checksummed address (e.g.
  subAccountTransfer.subAccountUser, vaultTransfer.vaultAddress, builder.b)
  makes the recovered signer garbage and the action fail as an unknown
  wallet. This is why BuilderFee's wire lowercases `b`.
- ACCOUNT EXISTENCE GATES EVERYTHING: every action from a never-deposited
  master answers "does not exist"; `approveAgent` answers "Must deposit
  before performing actions. User: 0x…". Agent approval — and therefore
  every downstream probe — requires a deposited account.

Open questions 1–3 (research §11) — ANSWERED on funded testnet 2026-07-28
(throwaway master funded with 95 mock USDC, zero volume; agent approved):

1. AGENT-COUNT CAP IS VOLUME-SCALED, NOT THE DOCUMENTED 1+3 (open Q1):
   a zero-volume account approved exactly 3 named agents, then refused with
   "Too many extra agents for cumulative volume traded. Current limit is 3".
   This explains the 26–103 agents seen on live mainnet whales — slots are
   earned by trading volume. Consequence for multi-user (Phase B): the quota
   that matters is the USER's, since each user's master approves our agent on
   their own account; Epigone's own account quota only bounds ITS agents.
   Also observed: re-approving an EXISTING agent NAME with a new address
   atomically swaps that slot's address — the clean rotation primitive (A2's
   runbook), and a removal leaves the account briefly in "User has pending
   agent removal" during which further approvals are refused.
2. scheduleCancel HAS A $1M CUMULATIVE-VOLUME GATE (open Q2 — this DECIDES
   A3's design): a funded zero-volume account is refused with "Cannot set
   scheduled cancel time until enough volume traded. Required: $1000000.
   Traded: $0." Both the set (+70s) and clear (None) forms are gated.
   THEREFORE A3 MUST SHIP THE WATCHDOG FALLBACK as the primary dead-man's
   switch — an operator account will not qualify at first, and the protocol
   primitive only becomes available after $1M of traded volume.
3. GRAY-ZONE L1 ACTIONS ARE AGENT-REACHABLE — the zero-volume negative was
   WRONG (open Q3, CORRECTED by the #142 run the same day). The earlier
   refusals ("User or API Wallet 0x… does not exist") were an artifact:
   that run's agent key was never actually approved — the Q1 cap probe had
   filled all 3 slots with SDK-generated-and-discarded keys, so the
   keys-file agent signing Q3 held no approval. Re-probed at $101k cumVlm
   with the agent GENUINELY approved (verified via extraAgents):
   - createSubAccount signed by the AGENT succeeds ({"status": "ok",
     "data": "0x38107077…"} — the agent minted a real sub-account);
   - subAccountTransfer signed by the AGENT succeeds AND really moves
     funds (the sub's accountValue readback confirms the deposit landed);
   - vaultTransfer signed by the agent now gets the ACTION-level error
     ("Vault not registered: 0x…"), identical to the master control — the
     signer is accepted; only vault registration is missing.
   THEREFORE an approved agent key drives the ENTIRE L1 surface, including
   intra-account fund plumbing: the L1-vs-user-signed signature-scheme
   split is the ONLY privilege boundary — there is no agent-vs-master
   distinction inside L1. What stays agent-unreachable is exactly the
   user-signed set (approveAgent, withdraw3, usdSend, …), so ADR-0005's
   no-external-exit claim stands; its implicit "agents can't touch
   sub/vault plumbing" reading falls (see finding 6 for what that does to
   sub-account isolation).
4. 429 semantics (raised by PR #140 review): still UNVERIFIED — the funded
   run never drove the live limiter into sustained 429s. Whether a 429'd
   /exchange submission is ALWAYS rejected before processing remains assumed
   nowhere and treated as ambiguous (AmbiguousExecutionError) until observed.
5. ADDRESS-BASED REQUEST BUDGET CONFIRMED (research §5's secondary-source
   formula, verified on a live mainnet account 2026-07-28): `userRateLimit`
   answered {"cumVlm": "58293.48", "nRequestsCap": 68293} — exactly
   10_000 + floor(cumVlm), confirming the documented "1 request per 1 USDC
   traded, plus a 10k buffer" allowance. The executor can therefore compute a
   per-account request budget from cumVlm without guessing; a fresh copy
   account starts with only the 10k buffer, which bounds how chatty a
   low-volume account's executor lane may be.
6. SUB-ACCOUNT SEMANTICS (issue #142's four questions, probed at $101k
   cumVlm by scripts/testnet_subaccount_probe.py after farming through the
   gate with scripts/testnet_farm_volume.py):
   - createSubAccount past the $100k gate: SUCCEEDS — {"status": "ok",
     "response": {"type": "createSubAccount", "data": "0xb583637e…"}}.
   - AGENTS ARE MASTER-SCOPED ONLY: approveAgent submitted with
     vaultAddress=<sub> is refused with "Vault may not perform this
     action." and extraAgents(<sub>) stays [] while the master's list is
     unchanged. userRole(<sub>) answers {"role": "subAccount", "data":
     {"master": "0x…"}} — a sub-account has no key of its own, so no
     user-signed action (agent approval included) can ever originate from
     it. There is no such thing as "an agent on the sub-account".
   - AN APPROVED AGENT TRADES THE SUB: an order carrying
     vaultAddress=<sub> signed by the master-approved agent is accepted
     and fills ({"filled": {"totalSz": "0.0003", "avgPx": "63486.0"}}),
     and so does the reduce-only close — the SDK's vault mechanism covers
     sub-accounts, and ONE master-level agent grant covers the master and
     every sub.
   - subAccountTransfer MOVES FUNDS BOTH WAYS and is NOT master-only: usd
     is micro-USD (usd=50_000_000 arrived as accountValue "50.0"), the
     withdraw leg drained the sub back to "0.0", and the APPROVED AGENT's
     deposit was accepted and really landed (finding 3).
7. AGENT-CAP GROWTH CONFIRMED: at $101k cumVlm a 4th named agent approved
   cleanly where the zero-volume cap was exactly 3 (finding 1) — slots
   scale with traded volume, curve unprobed.
8. scheduleCancel GATE COUNTER IS LIVE: at $101,605.98 both the set and
   clear forms still refuse, now naming the counter — "Cannot set
   scheduled cancel time until enough volume traded. Required: $1000000.
   Traded: $101605.98."
9. scheduleCancel WORKS PAST THE GATE, THROUGH THE PRODUCTION PATH: after
   farming the throwaway master to $1,001,002 cumVlm, set(+70s) and
   clear(None) were both ACCEPTED when submitted by HttpExecutionGateway
   with the AGENT signer — exactly the A3 dead-man's-switch path — with a
   master-signed SDK control matching ({"status": "ok"}). A3's protocol
   primitive is now testnet-proven end-to-end; finding 2's conclusion
   stands unchanged for fresh accounts (the watchdog fallback ships first,
   the primitive unlocks per account at $1M traded).
   Farming economics, measured (scripts/testnet_farm_volume.py): $1M of
   taker volume on testnet BTC cost $643.79 of mock equity, a steady
   6.3-6.5 bp per $ of cumVlm — 4.5 bp of that is the taker fee (cumVlm
   counts both legs, each leg pays 4.5 bp), the remaining ~2 bp is
   spread+impact on a book only ~$1.5-5k deep in the top 3 levels. Also
   observed
   mid-run: a testnet network upgrade put the exchange in a post-only
   phase ("Only post-only orders allowed immediately after network
   upgrade") during which every IOC is refused — transient, lifted within
   minutes, worth knowing before reading executor errors as bugs.

10. SUB-ACCOUNT COUNT IS CAPPED AT 10 PER MASTER, FLAT (issue #136, ADR-0007
   decision 1's "probe before implementation", scripts/testnet_subaccount_
   cap_probe.py, 2026-08-04): creating sub-accounts one at a time on the
   $1,001,001 cumVlm throwaway master succeeded through the 10th and the
   11th was refused with `{"status": "err", "response": "Too many
   sub-accounts."}`. Unlike the AGENT cap (finding 1/7, which scales with
   traded volume), this one did not move at 10x the highest volume gate we
   know of, so treat it as a flat ceiling until something contradicts it.
   CONSEQUENCE FOR A4: at most 10 concurrent Copy Sub-accounts, i.e. at most
   10 concurrent Leaders under the one-sub-per-Leader capital model — fewer
   in practice, since every non-copy sub the master already holds spends one
   slot, and a sub cannot be deleted (the refusal is permanent-ish, so a
   retired Leader's sub is reused, not replaced). That is far above phase
   A's single-operator handful, so it constrains nothing today; it is the
   number that decides whether multi-Leader subs (deferred in decision 1)
   ever need to come back.

11. A SUB-ACCOUNT CAN BE RENAMED, AND THE ACTION IS `subAccountModify` (issue
   #178, scripts/testnet_subaccount_rename_probe.py, 2026-08-05): submitting
   `{"type": "subAccountModify", "subAccountUser": <sub>, "name": <new>}`
   signed as the master answered `{"status": "ok", "response": {"type":
   "default"}}` and the `subAccounts` listing read the new name back
   immediately. The SDK has no method for it, so the wire dict is built here
   like every other action. This is what lets ADOPTION (the cap-refusal
   fallback, ADR-0007 amendment D-3) name a sub it did not mint after its
   Leader: the NAME is the only part of a sub-account that is not permanent
   — the sub itself still cannot be deleted, and renaming frees no slot.

A4 IMPLICATION (the question #142 existed to answer): sub-accounts are NOT
a key-compromise boundary. A compromised master-approved agent key reaches
the master AND every sub — it trades subs via vaultAddress, shuffles funds
via subAccountTransfer, and can mint new subs (findings 3 and 6) — so
ring-fencing copy capital in a sub-account contains nothing once the key
leaks. The only key-compromise boundary Hyperliquid offers is the master
account itself: a DEDICATED WALLET whose master approves a copy-only agent
key. Sub-accounts DO still bound Epigone SOFTWARE faults: margin is
per-(sub)account and Epigone's execution surface is orders-only by
construction (no subAccountTransfer exists in this seam), so a mis-sized
copy order pointed at a sub can lose at most the sub's equity — useful
margin/PnL segregation inside one trust domain, nothing more. Phase B
corollary: a user approving our agent on their main account implicitly
grants trade+shuffle reach over ALL their sub-accounts; onboarding should
steer copy capital into a dedicated wallet, never a sub of the user's main
account.
"""

import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Protocol

from eth_account.datastructures import SignedMessage
from eth_account.messages import SignableMessage

from epigone.clock import Clock


class Signer(Protocol):
    """An eth_account LocalAccount-compatible signer — exactly the shape
    hyperliquid-python-sdk's sign_l1_action consumes. The keystore (issue
    #134) produces these; the gateway only ever calls sign_message and reads
    the address."""

    @property
    def address(self) -> str: ...

    def sign_message(self, signable_message: SignableMessage) -> SignedMessage: ...


class ExecutionError(Exception):
    """An exchange write failed BEFORE anything reached the exchange (a
    connection that never established, a refused construction). Nothing
    executed. Failures where that guarantee cannot be made are the subclass
    below — the split is the load-bearing part of this hierarchy."""


class AmbiguousExecutionError(ExecutionError):
    """The action MAY have executed and the gateway cannot know (PR #140
    review). Raised whenever failure happened AFTER the request may have
    reached the exchange: a timeout, a post-send transport error, a 200
    whose body we cannot parse (the exchange processed SOMETHING), and an
    invalid-nonce reject following a 429 retry — if a 429'd attempt can
    ever have been processed (UNVERIFIED, see ExecutionRateLimitedError),
    the same-nonce retry answers "Invalid nonce" for an order that is LIVE.
    Callers MUST reconcile through the read gateway (open orders / fills)
    before re-issuing anything that is not idempotent — treating this as a
    clean failure is the silent-live-order hazard in money code."""


class ExecutionRateLimitedError(AmbiguousExecutionError):
    """The exchange kept answering 429 after backoff-and-retry (the issue #28
    convention). Pacing, not an outage. THE UNVERIFIED 429 ASSUMPTION (the
    one citation point — everything else refers here): whether a 429'd
    /exchange submission is ALWAYS rejected before processing has never been
    observed against the live limiter (funded-testnet probe #4, module
    docstring); until it is, a 429'd attempt counts as possibly-processed,
    which is why this subclasses AmbiguousExecutionError — reconcile before
    re-issuing. Downgrade to a plain ExecutionError sibling only if the
    probe proves 429s are pre-processing rejects."""


class MasterKeySignerError(ValueError):
    """The signer's address equals the master account it would trade — a
    master key on the execution path, which ADR-0005 forbids Epigone's
    servers to ever hold. Raised at construction so the path cannot exist."""


class MainnetNotEnabledError(ValueError):
    """The gateway was pointed at the MAINNET exchange without the explicit
    A5 capability. Phase A1–A4 is testnet-only BY CONSTRUCTION: nothing in
    the codebase passes allow_mainnet=True until A5 (risk policy v0 — caps,
    allowlist, kill switch) wires it around the executor, so a mainnet order
    is unreachable before the safety layer exists. Raised at construction."""


class RejectReason(Enum):
    """Why the exchange refused an action or one order within it (research §2
    error semantics). One taxonomy serves both levels because pre-validation
    failures surface batch-wide with the same vocabulary the per-order
    statuses use."""

    INVALID_NONCE = "invalid_nonce"
    # Signer not an approved agent / approval expired / account nonexistent.
    UNAUTHORIZED_SIGNER = "unauthorized_signer"
    INSUFFICIENT_MARGIN = "insufficient_margin"
    POST_ONLY_CROSS = "post_only_cross"  # Alo order would have matched
    MIN_NOTIONAL = "min_notional"  # below the $10 order minimum
    TICK_PRICE = "tick_price"  # price not divisible by tick size
    PRICE_BAND = "price_band"  # price too far from reference price
    REDUCE_ONLY_VIOLATION = "reduce_only_violation"
    BAD_TRIGGER_PRICE = "bad_trigger_price"
    NO_IMMEDIATE_MATCH = "no_immediate_match"  # Ioc/market found no liquidity
    OPEN_INTEREST_CAP = "open_interest_cap"
    MISSING_ORDER = "missing_order"  # cancel/modify of an unknown oid
    # A cumulative-traded-volume eligibility gate (funded probe 2026-07-28):
    # scheduleCancel ("Cannot set scheduled cancel time until enough volume
    # traded. Required: $1000000. Traded: $0.") and createSubAccount ($100k)
    # both refuse with this shape. The dead-man's-switch eligibility probe
    # (issue #135) keys on this reason — the refusal string is unambiguous.
    VOLUME_GATED = "volume_gated"
    # The master already holds its ten sub-accounts (finding 10): "Too many
    # sub-accounts." A class of its own because it is the ONE refusal
    # provisioning can recover from without the operator — issue #178 adopts
    # an orphaned sub instead of minting one — and telling it apart from
    # every other refusal is what makes that recovery safe to attempt.
    SUB_ACCOUNT_CAP = "sub_account_cap"
    # A throttle the exchange voices as reject PROSE (e.g. the address-based
    # budget's trickle) rather than as HTTP 429 — the 429 path surfaces as
    # ExecutionRateLimitedError after in-place retry instead (research §5).
    RATE_LIMITED = "rate_limited"
    UNKNOWN = "unknown"


# Substring → reason, checked in order. Sources: the GitBook error-responses
# table (documented reject classes, research §2) and the strings observed live
# by scripts/testnet_probe.py (2026-07-27). Matching is case-insensitive and
# deliberately loose — the exchange's prose drifts; an unmatched string
# classifies UNKNOWN and keeps its raw message rather than guessing.
_REJECT_PATTERNS: tuple[tuple[str, RejectReason], ...] = (
    ("nonce", RejectReason.INVALID_NONCE),
    ("does not exist", RejectReason.UNAUTHORIZED_SIGNER),
    ("not allowed", RejectReason.UNAUTHORIZED_SIGNER),
    ("unauthorized", RejectReason.UNAUTHORIZED_SIGNER),
    # Before the "immediately match" patterns: the Ioc no-liquidity string is
    # "Order could not immediately match against any resting orders", the Alo
    # string "Post only order would have immediately matched" — the longer
    # pattern must win.
    ("could not immediately match", RejectReason.NO_IMMEDIATE_MATCH),
    ("post only", RejectReason.POST_ONLY_CROSS),
    ("immediately match", RejectReason.POST_ONLY_CROSS),
    ("margin", RejectReason.INSUFFICIENT_MARGIN),
    ("minimum value", RejectReason.MIN_NOTIONAL),
    ("divisible by tick size", RejectReason.TICK_PRICE),
    ("away from the reference price", RejectReason.PRICE_BAND),
    ("reduce only", RejectReason.REDUCE_ONLY_VIOLATION),
    ("tp/sl price", RejectReason.BAD_TRIGGER_PRICE),
    ("trigger price", RejectReason.BAD_TRIGGER_PRICE),
    ("open interest", RejectReason.OPEN_INTEREST_CAP),
    ("never placed", RejectReason.MISSING_ORDER),
    ("already canceled", RejectReason.MISSING_ORDER),
    # Observed live 2026-07-28 (funded probe, module docstring): the shared
    # tail of every cumulative-volume eligibility refusal.
    ("until enough volume traded", RejectReason.VOLUME_GATED),
    # Observed live 2026-08-04 (cap probe, finding 10): the 11th creation's
    # verbatim refusal. Narrow enough that no other cap can land here by
    # accident — the neighbouring "too many requests" arm cannot collide.
    ("too many sub-accounts", RejectReason.SUB_ACCOUNT_CAP),
    # "too many REQUESTS" specifically — a bare "too many" would misread a
    # "Too many open orders" cap (research §5's 1,000-order limit) as a
    # throttle; an order-count cap has no arm yet and classifies UNKNOWN.
    ("too many requests", RejectReason.RATE_LIMITED),
    ("rate limit", RejectReason.RATE_LIMITED),
)


def classify_reject(message: str) -> RejectReason:
    """Map an exchange reject string to the taxonomy: first matching pattern
    wins, so _REJECT_PATTERNS orders more-specific strings first."""
    lowered = message.lower()
    for pattern, reason in _REJECT_PATTERNS:
        if pattern in lowered:
            return reason
    return RejectReason.UNKNOWN


class ActionRejectedError(ExecutionError):
    """The exchange answered {"status": "err"} — the WHOLE action failed
    pre-validation (research §2: one error for the whole batch), nothing in
    it executed. `reason` is the classified taxonomy entry; `message` the
    exchange's raw prose, kept verbatim for the audit trail."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message
        self.reason = classify_reject(message)


class Tif(Enum):
    """Time-in-force for a resting limit order (research §2 order surface)."""

    GTC = "Gtc"
    IOC = "Ioc"  # aggressive; a market order is an Ioc at a slippage-bounded price
    ALO = "Alo"  # post-only: rejects instead of crossing


class TpSl(Enum):
    TAKE_PROFIT = "tp"
    STOP_LOSS = "sl"


class Grouping(Enum):
    """How the orders of one batch relate (research §2): NA for independent
    orders; NORMAL_TPSL ties TP/SL legs to a parent order in the same batch;
    POSITION_TPSL sizes the legs against the position instead."""

    NA = "na"
    NORMAL_TPSL = "normalTpsl"
    POSITION_TPSL = "positionTpsl"


_CLOID_PATTERN = re.compile(r"^0x[0-9a-f]{32}$")


def _validate_cloid(cloid: str) -> None:
    # The wire format is 16 bytes hex, 0x-prefixed, lowercase (SDK Cloid).
    if not _CLOID_PATTERN.match(cloid):
        raise ValueError(f"cloid must be 0x + 32 lowercase hex chars, got {cloid!r}")


@dataclass(frozen=True)
class Trigger:
    """The trigger leg of a TP/SL order: `trigger_price` arms it; `is_market`
    executes at market on arm (else the order's limit_price becomes the
    resting price / slippage bound)."""

    trigger_price: Decimal
    is_market: bool
    tpsl: TpSl

    def __post_init__(self) -> None:
        if self.trigger_price <= 0:
            raise ValueError(f"trigger_price must be positive, got {self.trigger_price}")


@dataclass(frozen=True)
class OrderSpec:
    """One order of a batch, in the exchange's own vocabulary: `asset` is the
    integer asset index (the universe index from the info meta — the caller
    resolves names; the gateway talks only to /exchange), prices and sizes
    are Decimals with at most 8 decimal places (the wire's precision floor).

    A MARKET order is expressed the way the protocol itself expresses it
    (research §2, SDK market_open): tif=IOC with `limit_price` set to an
    aggressive slippage-bounded price the caller computes. reduce_only closes
    only. A TP/SL leg sets `trigger`; its tif is ignored by the exchange."""

    asset: int
    is_buy: bool
    size: Decimal
    limit_price: Decimal
    tif: Tif = Tif.GTC
    reduce_only: bool = False
    trigger: Trigger | None = None
    cloid: str | None = None

    def __post_init__(self) -> None:
        if self.size <= 0:
            raise ValueError(f"size must be positive, got {self.size}")
        if self.limit_price <= 0:
            raise ValueError(f"limit_price must be positive, got {self.limit_price}")
        if self.cloid is not None:
            _validate_cloid(self.cloid)


@dataclass(frozen=True)
class ModifySpec:
    """Replace resting order `oid` with `order` (batchModify)."""

    oid: int
    order: OrderSpec


@dataclass(frozen=True)
class CancelSpec:
    asset: int
    oid: int


@dataclass(frozen=True)
class CloidCancelSpec:
    asset: int
    cloid: str

    def __post_init__(self) -> None:
        _validate_cloid(self.cloid)


@dataclass(frozen=True)
class BuilderFee:
    """The optional per-order builder attribution (research §4): `fee_tenth_bp`
    is in TENTHS of a basis point (10 → 1bp), capped by the user's approved
    maxFeeRate. Not used in Phase A (operator-only); typed now so the wire
    format is settled where the signing is."""

    address: str
    fee_tenth_bp: int

    def __post_init__(self) -> None:
        if self.fee_tenth_bp < 0:
            raise ValueError(f"fee_tenth_bp must be non-negative, got {self.fee_tenth_bp}")


@dataclass(frozen=True)
class OrderResting:
    """The order rests on the book."""

    oid: int
    cloid: str | None = None


@dataclass(frozen=True)
class OrderFilled:
    """The order (fully) executed on placement."""

    oid: int
    total_size: Decimal
    avg_price: Decimal
    cloid: str | None = None


@dataclass(frozen=True)
class OrderRejected:
    """The exchange refused THIS order while the batch as a whole was accepted
    — data, not an exception, so one bad leg doesn't mask its siblings'
    resting oids."""

    reason: RejectReason
    message: str


OrderResult = OrderResting | OrderFilled | OrderRejected


@dataclass(frozen=True)
class CancelOk:
    pass


@dataclass(frozen=True)
class CancelRejected:
    reason: RejectReason
    message: str


CancelResult = CancelOk | CancelRejected


def decimal_to_wire(value: Decimal) -> str:
    """A Decimal as the exchange's wire string: normalized, no exponent, no
    trailing zeros — the same normal form the SDK's float_to_wire produces,
    minus the float detour (Decimals are exact, so nothing can silently
    round). At most 8 decimal places, the wire's documented precision."""
    if not value.is_finite():
        raise ValueError(f"cannot wire-encode {value}")
    normalized = value.normalize()
    exponent = normalized.as_tuple().exponent
    assert isinstance(exponent, int)  # is_finite() above rules out NaN/Inf markers
    if exponent < -8:
        raise ValueError(f"{value} has more than 8 decimal places")
    if normalized == 0:
        return "0"
    return f"{normalized:f}"


def timestamp_ms(at: datetime) -> int:
    return int(at.timestamp() * 1000)


class NonceSource:
    """Per-signer nonces: an atomic ms-timestamp counter (the docs' own
    concurrency advice, research §2). Monotonic even when calls land in the
    same millisecond or the clock steps backwards; asyncio's single thread
    makes next() atomic (no await inside).

    IN-MEMORY, single-process by contract (one gateway instance per signer
    per process — module docstring): a >1-action/ms burst runs the counter
    ahead of the wall clock, so a crash-restart inside that window can
    re-issue an already-used nonce. The exchange rejects the reuse
    ("Invalid nonce" → RejectReason.INVALID_NONCE) and the counter jumps
    past the clock on the next call, so the failure is a typed, transient,
    self-correcting reject — never a double-execution (nonces are
    single-use on-chain). Persisting the last-issued nonce is B4's
    multi-account concern; not worth a Postgres seam for one operator lane."""

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._last = 0

    def next(self) -> int:
        now_ms = timestamp_ms(self._clock.now())
        self._last = max(self._last + 1, now_ms)
        return self._last


def exchange_action_weight(batch_len: int) -> int:
    """An exchange action's IP-budget weight: 1 + floor(len/40) (research §5).
    The ADDRESS-based budget (1 request per 1 USDC traded, n per batch) is a
    separate ledger the exchange keeps per master account — not modeled here;
    its 429s surface as ExecutionRateLimitedError like any other."""
    return 1 + batch_len // 40


class ExecutionGateway(Protocol):
    """Sign-and-submit seam for Hyperliquid L1 trading actions (ADR-0005).

    Contract notes shared by all methods:
    - Actions submit in call order, one fresh per-signer nonce each
      (NonceSource); callers serialize calls per signer — one gateway
      instance, one signer, one process lane.
    - A whole-action pre-validation failure raises ActionRejectedError
      (nothing executed); per-item verdicts come back as typed results.
    - Failures split on one question: could anything have reached the
      exchange? No → ExecutionError (nothing executed). Maybe →
      AmbiguousExecutionError (reconcile via the read gateway before
      re-issuing; its docstring enumerates the cases). Sustained 429 →
      ExecutionRateLimitedError, an AmbiguousExecutionError subclass while
      the 429 assumption stays unverified (see its docstring, the one
      citation point).

    Two deliberate deltas from issue #133's action list, recorded here per
    the honest-deviation convention:
    - "cancel-all": Hyperliquid has no cancel-all L1 action. Immediate
      cancel-all is a composition — enumerate open orders (read gateway,
      which owns the coin-name↔asset mapping) + cancel_orders — and belongs
      to A3's /kill, not this seam; the dead-man's variant is
      schedule_cancel (research §2). scheduleCancel cancels ORDERS only —
      position unwind on executor death is likewise A3's policy.
    - "address-based budget accounting": the address ledger (1 request per
      1 USDC traded + 10k buffer, research §5) is the exchange's own
      per-master account; modeling it client-side needs traded-volume data
      this seam doesn't see. A1 ships the IP-side execution lane
      (SharedWeightBudget, exchange_action_weight); address-limit throttles
      surface as ExecutionRateLimitedError / RejectReason.RATE_LIMITED, and
      a client-side ledger is B4's multi-account concern (open Q7).
    """

    async def place_orders(
        self,
        orders: list[OrderSpec],
        *,
        grouping: Grouping = Grouping.NA,
        builder: BuilderFee | None = None,
        vault_address: str | None = None,
    ) -> list[OrderResult]:
        """Submit a batch of orders (limit, market-as-aggressive-Ioc, reduce-
        only, TP/SL trigger legs — research §2 order surface). Returns one
        OrderResult per order, in order. TP/SL legs tied to a parent use
        grouping=NORMAL_TPSL in the same batch; POSITION_TPSL sizes legs
        against the position.

        `vault_address` places ON A SUB-ACCOUNT (or vault) of the master
        instead of the master itself — the SDK's vault mechanism, verified to
        cover sub-accounts on testnet (finding 6: ONE master-level agent grant
        covers the master and every sub, so this changes WHICH BOOK the order
        lands on, never which key signs). ADR-0007's capital model gives every
        Leader its own Copy Sub-account, which is what needs it. Lowercase by
        the same rule every in-action address obeys (finding 2)."""
        ...

    async def cancel_orders(
        self, cancels: list[CancelSpec], *, vault_address: str | None = None
    ) -> list[CancelResult]:
        """Cancel resting orders by oid. One CancelResult per cancel, in
        order; an unknown oid comes back CancelRejected(MISSING_ORDER).

        `vault_address` cancels on a sub-account's book, exactly as
        place_orders places on it — and it is the sweep's half of the pair:
        A4 is the first thing that can place on a sub, so the watchdog's
        cancel-all had to learn to reach one (ADR-0007 decision 1)."""
        ...

    async def cancel_orders_by_cloid(self, cancels: list[CloidCancelSpec]) -> list[CancelResult]:
        """Cancel resting orders by client order id (cloid)."""
        ...

    async def modify_orders(self, modifies: list[ModifySpec]) -> list[OrderResult]:
        """Replace resting orders in place (batchModify): each oid's order
        becomes the new spec, answering like a fresh placement (resting /
        filled / rejected per modify)."""
        ...

    async def update_leverage(
        self,
        asset: int,
        leverage: int,
        *,
        is_cross: bool = True,
        vault_address: str | None = None,
    ) -> None:
        """Set leverage for an asset (cross or isolated). Raises
        ActionRejectedError if the exchange refuses.

        `vault_address` sets it ON A SUB-ACCOUNT's book, exactly as
        place_orders places on one — the copy executor's Base Stake sizing
        needs it per (sub, coin) before the first order of an episode, since
        leverage is a property of the ACCOUNT-asset pair and a sub is its own
        account (ADR-0007 amendment D-4). Setting it is a SIGNING action like
        any other: it rides the audit wrapper and the late halt re-check."""
        ...

    async def schedule_cancel(self, at: datetime | None) -> None:
        """The protocol-native dead-man's switch (research §2): at `at` (≥5s
        in the future) the exchange cancels ALL the account's open orders and
        burns one of the 10 daily triggers (reset 00:00 UTC). None removes
        the schedule. The executor heartbeats this forward; it must push the
        time before it arrives or eat a trigger."""
        ...


class SubAccountProvisioning(Protocol):
    """Creating and funding Copy Sub-accounts (ADR-0007 decision 12) — a
    SEPARATE protocol from ExecutionGateway, and that separation is the whole
    point.

    ExecutionGateway's contract opens by stating that moving funds is
    IMPOSSIBLE BY CONSTRUCTION because its surface contains only trading
    actions. Finding 3 corrected the reason (an approved agent key CAN drive
    `subAccountTransfer`; the privilege boundary is the user-signed signature
    scheme, not agent-versus-master), and ADR-0007 decision 12 then chose to
    automate the funding leg. Bolting those actions onto ExecutionGateway
    would quietly retire a property four other modules rely on: the executor's
    order path, the watchdog's cancel-only lane and every test that types a
    collaborator as `ExecutionGateway` would all silently gain fund-moving
    reach. Declaring them here instead keeps the old sentence true — anything
    holding an `ExecutionGateway` can still only trade — while the ONE code
    path that provisions asks for this protocol by name.

    What it does NOT weaken: ADR-0005's no-external-exit invariant.
    `subAccountTransfer` moves money between the master and its OWN subs and
    nothing else; `withdraw3` / `usdSend` are user-signed and remain
    unreachable from any key Epigone holds. Both actions ride the SAME signer
    and nonce lane as the trading actions (one instance per signer per
    process), so an implementation implements both protocols rather than
    standing up a second gateway — two nonce sources for one signer would
    collide.
    """

    async def create_sub_account(self, name: str) -> str:
        """Create a sub-account of the master and return its address,
        lowercased. Behind a $100k cumulative-volume gate (finding 6) and
        capped at 10 per master (finding 10); both refuse as
        ActionRejectedError with the exchange's own prose. The cap refusal
        classifies RejectReason.SUB_ACCOUNT_CAP, which is the signal the
        caller adopts an existing sub on (issue #178). Sub-accounts cannot be
        deleted, so a slot is spent for good — the NAME, however, is not:
        rename_sub_account takes it back."""
        ...

    async def rename_sub_account(self, sub_address: str, name: str) -> None:
        """Rename an existing sub-account of the master (finding 11's
        `subAccountModify`).

        Cosmetic BY CONTRACT: the name is what the operator reads in the
        Hyperliquid UI, and nothing in Epigone keys off it. It exists for
        adoption — a sub minted as `capprobe_003` and handed to a Leader
        should say so on the exchange — so a caller treats a failure here as
        a blemish to report, never as a reason to abandon a provisioning run
        that has already moved money. Raises ActionRejectedError if the
        exchange refuses; the address rides inside the action, so it is
        lowercased on the wire like every other one (finding 2)."""
        ...

    async def sub_account_transfer(
        self, sub_address: str, *, is_deposit: bool, usd_micro: int
    ) -> None:
        """Move USDC between the master and one of its sub-accounts:
        `is_deposit` funds the sub, False withdraws back to the master.
        `usd_micro` is MICRO-USD — 50_000_000 arrives as an accountValue of
        "50.0" (finding 6, measured, not documented) — so callers convert
        from dollars in exactly one place and never here."""
        ...
