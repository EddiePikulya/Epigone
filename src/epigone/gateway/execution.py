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

== TESTNET FINDINGS (recorded 2026-07-27, api.hyperliquid-testnet.xyz) ==

Probed by scripts/testnet_probe.py (throwaway keys, testnet only — the #63
empirical-contract convention). What the run established:

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

Open questions 1–3 (research §11) themselves are BLOCKED on that funding
wall: the testnet faucet is an on-chain claimDrip() on Arbitrum Sepolia
(gas + mainnet-history wallet), a one-time manual operator step. The
harness detects funding and completes all three probes on re-run:

1. Agent-count cap (open Q1): pending funded re-run of the harness.
2. scheduleCancel volume gate (open Q2 — feeds A3's contingency): pending
   funded re-run; the zero-volume submission path is already wired.
3. Gray-zone L1 actions from an agent signer (open Q3): pending funded
   re-run; master-vs-agent control methodology is already wired.
4. 429 semantics (raised by PR #140 review): whether a 429'd /exchange
   submission is ALWAYS rejected before processing — assumed nowhere,
   treated as ambiguous (AmbiguousExecutionError) until a funded run can
   drive the live limiter and observe.
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
    ) -> list[OrderResult]:
        """Submit a batch of orders (limit, market-as-aggressive-Ioc, reduce-
        only, TP/SL trigger legs — research §2 order surface). Returns one
        OrderResult per order, in order. TP/SL legs tied to a parent use
        grouping=NORMAL_TPSL in the same batch; POSITION_TPSL sizes legs
        against the position."""
        ...

    async def cancel_orders(self, cancels: list[CancelSpec]) -> list[CancelResult]:
        """Cancel resting orders by oid. One CancelResult per cancel, in
        order; an unknown oid comes back CancelRejected(MISSING_ORDER)."""
        ...

    async def cancel_orders_by_cloid(self, cancels: list[CloidCancelSpec]) -> list[CancelResult]:
        """Cancel resting orders by client order id (cloid)."""
        ...

    async def modify_orders(self, modifies: list[ModifySpec]) -> list[OrderResult]:
        """Replace resting orders in place (batchModify): each oid's order
        becomes the new spec, answering like a fresh placement (resting /
        filled / rejected per modify)."""
        ...

    async def update_leverage(self, asset: int, leverage: int, *, is_cross: bool = True) -> None:
        """Set leverage for an asset (cross or isolated). Raises
        ActionRejectedError if the exchange refuses."""
        ...

    async def schedule_cancel(self, at: datetime | None) -> None:
        """The protocol-native dead-man's switch (research §2): at `at` (≥5s
        in the future) the exchange cancels ALL the account's open orders and
        burns one of the 10 daily triggers (reset 00:00 UTC). None removes
        the schedule. The executor heartbeats this forward; it must push the
        time before it arrives or eat a trigger."""
        ...
