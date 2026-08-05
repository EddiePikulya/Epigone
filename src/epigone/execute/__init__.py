"""The operator copy executor (issues #136 and #137, phases A4–A5 of ADR-0005).

The product loop: a Leader the operator explicitly enabled opens, scales or
closes a position; Epigone mirrors it into that Leader's Copy Sub-account,
sized by a fixed Base Stake at the Leader's own (capped) leverage, and tells
the operator what it did. ADR-0007 settles every money-losing edge this touches
and is the document to read first — including amendments D-4 and D-5, which are
what A5 changed; this package implements it and cites decisions rather than
re-arguing them.

The shape, one module per concern:

- `subs` / `episodes`  — durable state: the Leader→sub mapping the operator
  provisions, and one position's life inside a sub.
- `limits`             — the GLOBAL risk knobs, one row, re-read every cycle
  and changed with /limits.
- `policy`             — the risk policy judged before every signature, plus
  every constant ADR-0007 said to "record at implementation", in one place
  with its reasoning. The split from `limits` is by who owns the number.
- `pricing`            — Base Stake × mirrored leverage, and relative
  mirroring, turned into an order the exchange will accept: coin units, IOC
  limit inside the slippage cap, venue precision.
- `notices`            — the executor's OWN messages to the operator's chat.
- `executor`           — the loop: reconcile, provision, drain the backlog.
- `main`               — the process, wired like the watchdog's.

TESTNET BY DEFAULT, and mainnet now REACHABLE rather than absent (#137 §8):
`EXECUTOR_ALLOW_MAINNET` passes the capability `HttpExecutionGateway` has
always demanded, and the mainnet URL alone is still refused at construction.
Going live is a manual operator act in three parts — the flag, the URL, and a
funded account.
"""
