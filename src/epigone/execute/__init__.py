"""The operator copy executor (issue #136, phase A4 of ADR-0005).

The product loop: a Leader the operator explicitly enabled opens, scales or
closes a position; Epigone mirrors it into that Leader's Copy Sub-account,
sized by a fixed Base Notional, and tells the operator what it did. ADR-0007
settles every money-losing edge this touches and is the document to read
first; this package implements it and cites decisions rather than re-arguing
them.

The shape, one module per concern:

- `subs` / `episodes`  — durable state: the Leader→sub mapping the operator
  provisions, and one position's life inside a sub.
- `policy`             — the hardcoded v0 risk policy and every constant
  ADR-0007 said to "record at implementation", in one place with its
  reasoning. A5 (#137) replaces the policy; the constants stay.
- `pricing`            — Base Notional and relative mirroring turned into an
  order the exchange will accept: coin units, IOC limit inside the slippage
  cap, venue precision.
- `notices`            — the executor's OWN messages to the operator's chat.
- `executor`           — the loop: reconcile, provision, drain the backlog.
- `main`               — the process, wired like the watchdog's.

TESTNET-ONLY, unchanged (the ticket's live gate): nothing here passes
allow_mainnet, so a mainnet order is refused at gateway construction until
A5 lands and the operator hand-runs the switch.
"""
