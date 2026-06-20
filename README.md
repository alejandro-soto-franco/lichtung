# lichtung

A Rust actor library where causal observability is a first-class runtime
property. Lock-free mailboxes stamp every message with a vector clock; the same
actor code runs under a lock-free multi-core executor (records the causal log)
or a deterministic executor (replays a recorded log through one linear extension
of its happens-before order). A Manim pipeline animates the result.

## Status

M0 (foundation): `lichtung-clock` (vector clock + Lamport, exact happens-before
embedding) and `lichtung-log` (CausalEvent + JSON-lines, schema-compatible with
the `concurrency-causality` analyzer). Runtime + replay + viz: M1–M3.

## Crates

- `lichtung-clock` — vector clocks, Lamport stamps, the causal order.
- `lichtung-log` — the causal event log format and I/O.
