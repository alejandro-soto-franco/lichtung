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

## Performance

lichtung keeps the actor hot path allocation-light, in the spirit of
fastwebsockets: `ActorId` is `Arc<str>` (clones are refcount bumps), each
message costs one payload box + one vclock snapshot, per-actor clocks are owned
(no shared locks), cross-task state is atomic, and the causal log is written by a
single task that batches with a `BufWriter` and flushes only when its queue
drains. Throughput is measured, not asserted:

```
cargo bench -p lichtung-prod
```

reports messages/second through a 4-stage relay. Future work: bounded mailboxes
with `drop`-event backpressure, a thingbuf/flume `Mailbox` flavor selected by
this benchmark, and integer-indexed vector clocks for very wide systems.
