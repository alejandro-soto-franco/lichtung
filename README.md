# lichtung

A Rust actor library where causal observability is a first-class runtime
property. Lock-free mailboxes stamp every message with a vector clock; the same
actor code runs under a lock-free multi-core executor (records the causal log)
or a deterministic executor (replays a recorded log through one linear extension
of its happens-before order).

## Crates

- `lichtung-clock`: vector clocks and Lamport stamps with exact happens-before embedding.
- `lichtung-log`: causal event log format (JSON-lines), schema-compatible with the `concurrency-causality` analyser.
- `lichtung-core`: `Actor`, `Addr`, and `Context` traits; the object-safe `Dispatch` seam.
- `lichtung-prod`: lock-free Tokio executor with batched causal-log recording.
- `lichtung-replay`: poset build, Kahn linearisation, and deterministic re-execution with byte-exact fidelity checking.

## Usage

Add the crates you need to your `Cargo.toml`. The minimal production setup:

```toml
lichtung-clock = { git = "https://github.com/alejandro-soto-franco/lichtung" }
lichtung-log   = { git = "https://github.com/alejandro-soto-franco/lichtung" }
lichtung-core  = { git = "https://github.com/alejandro-soto-franco/lichtung" }
lichtung-prod  = { git = "https://github.com/alejandro-soto-franco/lichtung" }
```

Add `lichtung-replay` for offline replay and fidelity checking.

## Example: risk desk on real market data

`crates/lichtung-replay/examples/risk_desk.rs` runs an end-to-end showcase on
real Binance BTC/ETH/SOL 1-minute klines (committed as fixtures). Ten actors:
world feeds into fast (Black-Scholes) and slow (CRR binomial tree) pricers per
symbol, which fan into a book aggregator (EWMA + last-write-wins) and a
portfolio risk actor (realised variation).

The bug: each tick's fast and slow quotes are causally concurrent (spacelike),
so the book's fold order and the portfolio risk number are scheduler-determined.
This is invisible to any timestamp or value log and is legible only from the
vector-clock record. The readback proves it: 100% of fast-vs-slow quote pairs
into the book are incomparable by vector clock. Replay is byte-deterministic
across any number of re-runs.

```
cargo run -p lichtung-replay --example risk_desk
```

Tunable via `LICHTUNG_TICKS` and `LICHTUNG_TREE`.

## Performance

The actor hot path is allocation-light: `ActorId` is `Arc<str>` (clones are
refcount bumps), each message costs one payload box plus one vclock snapshot,
per-actor clocks are owned with no shared locks, cross-task state is atomic, and
the causal log is written by a single dedicated task that batches with a
`BufWriter` and flushes only when its queue drains.

```
cargo bench -p lichtung-prod
```

reports messages per second through a 4-stage relay.

## License

MIT OR Apache-2.0
