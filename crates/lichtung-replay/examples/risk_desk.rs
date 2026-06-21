//! End-to-end lichtung demo: a real-time options risk desk whose central bug is
//! INVISIBLE to any timestamp/value log and only legible through causal structure.
//!
//! Topology (10 world-lines):
//!
//!     world --trigger--> feed --(per tick)--> fast-{sym}  --quote--\
//!                                         \--> slow-{sym}  --quote--> book --mark--> risk
//!
//! - `world` injects the single causal origin: "market session open".
//! - `feed` imports that and synthesizes a DETERMINISTIC stream of quote ticks
//!   for three symbols (a seeded GBM walk). For every tick it fans out to that
//!   symbol's fast and slow pricer.
//! - `fast-{sym}` prices analytically (Black-Scholes closed form) — cheap, rough.
//! - `slow-{sym}` prices with a high-resolution CRR binomial tree — genuine heavy
//!   CPU work, accurate, and SLOWER. The fast and slow pricings of the SAME tick
//!   are causally concurrent (spacelike): neither sends to the other.
//! - `book` folds every incoming quote into a per-symbol EWMA (an ORDER-SENSITIVE
//!   running aggregate) and keeps a last-writer-wins "current mark".
//! - `risk` consumes each mark and flags limit breaches.
//!
//! THE BUG: because fast(t) and slow(t) are concurrent, the order they fold into
//! the EWMA — and which one wins the LWW mark — is decided by the SCHEDULER, not
//! by causality. The session aggregate is therefore quietly non-deterministic
//! over identical inputs. A normal log (wall-clock + values) shows a clean,
//! defensible total order and hides this completely. lichtung's vector clocks
//! prove the folded events were concurrent, and its replay linearizes any single
//! captured log deterministically — making the heisenbug reproducible.
//!
//! Usage: cargo run --release -p lichtung-replay --example risk_desk -- [log_path]
//!   env LICHTUNG_TICKS=<n>  ticks per symbol (default 120; the ~10s run)
//!   env LICHTUNG_TREE=<k>   binomial steps in the slow pricer (default 3200)

use lichtung_core::{Actor, Addr, Context};
use lichtung_log::{CausalEvent, Op};
use lichtung_prod::System;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

const SYMBOLS: [&str; 3] = ["btc", "eth", "sol"];

// ---------------------------------------------------------------------------
// Quant kernels (real arithmetic — the fast/slow asymmetry is genuine).
// ---------------------------------------------------------------------------

/// Abramowitz–Stegun normal CDF.
fn norm_cdf(x: f64) -> f64 {
    let k = 1.0 / (1.0 + 0.2316419 * x.abs());
    let poly = k * (0.319381530
        + k * (-0.356563782 + k * (1.781477937 + k * (-1.821255978 + k * 1.330274429))));
    let nd = (-0.5 * x * x).exp() / (2.0 * std::f64::consts::PI).sqrt();
    if x >= 0.0 { 1.0 - nd * poly } else { nd * poly }
}

/// Black–Scholes European call — the FAST pricer's closed form.
fn bs_call(spot: f64, strike: f64, t: f64, vol: f64) -> f64 {
    let r = 0.0;
    let srt = vol * t.sqrt();
    let d1 = ((spot / strike).ln() + (r + 0.5 * vol * vol) * t) / srt;
    let d2 = d1 - srt;
    spot * norm_cdf(d1) - strike * (-r * t).exp() * norm_cdf(d2)
}

/// CRR binomial European call with `steps` levels — the SLOW pricer. O(steps^2);
/// converges to Black–Scholes as steps→∞. This is real, heavy, accurate work.
fn crr_call(spot: f64, strike: f64, t: f64, vol: f64, steps: usize) -> f64 {
    let dt = t / steps as f64;
    let u = (vol * dt.sqrt()).exp();
    let d = 1.0 / u;
    let p = (1.0 - d) / (u - d); // r = 0
    let mut v = vec![0.0f64; steps + 1];
    for (i, slot) in v.iter_mut().enumerate() {
        let st = spot * u.powi(i as i32) * d.powi((steps - i) as i32);
        *slot = (st - strike).max(0.0);
    }
    for step in (0..steps).rev() {
        for i in 0..=step {
            v[i] = p * v[i + 1] + (1.0 - p) * v[i];
        }
    }
    v[0]
}

// ---------------------------------------------------------------------------
// Messages (one type per actor; all Copy → trivially Send + 'static).
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct Tick {
    sym: usize,
    tick: u32,
    spot: f64,
    strike: f64,
    t: f64,
    vol: f64,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Kind {
    Fast,
    Slow,
}

#[derive(Clone, Copy)]
struct Quote {
    sym: usize,
    tick: u32,
    kind: Kind,
    price: f64,
}

#[derive(Clone, Copy)]
struct Mark {
    sym: usize,
    ewma: f64,
}

/// What the book concluded for one symbol — read out-of-band after the run.
/// (Instrumentation only; never touches the causal log.)
#[derive(Clone, Copy, Default)]
struct BookCell {
    ewma: f64,
    folds: u64,
    last_tick: u32,
    last_kind_is_fast: bool,
}

type BookState = Arc<Mutex<Vec<BookCell>>>;

// ---------------------------------------------------------------------------
// Real market data — Binance 1-minute klines, committed as fixtures.
// ---------------------------------------------------------------------------

/// One symbol's REAL price history: per-minute close + the realized vol
/// estimated from its OWN returns. Nothing here is synthesized.
struct SymSeries {
    closes: Vec<f64>,
    vols: Vec<f64>,
    first_ms: i64,
    last_ms: i64,
}

/// Load a Binance klines fixture: rows are `[openTime, open, high, low, close, …]`.
/// Returns real closes + a rolling annualized realized vol from real log-returns.
fn load_series(path: &str) -> SymSeries {
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {path}: {e}"));
    let rows: Vec<Vec<serde_json::Value>> = serde_json::from_slice(&bytes).unwrap();
    let closes: Vec<f64> = rows.iter().map(|r| r[4].as_str().unwrap().parse().unwrap()).collect();
    let first_ms = rows.first().unwrap()[0].as_i64().unwrap();
    let last_ms = rows.last().unwrap()[0].as_i64().unwrap();

    // Rolling realized vol over the last W real log-returns, annualized (1m bars).
    const W: usize = 30;
    const ANN: f64 = 525_600.0; // minutes per year
    let rets: Vec<f64> = closes.windows(2).map(|w| (w[1] / w[0]).ln()).collect();
    let mut vols = Vec::with_capacity(closes.len());
    for i in 0..closes.len() {
        let lo = i.saturating_sub(W);
        let win = &rets[lo..i.min(rets.len())];
        let v = if win.len() >= 2 {
            let mean = win.iter().sum::<f64>() / win.len() as f64;
            let var = win.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (win.len() - 1) as f64;
            (var * ANN).sqrt()
        } else {
            0.5
        };
        vols.push(v.max(0.05)); // floor so the pricers stay well-posed
    }
    SymSeries { closes, vols, first_ms, last_ms }
}

// ---------------------------------------------------------------------------
// Actors
// ---------------------------------------------------------------------------

/// Imports the session and REPLAYS the real market tape into the pricer fan-out.
struct Feed {
    fast: Vec<Addr<Tick>>,
    slow: Vec<Addr<Tick>>,
    series: Arc<Vec<SymSeries>>,
    ticks: u32,
}
impl Actor for Feed {
    type Msg = u64; // the session id (origin payload)
    fn handle(&mut self, _session: u64, ctx: &mut Context) {
        for tick in 0..self.ticks as usize {
            for sym in 0..SYMBOLS.len() {
                let s = &self.series[sym];
                let t = Tick {
                    sym,
                    tick: tick as u32,
                    spot: s.closes[tick],         // REAL close
                    strike: s.closes[tick] * 1.02, // a fixed 2%-OTM call on the real spot
                    t: 30.0 / 365.0,
                    vol: s.vols[tick],            // realized vol from REAL returns
                };
                ctx.send(&self.fast[sym], t);
                ctx.send(&self.slow[sym], t);
            }
        }
    }
}

/// Fast analytic pricer — one compute step, closed form, rough.
struct FastPricer {
    book: Addr<Quote>,
}
impl Actor for FastPricer {
    type Msg = Tick;
    fn handle(&mut self, t: Tick, ctx: &mut Context) {
        ctx.compute(); // the single pricing step
        let price = bs_call(t.spot, t.strike, t.t, t.vol);
        ctx.send(&self.book, Quote { sym: t.sym, tick: t.tick, kind: Kind::Fast, price });
    }
}

/// Slow high-resolution pricer — heavy CRR tree, emits refinement milestones.
struct SlowPricer {
    book: Addr<Quote>,
    steps: usize,
}
impl Actor for SlowPricer {
    type Msg = Tick;
    fn handle(&mut self, t: Tick, ctx: &mut Context) {
        // Four logged refinement milestones; the real cost is in the final tree.
        let mut price = 0.0;
        for m in 1..=4 {
            let s = (self.steps * m / 4).max(2);
            price = crr_call(t.spot, t.strike, t.t, t.vol, s);
            ctx.compute(); // milestone on this world-line
        }
        ctx.send(&self.book, Quote { sym: t.sym, tick: t.tick, kind: Kind::Slow, price });
    }
}

/// Aggregator: folds every quote into a per-symbol EWMA (ORDER-SENSITIVE) and
/// keeps a last-writer-wins current mark. The defect lives here.
struct Book {
    risk: Addr<Mark>,
    state: BookState,
    alpha: f64,
}
impl Actor for Book {
    type Msg = Quote;
    fn handle(&mut self, q: Quote, ctx: &mut Context) {
        ctx.compute(); // "fold" event on the book's world-line
        let mark = {
            let mut st = self.state.lock().unwrap();
            let cell = &mut st[q.sym];
            // EWMA fold — depends on the ARRIVAL ORDER of concurrent quotes.
            cell.ewma = if cell.folds == 0 { q.price } else { self.alpha * q.price + (1.0 - self.alpha) * cell.ewma };
            cell.folds += 1;
            // LWW current mark — concurrent fast(t)/slow(t) resolved by arrival.
            if q.tick >= cell.last_tick {
                cell.last_tick = q.tick;
                cell.last_kind_is_fast = q.kind == Kind::Fast;
            }
            Mark { sym: q.sym, ewma: cell.ewma }
        };
        ctx.send(&self.risk, mark);
    }
}

/// Downstream consumer: accumulates a PORTFOLIO realized-variation over the
/// cross-symbol mark stream — `rv += (ewma - last_ewma)^2` in ARRIVAL ORDER.
/// The marks interleave across symbols by schedule, so this "deterministic"
/// risk number is in fact race-determined.
struct Risk {
    limits: Vec<f64>,
    port: Arc<Mutex<Portfolio>>,
}
#[derive(Clone, Copy, Default)]
struct Portfolio {
    rv: f64,
    last_ewma: f64,
    seen: u64,
    breaches: u64,
}
impl Actor for Risk {
    type Msg = Mark;
    fn handle(&mut self, m: Mark, ctx: &mut Context) {
        let mut p = self.port.lock().unwrap();
        if p.seen > 0 {
            p.rv += (m.ewma - p.last_ewma).powi(2);
        }
        p.last_ewma = m.ewma;
        p.seen += 1;
        if m.ewma > self.limits[m.sym] {
            p.breaches += 1;
            drop(p);
            ctx.compute(); // a logged "breach" decision — driven by the racy stream
        }
    }
}

// ---------------------------------------------------------------------------
// One full run of the desk → returns the persisted causal log bytes + book state.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Buf(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for Buf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct RunResult {
    log: Vec<u8>,
    book: Vec<BookCell>,
    port: Portfolio,
    events: usize,
}

async fn run_desk(series: Arc<Vec<SymSeries>>, ticks: u32, steps: usize, session: u64) -> RunResult {
    let buf = Buf(Arc::new(Mutex::new(Vec::new())));
    let mut sys = System::new(buf.clone());

    let state: BookState = Arc::new(Mutex::new(vec![BookCell::default(); SYMBOLS.len()]));
    let port = Arc::new(Mutex::new(Portfolio::default()));

    let risk = sys.spawn(
        "risk",
        Risk { limits: vec![1500.0, 90.0, 2.5], port: port.clone() },
    );
    let book = sys.spawn(
        "book",
        Book { risk: risk.clone(), state: state.clone(), alpha: 0.3 },
    );

    let mut fast = Vec::new();
    let mut slow = Vec::new();
    for sym in 0..SYMBOLS.len() {
        fast.push(sys.spawn(&format!("fast-{}", SYMBOLS[sym]), FastPricer { book: book.clone() }));
        slow.push(sys.spawn(&format!("slow-{}", SYMBOLS[sym]), SlowPricer { book: book.clone(), steps }));
    }

    let feed = sys.spawn("feed", Feed { fast, slow, series, ticks });
    sys.send_external(&feed, session);
    let events = sys.run_until_quiescent().await.unwrap();

    let log = buf.0.lock().unwrap().clone();
    let book = state.lock().unwrap().clone();
    let port = *port.lock().unwrap();
    RunResult { log, book, port, events }
}

// ---------------------------------------------------------------------------
// Readback: causal analysis of the PERSISTED log.
// ---------------------------------------------------------------------------

/// Vector-clock product order on the wire maps (absent key = 0).
/// Returns true iff `a` happens-before-or-equal `b`.
fn leq(a: &BTreeMap<String, u64>, b: &BTreeMap<String, u64>) -> bool {
    a.iter().all(|(k, va)| b.get(k).copied().unwrap_or(0) >= *va)
}
fn concurrent(a: &CausalEvent, b: &CausalEvent) -> bool {
    !leq(&a.vclock, &b.vclock) && !leq(&b.vclock, &a.vclock)
}

fn fmt_vc(vc: &BTreeMap<String, u64>) -> String {
    let mut parts: Vec<String> = vc.iter().map(|(k, v)| format!("{k}:{v}")).collect();
    parts.sort();
    format!("{{{}}}", parts.join(", "))
}

fn readback(log: &[u8], book: &[BookCell], port: &Portfolio) {
    let events = lichtung_log::read_events(log).unwrap();
    println!("\n========================  READBACK: {} events  ========================", events.len());
    println!(
        "  desk portfolio realized-variation (folded over the cross-symbol mark stream) = {:.6}  [{} marks, {} breaches]",
        port.rv, port.seen, port.breaches,
    );

    // The quotes that fed the book: emit events whose dst is `book`, split by pricer.
    for sym in 0..SYMBOLS.len() {
        let fast_name = format!("fast-{}", SYMBOLS[sym]);
        let slow_name = format!("slow-{}", SYMBOLS[sym]);
        let fast_quotes: Vec<&CausalEvent> = events
            .iter()
            .filter(|e| e.op == Op::Emit && e.actor == fast_name && e.dst.as_deref() == Some("book"))
            .collect();
        let slow_quotes: Vec<&CausalEvent> = events
            .iter()
            .filter(|e| e.op == Op::Emit && e.actor == slow_name && e.dst.as_deref() == Some("book"))
            .collect();

        // How many fast×slow quote pairs into the book are causally concurrent?
        let mut conc = 0usize;
        let total = fast_quotes.len() * slow_quotes.len();
        for f in &fast_quotes {
            for s in &slow_quotes {
                if concurrent(f, s) {
                    conc += 1;
                }
            }
        }

        let cell = book[sym];
        println!(
            "\n[{}]  book EWMA = {:.4}  ({} folds, last tick {} kept the {} price)",
            SYMBOLS[sym].to_uppercase(),
            cell.ewma,
            cell.folds,
            cell.last_tick,
            if cell.last_kind_is_fast { "ROUGH/fast" } else { "accurate/slow" },
        );
        println!(
            "      fast→book quotes: {:>4}   slow→book quotes: {:>4}   concurrent pairs: {}/{}  ({:.0}%)",
            fast_quotes.len(),
            slow_quotes.len(),
            conc,
            total,
            if total > 0 { 100.0 * conc as f64 / total as f64 } else { 0.0 },
        );

        // Show ONE contended pair with both clocks side by side — the proof.
        if let (Some(f), Some(s)) = (fast_quotes.last(), slow_quotes.last()) {
            if concurrent(f, s) {
                println!("      ── spacelike contention for the final marks (neither happens-before the other):");
                println!("         fast {}  vc={}", f.id, fmt_vc(&f.vclock));
                println!("         slow {}  vc={}", s.id, fmt_vc(&s.vclock));
            }
        }
    }

    // The "normal log" lie: the book's folds sorted by lamport look like a clean
    // total order — exactly the view you'd have without vector clocks.
    let mut book_recvs: Vec<&CausalEvent> = events
        .iter()
        .filter(|e| e.actor == "book" && e.op == Op::Recv)
        .collect();
    book_recvs.sort_by_key(|e| e.lamport);
    println!(
        "\n  Without causal clocks, the book's {} folds present as ONE tidy lamport-ordered\n  stream (first 8 by src): {}",
        book_recvs.len(),
        book_recvs
            .iter()
            .take(8)
            .map(|e| e.src.as_deref().unwrap_or("-"))
            .collect::<Vec<_>>()
            .join(" → ")
    );
    println!("  …which hides that adjacent fast/slow folds are concurrent: their order — and so the\n  EWMA above — is decided by the scheduler, not by causality.");
}

// ---------------------------------------------------------------------------

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let log_path = std::env::args().nth(1).unwrap_or_else(|| "/tmp/lichtung_risk_desk.jsonl".to_string());
    let steps: usize = std::env::var("LICHTUNG_TREE").ok().and_then(|v| v.parse().ok()).unwrap_or(9000);

    // ---- Load REAL market data (Binance 1-minute klines, committed fixtures). ----
    let dir = format!("{}/examples/data", env!("CARGO_MANIFEST_DIR"));
    let series: Vec<SymSeries> = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        .iter()
        .map(|p| load_series(&format!("{dir}/{p}.json")))
        .collect();
    let avail = series.iter().map(|s| s.closes.len()).min().unwrap();
    let want: u32 = std::env::var("LICHTUNG_TICKS").ok().and_then(|v| v.parse().ok()).unwrap_or(240);
    let ticks = want.min(avail as u32);
    println!("== Real market data: Binance BTC/ETH/SOL 1m closes ==");
    for (i, s) in series.iter().enumerate() {
        println!(
            "   {}  {} candles  spot {:.2}→{:.2}  realized vol {:.1}%→{:.1}%  [{}..{}]",
            SYMBOLS[i].to_uppercase(),
            s.closes.len(),
            s.closes[0],
            s.closes[ticks as usize - 1],
            s.vols[0] * 100.0,
            s.vols[ticks as usize - 1] * 100.0,
            s.first_ms,
            s.last_ms,
        );
    }
    let series = Arc::new(series);
    let session = series[0].last_ms as u64;

    // ---- Phase 1: the real ~10s run, streamed to a persisted log on disk. ----
    println!("\n== Phase 1: running the risk desk ({} real ticks/symbol, {}-step slow trees) ==", ticks, steps);
    let t0 = Instant::now();
    let run = run_desk(series.clone(), ticks, steps, session).await;
    let elapsed = t0.elapsed();
    std::fs::write(&log_path, &run.log).unwrap();
    println!(
        "   ran in {:.2}s · {} causal events · log persisted to {} ({:.1} MiB)",
        elapsed.as_secs_f64(),
        run.events,
        log_path,
        run.log.len() as f64 / (1024.0 * 1024.0),
    );

    // ---- Phase 2: read the PERSISTED log back and surface the causal truth. ----
    let disk = std::fs::read(&log_path).unwrap();
    readback(&disk, &run.book, &run.port);

    // ---- Phase 3: the heisenbug — identical real inputs, scheduler-dependent output. ----
    println!("\n========================  HEISENBUG: 6 runs, identical REAL inputs  ========================");
    println!("  Portfolio realized-variation across runs (differs ⇒ the risk number is race-determined):");
    for r in 0..6 {
        let small = run_desk(series.clone(), 60.min(ticks), 512, session).await;
        println!("    run {}:  rv = {:.6}   (over {} marks)", r + 1, small.port.rv, small.port.seen);
    }

    // ---- Phase 4: lichtung's own engine — deterministic replay of one log. ----
    println!("\n========================  REPLAY: deterministic linearization  ========================");
    let one = run_desk(series.clone(), 6.min(ticks), 64, session).await;
    let evs = lichtung_log::read_events(one.log.as_slice()).unwrap();
    let r1 = lichtung_replay::replay_log(&evs).unwrap();
    let r2 = lichtung_replay::replay_log(&evs).unwrap();
    let mut b1 = Vec::new();
    let mut b2 = Vec::new();
    r1.write_timeline(&mut b1).unwrap();
    r2.write_timeline(&mut b2).unwrap();
    println!(
        "  replayed a {}-event log twice: timelines byte-identical = {} · engine found {} spacelike pairs",
        evs.len(),
        b1 == b2,
        r1.poset_json.concurrent.len(),
    );
    println!("  ⇒ the original run is non-reproducible, but ANY captured log replays deterministically.");
}
