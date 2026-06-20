//! End-to-end: record a real run with lichtung-prod, then re-execute it with
//! lichtung-replay and prove (a) faithful replay, (b) determinism, (c) drift detection.

use lichtung_core::{Actor, Addr, Context};
use lichtung_prod::System;
use lichtung_replay::ReplaySystem;
use std::sync::{Arc, Mutex};

// ---- Shared actor definitions (identical code drives prod AND replay) ----
struct Source {
    a: Addr<u32>,
    b: Addr<u32>,
}
impl Actor for Source {
    type Msg = u32;
    fn handle(&mut self, n: u32, ctx: &mut Context) {
        ctx.send(&self.a, n);
        ctx.send(&self.b, n + 100);
    }
}
struct Worker {
    sink: Addr<u32>,
}
impl Actor for Worker {
    type Msg = u32;
    fn handle(&mut self, n: u32, ctx: &mut Context) {
        ctx.compute();
        ctx.send(&self.sink, n * 2);
    }
}
struct Sink;
impl Actor for Sink {
    type Msg = u32;
    fn handle(&mut self, _n: u32, ctx: &mut Context) {
        ctx.compute();
    }
}

// A worker whose handler has a STRUCTURAL drift: it sends an EXTRA message
// (two sends instead of one) that was never in the recording.
//
// NOTE: A pure value-change (e.g., n*3 instead of n*2) would NOT be detected,
// because the causal event schema only records msg_id/src/dst/vclock — NOT the
// message payload value. The value n*3 vs n*2 produces an identical causal
// footprint (same msg_id by send-index, same dst, same vclock), so event
// equality passes and no divergence is flagged. This is an honest limitation of
// causal-structure fidelity: it catches topology/ordering changes but is blind
// to payload mutations. The STRUCTURAL drift here — an extra ctx.send — causes
// `next_msg_id` to be called a second time for a send the recording never had,
// producing a "more sends than recorded" divergence that IS detected.
struct DriftWorker {
    sink: Addr<u32>,
}
impl Actor for DriftWorker {
    type Msg = u32;
    fn handle(&mut self, n: u32, ctx: &mut Context) {
        ctx.compute();
        ctx.send(&self.sink, n * 2); // same as Worker
        ctx.send(&self.sink, n); // EXTRA send: changes causal structure, never in recording
    }
}

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

async fn record_fanout() -> Vec<lichtung_log::CausalEvent> {
    let buf = Buf(Arc::new(Mutex::new(Vec::new())));
    let mut sys = System::new(buf.clone());
    let sink = sys.spawn("sink", Sink);
    let a = sys.spawn("worker-a", Worker { sink: sink.clone() });
    let b = sys.spawn("worker-b", Worker { sink: sink.clone() });
    let source = sys.spawn("source", Source { a, b });
    sys.send_external(&source, 1u32);
    sys.run_until_quiescent().await.unwrap();
    let bytes = buf.0.lock().unwrap().clone();
    lichtung_log::read_events(bytes.as_slice()).unwrap()
}

fn rebuild_faithful() -> (ReplaySystem, Addr<u32>) {
    let mut sys = ReplaySystem::new();
    let sink = sys.spawn("sink", Sink);
    let a = sys.spawn("worker-a", Worker { sink: sink.clone() });
    let b = sys.spawn("worker-b", Worker { sink: sink.clone() });
    let source = sys.spawn("source", Source { a, b });
    (sys, source)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn faithful_replay_has_no_divergences() {
    let log = record_fanout().await;
    let (mut sys, source) = rebuild_faithful();
    sys.origin(&source, 1u32);
    let outcome = sys.replay(&log).unwrap();
    assert!(
        outcome.is_faithful(),
        "faithful rebuild must replay cleanly, got: {:?}",
        outcome.divergences
    );
    assert_eq!(outcome.replayed.timeline.len(), log.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_is_deterministic() {
    let log = record_fanout().await;
    let mut a = Vec::new();
    let mut b = Vec::new();
    {
        let (mut sys, source) = rebuild_faithful();
        sys.origin(&source, 1u32);
        sys.replay(&log).unwrap().replayed.write_timeline(&mut a).unwrap();
    }
    {
        let (mut sys, source) = rebuild_faithful();
        sys.origin(&source, 1u32);
        sys.replay(&log).unwrap().replayed.write_timeline(&mut b).unwrap();
    }
    assert_eq!(a, b, "replay timeline must be byte-identical across runs");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drifted_handler_is_detected() {
    let log = record_fanout().await;
    // Rebuild with worker-a REPLACED by a structurally-divergent handler.
    // DriftWorker sends one EXTRA message the recording never had — this changes
    // the causal structure (extra emit event), which IS detected as a divergence.
    let mut sys = ReplaySystem::new();
    let sink = sys.spawn("sink", Sink);
    let a = sys.spawn("worker-a", DriftWorker { sink: sink.clone() });
    let b = sys.spawn("worker-b", Worker { sink: sink.clone() });
    let source = sys.spawn("source", Source { a, b });
    sys.origin(&source, 1u32);
    let outcome = sys.replay(&log).unwrap();
    assert!(
        !outcome.is_faithful(),
        "a structurally-drifted handler must produce at least one divergence"
    );
}
