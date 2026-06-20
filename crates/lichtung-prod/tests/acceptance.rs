use lichtung_core::{Actor, Addr, Context};
use lichtung_log::Op;
use lichtung_prod::System;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

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

/// Rebuild a `BTreeMap<String,u64>` vclock comparison: returns true iff the two
/// clocks are incomparable (concurrent) under the product order.
fn concurrent(x: &BTreeMap<String, u64>, y: &BTreeMap<String, u64>) -> bool {
    let mut saw_less = false;
    let mut saw_greater = false;
    let keys: std::collections::BTreeSet<&String> = x.keys().chain(y.keys()).collect();
    for k in keys {
        let a = x.get(k).copied().unwrap_or(0);
        let b = y.get(k).copied().unwrap_or(0);
        if a < b {
            saw_less = true;
        } else if a > b {
            saw_greater = true;
        }
    }
    saw_less && saw_greater
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fanout_log_is_schema_valid_with_concurrency() {
    let buf = Buf(Arc::new(Mutex::new(Vec::new())));
    let mut sys = System::new(buf.clone());
    let sink = sys.spawn("sink", Sink);
    let a = sys.spawn("worker-a", Worker { sink: sink.clone() });
    let b = sys.spawn("worker-b", Worker { sink: sink.clone() });
    let source = sys.spawn("source", Source { a, b });
    sys.send_external(&source, 1u32);
    let _ = sys.run_until_quiescent().await.unwrap();

    let bytes = buf.0.lock().unwrap().clone();
    let events = lichtung_log::read_events(bytes.as_slice()).unwrap();

    // Non-vacuity guard: the run must have produced a log, or the per-event
    // assertions below would pass with zero iterations. The fan-out/fan-in
    // topology emits 14 events: world emit; source recv; 2 source emits;
    // per worker (×2) recv+compute+emit (6); per sink message (×2) recv+compute (4).
    assert!(
        events.len() >= 10,
        "expected a non-trivial causal log, got {} events",
        events.len()
    );

    // 1) Schema conformance: every event validates against the canonical schema.
    let schema_src = std::fs::read_to_string(
        concat!(env!("CARGO_MANIFEST_DIR"), "/../lichtung-log/tests/fixtures/event.schema.json"),
    )
    .unwrap();
    let schema: serde_json::Value = serde_json::from_str(&schema_src).unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();
    for e in &events {
        let v = serde_json::to_value(e).unwrap();
        assert!(validator.is_valid(&v), "event violates schema: {v}");
    }

    // 2) Embedding holds: every recv's vclock dominates its matching emit's.
    let emits: std::collections::HashMap<String, &lichtung_log::CausalEvent> = events
        .iter()
        .filter(|e| e.op == Op::Emit)
        .map(|e| (e.msg_id.clone().unwrap(), e))
        .collect();
    // Non-vacuity guard: there must be recv events for the embedding check to mean anything.
    let recv_count = events.iter().filter(|e| e.op == Op::Recv).count();
    assert!(recv_count > 0, "expected recv events to verify the embedding");
    for r in events.iter().filter(|e| e.op == Op::Recv) {
        let em = emits.get(r.msg_id.as_ref().unwrap()).expect("recv without emit");
        for (k, &v) in &em.vclock {
            assert!(
                r.vclock.get(k).copied().unwrap_or(0) >= v,
                "recv vclock does not dominate its emit"
            );
        }
    }

    // 3) Genuine concurrency: worker-a's compute and worker-b's compute are
    //    incomparable (spacelike) — the property M3 visualizes.
    let ca = events.iter().find(|e| e.actor == "worker-a" && e.op == Op::Compute).unwrap();
    let cb = events.iter().find(|e| e.actor == "worker-b" && e.op == Op::Compute).unwrap();
    assert!(concurrent(&ca.vclock, &cb.vclock), "worker computes should be concurrent");
}
