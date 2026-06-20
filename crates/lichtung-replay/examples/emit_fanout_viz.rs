//! Generate the M3 viz data from a REAL prod->replay round-trip of the fanout.
//! Usage: cargo run -p lichtung-replay --example emit_fanout_viz -- <out_dir>
use lichtung_core::{Actor, Addr, Context};
use lichtung_prod::System;
use std::sync::{Arc, Mutex};

struct Source { a: Addr<u32>, b: Addr<u32> }
impl Actor for Source {
    type Msg = u32;
    fn handle(&mut self, n: u32, ctx: &mut Context) { ctx.send(&self.a, n); ctx.send(&self.b, n + 100); }
}
struct Worker { sink: Addr<u32> }
impl Actor for Worker {
    type Msg = u32;
    fn handle(&mut self, n: u32, ctx: &mut Context) { ctx.compute(); ctx.send(&self.sink, n * 2); }
}
struct Sink;
impl Actor for Sink {
    type Msg = u32;
    fn handle(&mut self, _n: u32, ctx: &mut Context) { ctx.compute(); }
}

#[derive(Clone)]
struct Buf(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for Buf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> { self.0.lock().unwrap().extend_from_slice(b); Ok(b.len()) }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let out = std::env::args().nth(1).expect("usage: emit_fanout_viz <out_dir>");
    // Record a real run.
    let buf = Buf(Arc::new(Mutex::new(Vec::new())));
    let mut sys = System::new(buf.clone());
    let sink = sys.spawn("sink", Sink);
    let a = sys.spawn("worker-a", Worker { sink: sink.clone() });
    let b = sys.spawn("worker-b", Worker { sink: sink.clone() });
    let source = sys.spawn("source", Source { a, b });
    sys.send_external(&source, 1u32);
    sys.run_until_quiescent().await.unwrap();
    let bytes = buf.0.lock().unwrap().clone();
    let events = lichtung_log::read_events(bytes.as_slice()).unwrap();

    // Replay -> canonical timeline + poset.
    let replayed = lichtung_replay::replay_log(&events).unwrap();
    let tl = std::fs::File::create(format!("{out}/timeline.jsonl")).unwrap();
    replayed.write_timeline(tl).unwrap();
    let ps = std::fs::File::create(format!("{out}/poset.json")).unwrap();
    replayed.write_poset(ps).unwrap();
    eprintln!("wrote {}/timeline.jsonl ({} events) + poset.json", out, replayed.timeline.len());
}
