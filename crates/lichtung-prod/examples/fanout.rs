//! Fan-out/fan-in: one Source splits work to two independent Workers, which
//! report to one Sink. The two Workers' compute events are concurrent — exactly
//! the spacelike separation M3 will draw. Run: `cargo run -p lichtung-prod --example fanout`.

use lichtung_core::{Actor, Addr, Context};
use lichtung_prod::System;

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

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let file = std::fs::File::create("run.jsonl").expect("create run.jsonl");
    let mut sys = System::new(file);
    let sink = sys.spawn("sink", Sink);
    let a = sys.spawn("worker-a", Worker { sink: sink.clone() });
    let b = sys.spawn("worker-b", Worker { sink: sink.clone() });
    let source = sys.spawn("source", Source { a, b });
    sys.send_external(&source, 1u32);
    let n = sys.run_until_quiescent().await.expect("clean shutdown");
    eprintln!("wrote {n} events to run.jsonl");
}
