//! Throughput: drive N messages through a relay chain and measure wall-clock.
//! `cargo bench -p lichtung-prod`.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use lichtung_core::{Actor, Addr, Context};
use lichtung_prod::System;

struct Relay {
    next: Addr<u64>,
}
impl Actor for Relay {
    type Msg = u64;
    fn handle(&mut self, n: u64, ctx: &mut Context) {
        ctx.send(&self.next, n);
    }
}
struct Sink;
impl Actor for Sink {
    type Msg = u64;
    fn handle(&mut self, _n: u64, _ctx: &mut Context) {}
}

fn run_chain(stages: usize, messages: u64) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap();
    rt.block_on(async move {
        let mut sys = System::new(std::io::sink());
        let mut addr: Addr<u64> = sys.spawn("sink", Sink);
        for i in 0..stages {
            addr = sys.spawn(&format!("relay-{i}"), Relay { next: addr });
        }
        for _ in 0..messages {
            sys.send_external(&addr, 1u64);
        }
        sys.run_until_quiescent().await.unwrap();
    });
}

fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("relay_throughput");
    let messages = 10_000u64;
    g.throughput(Throughput::Elements(messages));
    g.bench_with_input(BenchmarkId::new("stages", 4), &messages, |bch, &m| {
        bch.iter(|| run_chain(4, m));
    });
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
