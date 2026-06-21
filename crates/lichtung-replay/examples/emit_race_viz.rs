//! Generate the M3 viz data — fast-vs-slow race topology.
//! Actors: world (origin), import, compute-fast, compute-slow, export.
//! Flow: world→import→{compute-fast,compute-slow}→export (fan-in).
//! Key property: export receives compute-fast's message FIRST (fast wins race).
//! Retries up to 50 times until fast-first ordering is captured.
//! Usage: cargo run -p lichtung-replay --example emit_race_viz -- <out_dir>
use lichtung_core::{Actor, Addr, Context};
use lichtung_log::Op;
use lichtung_prod::System;
use std::sync::{Arc, Mutex};

struct Importer { fast: Addr<u32>, slow: Addr<u32> }
impl Actor for Importer {
    type Msg = u32;
    fn handle(&mut self, n: u32, ctx: &mut Context) {
        ctx.send(&self.fast, n);
        ctx.send(&self.slow, n);
    }
}

struct ComputeFast { export: Addr<u32> }
impl Actor for ComputeFast {
    type Msg = u32;
    fn handle(&mut self, n: u32, ctx: &mut Context) {
        ctx.compute(); // 1 unit of work
        ctx.send(&self.export, n);
    }
}

struct ComputeSlow { export: Addr<u32> }
impl Actor for ComputeSlow {
    type Msg = u32;
    fn handle(&mut self, n: u32, ctx: &mut Context) {
        ctx.compute(); // 4 units of work — slower
        ctx.compute();
        ctx.compute();
        ctx.compute();
        ctx.send(&self.export, n);
    }
}

struct Exporter;
impl Actor for Exporter {
    type Msg = u32;
    fn handle(&mut self, _n: u32, ctx: &mut Context) {
        ctx.compute(); // one compute per recv (result processing)
    }
}

#[derive(Clone)]
struct Buf(Arc<Mutex<Vec<u8>>>);
impl std::io::Write for Buf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

/// Run one round and return the replayed log if export received fast-first.
async fn try_once() -> Option<lichtung_replay::Replayed> {
    let buf = Buf(Arc::new(Mutex::new(Vec::new())));
    let mut sys = System::new(buf.clone());
    let export      = sys.spawn("export",       Exporter);
    let fast        = sys.spawn("compute-fast", ComputeFast { export: export.clone() });
    let slow        = sys.spawn("compute-slow", ComputeSlow { export: export.clone() });
    let import      = sys.spawn("import",       Importer { fast, slow });
    sys.send_external(&import, 1u32);
    sys.run_until_quiescent().await.unwrap();

    let bytes = buf.0.lock().unwrap().clone();
    let events = lichtung_log::read_events(bytes.as_slice()).unwrap();
    let replayed = lichtung_replay::replay_log(&events).unwrap();

    // Find export's recv events in timeline order (ascending seq).
    let mut export_recvs: Vec<_> = replayed.timeline.iter()
        .filter(|e| e.actor == "export" && e.op == Op::Recv)
        .collect();
    export_recvs.sort_by_key(|e| e.seq);

    if export_recvs.len() >= 2
        && export_recvs[0].src.as_deref() == Some("compute-fast")
        && export_recvs[1].src.as_deref() == Some("compute-slow")
    {
        Some(replayed)
    } else {
        None
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let out = std::env::args().nth(1).expect("usage: emit_race_viz <out_dir>");

    let mut replayed = None;
    for attempt in 1..=50 {
        if let Some(r) = try_once().await {
            eprintln!("captured fast-first run on attempt {}", attempt);
            replayed = Some(r);
            break;
        }
        eprintln!("attempt {}: slow-first (retrying…)", attempt);
    }

    let replayed = replayed.unwrap_or_else(|| {
        eprintln!("ERROR: could not capture a fast-first run in 50 attempts");
        std::process::exit(1);
    });

    // Print full event list for verification.
    eprintln!("--- event list ---");
    for e in &replayed.timeline {
        eprintln!(
            "  seq={} actor={} op={:?} src={} dst={} lamport={}",
            e.seq,
            e.actor,
            e.op,
            e.src.as_deref().unwrap_or("-"),
            e.dst.as_deref().unwrap_or("-"),
            e.lamport,
        );
    }

    let tl = std::fs::File::create(format!("{out}/timeline.jsonl")).unwrap();
    replayed.write_timeline(tl).unwrap();
    let ps = std::fs::File::create(format!("{out}/poset.json")).unwrap();
    replayed.write_poset(ps).unwrap();
    eprintln!("wrote {}/timeline.jsonl ({} events) + poset.json", out, replayed.timeline.len());
}
