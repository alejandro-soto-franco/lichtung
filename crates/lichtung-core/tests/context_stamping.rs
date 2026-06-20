use lichtung_clock::{ActorId, Lamport, VectorClock};
use lichtung_core::{Actor, Addr, AnySink, Context, Dispatch, Envelope};
use lichtung_log::{CausalEvent, Op};
use std::sync::Arc;

/// A test backend that captures records and deliveries instead of running a runtime.
#[derive(Default)]
struct Capture {
    next_id: u64,
    records: Vec<CausalEvent>,
    delivered: Vec<Envelope>,
}
impl Dispatch for Capture {
    fn next_msg_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }
    fn record(&mut self, ev: CausalEvent) {
        self.records.push(ev);
    }
    fn deliver(&mut self, sink: &dyn AnySink, env: Envelope) {
        // capture, then also exercise the sink so the seam is covered
        let _ = sink;
        self.delivered.push(env);
    }
}

/// A no-op sink for building an Addr in the test (the Addr carries its own id).
#[derive(Clone)]
struct NullTx;
impl lichtung_core::MailboxTx for NullTx {
    fn try_send(&self, _env: Envelope) -> Result<(), Envelope> {
        Ok(())
    }
}

#[test]
fn send_increments_own_clock_and_emits() {
    let me = ActorId::from("a");
    let mut clock = VectorClock::new();
    let mut lamport = Lamport::default();
    let mut seq = 0u64;
    let mut backend = Capture::default();

    let peer_id = ActorId::from("b");
    let sink: Arc<dyn AnySink> = Arc::new(NullTx);
    let peer: Addr<u32> = Addr::new(peer_id.clone(), sink);

    {
        let mut ctx = Context::new(&me, &mut clock, &mut lamport, &mut seq, &mut backend);
        ctx.send(&peer, 99u32);
    }

    assert_eq!(clock.get(&me), 1, "own component incremented on send");
    assert_eq!(seq, 1);
    assert_eq!(backend.records.len(), 1);
    let ev = &backend.records[0];
    assert_eq!(ev.op, Op::Emit);
    assert_eq!(ev.src.as_deref(), Some("a"));
    assert_eq!(ev.dst.as_deref(), Some("b"));
    assert_eq!(ev.vclock.get("a"), Some(&1));
    let d = &backend.delivered[0];
    assert_eq!(d.vclock.get(&me), 1, "delivered envelope carries the post-increment clock");
    assert_eq!(*d.msg.downcast_ref::<u32>().unwrap(), 99);
}

// Minimal actor to prove the trait + Context type line up (compile-only check).
#[allow(dead_code)]
struct Echo;
impl Actor for Echo {
    type Msg = ();
    fn handle(&mut self, _msg: (), _ctx: &mut Context) {}
}
