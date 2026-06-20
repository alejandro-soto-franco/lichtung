use crate::dispatch::ProdDispatch;
use crate::mailbox::{TokioMailbox, TokioRx};
use crate::shared::SharedRuntime;
use lichtung_clock::{ActorId, Lamport, VectorClock};
use lichtung_core::{recv_event, Actor, Addr, AnySink, Context, Dispatch, Mailbox, MailboxRx};
use lichtung_log::CausalEvent;
use std::sync::Arc;
use tokio::task::JoinHandle;

/// One actor's event loop: own its clock/lamport/seq, recv-stamp each message,
/// then run synchronous `handle`. The only `.await` is the mailbox recv —
/// outside `handle`, so no causality escapes the log.
async fn actor_loop<A: Actor>(
    mut actor: A,
    id: ActorId,
    mut rx: TokioRx,
    shared: Arc<SharedRuntime>,
) {
    let mut clock = VectorClock::new();
    let mut lamport = Lamport::default();
    let mut seq = 0u64;
    let mut backend = ProdDispatch { shared: shared.clone() };

    {
        let mut ctx = Context::new(&id, &mut clock, &mut lamport, &mut seq, &mut backend);
        actor.started(&mut ctx);
    }

    while let Some(env) = rx.recv().await {
        // recv-stamp: the §4 receive rule (merge, then increment own).
        clock.merge(&env.vclock);
        clock.increment(&id);
        let lam = lamport.update(env.lamport);
        seq += 1;
        backend.record(recv_event(&id, seq, &clock, lam, &env));

        let msg = match env.msg.downcast::<A::Msg>() {
            Ok(b) => *b,
            Err(_) => {
                // Type mismatch is a programming error; account and skip.
                shared.dec_in_flight();
                continue;
            }
        };
        {
            let mut ctx = Context::new(&id, &mut clock, &mut lamport, &mut seq, &mut backend);
            actor.handle(msg, &mut ctx);
        }
        // Message fully handled (its children, if any, were already counted).
        shared.dec_in_flight();
    }
}

/// The production actor system. Build it, spawn actors up front, inject an
/// origin with `send_external`, then `run_until_quiescent`.
pub struct System {
    shared: Arc<SharedRuntime>,
    handles: Vec<JoinHandle<()>>,
    writer: Option<JoinHandle<Result<usize, lichtung_log::LogError>>>,
    senders: Vec<crate::mailbox::TokioTx>,
}

impl System {
    /// Create a system that writes its causal log (JSON-lines) to `w`.
    pub fn new<W: std::io::Write + Send + 'static>(w: W) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<CausalEvent>();
        let writer = crate::logwriter::spawn_log_writer(rx, w);
        let shared = Arc::new(SharedRuntime::new(tx));
        System { shared, handles: Vec::new(), writer: Some(writer), senders: Vec::new() }
    }

    /// Spawn an actor with a stable, human-readable name (appears in the log/viz).
    pub fn spawn<A: Actor>(&mut self, name: &str, actor: A) -> Addr<A::Msg> {
        let id = ActorId::from(name);
        let (tx, rx) = TokioMailbox::unbounded();
        self.senders.push(tx.clone());
        let sink: Arc<dyn AnySink> = Arc::new(tx);
        let h = tokio::spawn(actor_loop::<A>(actor, id.clone(), rx, self.shared.clone()));
        self.handles.push(h);
        Addr::new(id, sink)
    }

    /// Inject the single causal origin: a message from the synthetic `world`
    /// world-line. Records the `world` emit so every downstream `recv` pairs
    /// with an `emit` by `msg_id`.
    pub fn send_external<M: Send + 'static>(&self, to: &Addr<M>, msg: M) {
        let world = ActorId::from("world");
        let mut clock = VectorClock::new();
        clock.increment(&world); // {world: 1}
        let msg_id = self.shared.next_msg_id();
        let env = lichtung_core::Envelope {
            msg: Box::new(msg),
            vclock: clock,
            lamport: Lamport(1),
            msg_id,
            src: world.clone(),
            dst: to.id().clone(),
        };
        self.shared.record(lichtung_core::emit_event(&world, 1, &env));
        self.shared.inc_in_flight();
        let _ = to.deliver(env);
    }

    /// Drive to quiescence (no messages in flight), then shut down cleanly:
    /// abort idle actor tasks, close the event channel, and await the writer's
    /// final flush. Returns the number of events written.
    pub async fn run_until_quiescent(mut self) -> Result<usize, lichtung_log::LogError> {
        loop {
            let notified = self.shared.quiescent().notified();
            if self.shared.in_flight() == 0 {
                break;
            }
            notified.await;
        }
        for h in &self.handles {
            h.abort();
        }
        for h in self.handles.drain(..) {
            let _ = h.await; // observe the abort so the future (and its Arc) is dropped
        }
        let writer = self.writer.take().expect("writer present");
        self.senders.clear();
        drop(self.shared);
        writer.await.expect("writer task panicked")
    }

    /// Accessor used only by in-crate tests to drive quiescence directly.
    /// `#[cfg(test)]` so it does not exist in (and warn from) non-test builds.
    #[cfg(test)]
    pub(crate) fn shared(&self) -> &Arc<SharedRuntime> {
        &self.shared
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lichtung_core::Context;

    struct Echo;
    impl Actor for Echo {
        type Msg = u32;
        fn handle(&mut self, msg: u32, ctx: &mut Context) {
            ctx.compute(); // record a local event proving handle ran
            let _ = msg;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawned_actor_receives_and_records() {
        let buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        struct W(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for W {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let mut sys = System::new(W(buf.clone()));
        let echo: Addr<u32> = sys.spawn("echo", Echo);

        // Inject one origin message directly into the actor's mailbox.
        let world = ActorId::from("world");
        let mut clock = VectorClock::new();
        clock.increment(&world);
        let msg_id = sys.shared().next_msg_id();
        sys.shared().inc_in_flight();
        let env = lichtung_core::Envelope {
            msg: Box::new(7u32),
            vclock: clock,
            lamport: Lamport(1),
            msg_id,
            src: world,
            dst: ActorId::from("echo"),
        };
        let _ = echo.deliver(env);

        // Wait for quiescence (in-flight back to 0).
        loop {
            let n = sys.shared().quiescent().notified();
            if sys.shared().in_flight() == 0 {
                break;
            }
            n.await;
        }
        // Flush path is exercised fully in Task 7; here just assert the echo ran.
        assert_eq!(sys.shared().in_flight(), 0);
    }

    struct Relay {
        next: Addr<u32>,
    }
    impl Actor for Relay {
        type Msg = u32;
        fn handle(&mut self, msg: u32, ctx: &mut Context) {
            ctx.send(&self.next, msg + 1);
        }
    }

    struct Sink;
    impl Actor for Sink {
        type Msg = u32;
        fn handle(&mut self, _msg: u32, ctx: &mut Context) {
            ctx.compute();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn relay_drains_and_writes_valid_log() {
        let buf = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        struct W(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for W {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let mut sys = System::new(W(buf.clone()));
        let sink = sys.spawn("sink", Sink);
        let relay = sys.spawn("relay", Relay { next: sink.clone() });

        sys.send_external(&relay, 1u32);
        let written = sys.run_until_quiescent().await.unwrap();

        // world emit + relay recv + relay emit + sink recv + sink compute = 5 events.
        assert_eq!(written, 5);
        let bytes = buf.lock().unwrap().clone();
        let events = lichtung_log::read_events(bytes.as_slice()).unwrap();
        assert_eq!(events.len(), 5);
        // Every recv pairs with a prior emit of the same msg_id (M2's poset edge).
        use std::collections::HashSet;
        let emits: HashSet<_> = events.iter()
            .filter(|e| e.op == lichtung_log::Op::Emit)
            .filter_map(|e| e.msg_id.clone())
            .collect();
        for e in events.iter().filter(|e| e.op == lichtung_log::Op::Recv) {
            assert!(emits.contains(e.msg_id.as_ref().unwrap()), "recv has no matching emit");
        }
    }
}
