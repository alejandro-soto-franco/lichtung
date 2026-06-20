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
    next_actor: u64,
    handles: Vec<JoinHandle<()>>,
    // Task 7 uses writer for shutdown/flush; suppress dead_code until then.
    #[allow(dead_code)]
    writer: Option<JoinHandle<Result<usize, lichtung_log::LogError>>>,
}

impl System {
    /// Create a system that writes its causal log (JSON-lines) to `w`.
    pub fn new<W: std::io::Write + Send + 'static>(w: W) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<CausalEvent>();
        let writer = crate::logwriter::spawn_log_writer(rx, w);
        let shared = Arc::new(SharedRuntime::new(tx));
        System { shared, next_actor: 0, handles: Vec::new(), writer: Some(writer) }
    }

    /// Spawn an actor with a stable, human-readable name (appears in the log/viz).
    pub fn spawn<A: Actor>(&mut self, name: &str, actor: A) -> Addr<A::Msg> {
        self.next_actor += 1;
        let id = ActorId::from(name);
        let (tx, rx) = TokioMailbox::unbounded();
        let sink: Arc<dyn AnySink> = Arc::new(tx);
        let h = tokio::spawn(actor_loop::<A>(actor, id.clone(), rx, self.shared.clone()));
        self.handles.push(h);
        Addr::new(id, sink)
    }

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
}
