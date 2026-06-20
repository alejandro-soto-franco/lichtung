use crate::envelope::Envelope;
use std::future::Future;

/// Producer half. Cheap to clone (every `Addr` holds one, erased).
pub trait MailboxTx: Send + Sync + Clone + 'static {
    /// Enqueue without blocking. On a closed/full mailbox, returns the envelope.
    fn try_send(&self, env: Envelope) -> Result<(), Envelope>;
}

/// Consumer half, owned by exactly one actor task.
pub trait MailboxRx: Send + 'static {
    fn recv(&mut self) -> impl Future<Output = Option<Envelope>> + Send + '_;
}

/// The swap-seam: a mailbox flavor (tokio mpsc baseline, thingbuf/flume later).
pub trait Mailbox: 'static {
    type Tx: MailboxTx;
    type Rx: MailboxRx;
    fn unbounded() -> (Self::Tx, Self::Rx);
}

/// Type-erased delivery target. `Addr<M>` holds one so the producer's concrete
/// mailbox type is forgotten while the message type `M` is checked at compile time.
pub trait AnySink: Send + Sync {
    fn deliver(&self, env: Envelope) -> Result<(), Envelope>;
}

impl<T: MailboxTx> AnySink for T {
    #[inline]
    fn deliver(&self, env: Envelope) -> Result<(), Envelope> {
        self.try_send(env)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lichtung_clock::{ActorId, Lamport, VectorClock};
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct VecTx(Arc<Mutex<Vec<Envelope>>>);
    impl MailboxTx for VecTx {
        fn try_send(&self, env: Envelope) -> Result<(), Envelope> {
            self.0.lock().unwrap().push(env);
            Ok(())
        }
    }

    fn env(id: u64) -> Envelope {
        Envelope {
            msg: Box::new(id),
            vclock: VectorClock::new(),
            lamport: Lamport(0),
            msg_id: id,
            src: ActorId::from("a"),
            dst: ActorId::from("b"),
        }
    }

    #[test]
    fn erased_sink_delivers_and_downcasts() {
        let store = Arc::new(Mutex::new(Vec::new()));
        let sink: Arc<dyn AnySink> = Arc::new(VecTx(store.clone()));
        sink.deliver(env(42)).unwrap();
        let got = store.lock().unwrap().pop().unwrap();
        assert_eq!(*got.msg.downcast::<u64>().unwrap(), 42);
    }
}
