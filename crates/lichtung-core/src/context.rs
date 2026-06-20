use crate::addr::Addr;
use crate::dispatch::Dispatch;
use crate::envelope::Envelope;
use crate::event::{compute_event, emit_event};
use lichtung_clock::{ActorId, Lamport, VectorClock};

/// The actor's window onto the runtime. One concrete type in both modes; the
/// mode-specific behavior lives behind `&mut dyn Dispatch`. The §4 vector-clock
/// embedding is implemented here, exactly once.
pub struct Context<'a> {
    id: &'a ActorId,
    clock: &'a mut VectorClock,
    lamport: &'a mut Lamport,
    seq: &'a mut u64,
    backend: &'a mut dyn Dispatch,
}

impl<'a> Context<'a> {
    pub fn new(
        id: &'a ActorId,
        clock: &'a mut VectorClock,
        lamport: &'a mut Lamport,
        seq: &'a mut u64,
        backend: &'a mut dyn Dispatch,
    ) -> Self {
        Context { id, clock, lamport, seq, backend }
    }

    #[inline]
    pub fn id(&self) -> &ActorId {
        self.id
    }

    /// Send `msg` to `to`. A local event: increment own clock + lamport + seq,
    /// snapshot the clock into the envelope, record `emit`, hand to the backend.
    pub fn send<M: Send + 'static>(&mut self, to: &Addr<M>, msg: M) {
        self.clock.increment(self.id);
        let lam = self.lamport.tick();
        *self.seq += 1;
        let msg_id = self.backend.next_msg_id();
        let env = Envelope {
            msg: Box::new(msg),
            vclock: self.clock.clone(),
            lamport: Lamport(lam),
            msg_id,
            src: self.id.clone(),
            dst: to.id().clone(),
        };
        self.backend.record(emit_event(self.id, *self.seq, &env));
        self.backend.deliver(to.sink().as_ref(), env);
    }

    /// Record a local `compute` event on this actor's world-line.
    pub fn compute(&mut self) {
        self.clock.increment(self.id);
        let lam = self.lamport.tick();
        *self.seq += 1;
        self.backend
            .record(compute_event(self.id, *self.seq, self.clock, lam));
    }
}
