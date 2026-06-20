use lichtung_clock::{ActorId, Lamport, VectorClock};
use std::any::Any;
use std::fmt;

/// A message in transit, carrying its causal stamps. The payload is type-erased
/// (`Box<dyn Any + Send>`) and downcast to the receiver's `Actor::Msg`.
pub struct Envelope {
    pub msg: Box<dyn Any + Send>,
    pub vclock: VectorClock,
    pub lamport: Lamport,
    pub msg_id: u64,
    pub src: ActorId,
    pub dst: ActorId,
}

impl fmt::Debug for Envelope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Envelope")
            .field("msg_id", &self.msg_id)
            .field("src", &self.src)
            .field("dst", &self.dst)
            .field("lamport", &self.lamport)
            .finish_non_exhaustive()
    }
}
