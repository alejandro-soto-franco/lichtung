use lichtung_clock::{ActorId, Lamport, VectorClock};
use std::any::Any;

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
