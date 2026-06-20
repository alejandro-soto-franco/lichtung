use crate::envelope::Envelope;
use crate::mailbox::AnySink;
use lichtung_clock::ActorId;
use std::marker::PhantomData;
use std::sync::Arc;

/// A typed handle to an actor. `M` gates `send` at compile time; the underlying
/// sink is type-erased. Cloning is a refcount bump (`Arc` + `Arc<str>` id).
pub struct Addr<M> {
    id: ActorId,
    sink: Arc<dyn AnySink>,
    _pd: PhantomData<fn(M)>,
}

impl<M> Clone for Addr<M> {
    fn clone(&self) -> Self {
        Addr { id: self.id.clone(), sink: self.sink.clone(), _pd: PhantomData }
    }
}

impl<M: Send + 'static> Addr<M> {
    /// Construct from an erased sink. `#[doc(hidden)]`: the surface a runtime
    /// author (prod/replay) uses to mint addresses; not part of the user API.
    #[doc(hidden)]
    pub fn new(id: ActorId, sink: Arc<dyn AnySink>) -> Self {
        Addr { id, sink, _pd: PhantomData }
    }

    pub fn id(&self) -> &ActorId {
        &self.id
    }

    pub(crate) fn sink(&self) -> &Arc<dyn AnySink> {
        &self.sink
    }

    /// Deliver a pre-stamped envelope directly into the mailbox. `#[doc(hidden)]`:
    /// used by the `send_external` origin and runtime backends, not end users.
    #[doc(hidden)]
    pub fn deliver(&self, env: Envelope) -> Result<(), Envelope> {
        self.sink.deliver(env)
    }
}
