use crate::context::Context;

/// A unit of computation that processes one message at a time. `handle` is
/// synchronous: it may `send`/`compute` via the context but never awaits.
pub trait Actor: Send + 'static {
    type Msg: Send + 'static;

    /// Run once before the first message. Default: no-op.
    fn started(&mut self, _ctx: &mut Context) {}

    /// Handle exactly one message.
    fn handle(&mut self, msg: Self::Msg, ctx: &mut Context);
}
