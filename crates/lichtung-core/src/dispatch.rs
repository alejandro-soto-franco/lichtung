use crate::envelope::Envelope;
use crate::mailbox::AnySink;
use lichtung_log::CausalEvent;

/// The runtime backend a `Context` delegates to. Object-safe — no generic methods.
/// Prod records to the log and delivers live; M2 replay records and lets the
/// replay driver control delivery from the recorded log.
pub trait Dispatch {
    fn next_msg_id(&mut self) -> u64;
    fn record(&mut self, event: CausalEvent);
    fn deliver(&mut self, sink: &dyn AnySink, env: Envelope);
}
