use crate::shared::SharedRuntime;
use lichtung_core::{AnySink, Dispatch, Envelope};
use lichtung_log::CausalEvent;
use std::sync::Arc;

/// The production backend: record to the log, deliver live, count in-flight.
pub struct ProdDispatch {
    pub(crate) shared: Arc<SharedRuntime>,
}

impl Dispatch for ProdDispatch {
    #[inline]
    fn next_msg_id(&mut self) -> u64 {
        self.shared.next_msg_id()
    }

    #[inline]
    fn record(&mut self, event: CausalEvent) {
        self.shared.record(event);
    }

    #[inline]
    fn deliver(&mut self, sink: &dyn AnySink, env: Envelope) {
        // Count the message as in flight BEFORE delivery so the receiver's
        // decrement (after it handles) can never observe a transient zero.
        self.shared.inc_in_flight();
        if sink.deliver(env).is_err() {
            // Receiver gone (closed mailbox): this message will never be handled,
            // so undo the in-flight increment to preserve quiescence accounting.
            self.shared.dec_in_flight();
        }
    }
}
