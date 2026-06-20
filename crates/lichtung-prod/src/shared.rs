use lichtung_log::CausalEvent;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Notify;

/// Cross-task shared runtime state. No `Mutex` on the hot path: ids and the
/// in-flight count are atomics; events are handed off over an mpsc channel.
pub struct SharedRuntime {
    msg_id: AtomicU64,
    in_flight: AtomicUsize,
    quiescent: Notify,
    events: UnboundedSender<CausalEvent>,
}

impl SharedRuntime {
    pub fn new(events: UnboundedSender<CausalEvent>) -> Self {
        SharedRuntime {
            msg_id: AtomicU64::new(0),
            in_flight: AtomicUsize::new(0),
            quiescent: Notify::new(),
            events,
        }
    }

    #[inline]
    pub fn next_msg_id(&self) -> u64 {
        self.msg_id.fetch_add(1, Ordering::Relaxed) + 1
    }

    #[inline]
    pub fn record(&self, ev: CausalEvent) {
        // If the writer is gone the run is shutting down; dropping is fine.
        let _ = self.events.send(ev);
    }

    #[inline]
    pub fn inc_in_flight(&self) {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
    }

    /// Decrement; if this brings the count to zero, wake `run_until_quiescent`.
    #[inline]
    pub fn dec_in_flight(&self) {
        if self.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.quiescent.notify_one();
        }
    }

    #[inline]
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    pub fn quiescent(&self) -> &Notify {
        &self.quiescent
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn msg_ids_are_monotonic_from_one() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let s = SharedRuntime::new(tx);
        assert_eq!(s.next_msg_id(), 1);
        assert_eq!(s.next_msg_id(), 2);
    }

    #[test]
    fn in_flight_tracks_inc_dec() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let s = SharedRuntime::new(tx);
        s.inc_in_flight();
        s.inc_in_flight();
        assert_eq!(s.in_flight(), 2);
        s.dec_in_flight();
        assert_eq!(s.in_flight(), 1);
    }
}
