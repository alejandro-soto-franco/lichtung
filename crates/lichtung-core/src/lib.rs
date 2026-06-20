//! lichtung-core: the dual-mode actor boundary. Traits only, zero executor code.

mod actor;
mod addr;
mod context;
mod dispatch;
mod envelope;
mod event;
mod mailbox;

pub use actor::Actor;
pub use addr::Addr;
pub use context::Context;
pub use dispatch::Dispatch;
pub use envelope::Envelope;
pub use event::{compute_event, emit_event, recv_event};
pub use mailbox::{AnySink, Mailbox, MailboxRx, MailboxTx};

#[cfg(test)]
mod tests {
    use super::*;
    use lichtung_clock::{ActorId, Lamport, VectorClock};

    #[test]
    fn emit_event_maps_envelope_to_schema_fields() {
        let mut clock = VectorClock::new();
        clock.increment(&ActorId::from("src"));
        let env = Envelope {
            msg: Box::new(7u32),
            vclock: clock.clone(),
            lamport: Lamport(1),
            msg_id: 4,
            src: ActorId::from("src"),
            dst: ActorId::from("dst"),
        };
        let ev = emit_event(&ActorId::from("src"), 1, &env);
        assert_eq!(ev.actor, "src");
        assert_eq!(ev.seq, 1);
        assert_eq!(ev.op, lichtung_log::Op::Emit);
        assert_eq!(ev.msg_id.as_deref(), Some("m4"));
        assert_eq!(ev.src.as_deref(), Some("src"));
        assert_eq!(ev.dst.as_deref(), Some("dst"));
        assert_eq!(ev.vclock.get("src"), Some(&1));
        assert_eq!(ev.id, "src:1");
    }
}
