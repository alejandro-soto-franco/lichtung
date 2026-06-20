use crate::envelope::Envelope;
use lichtung_clock::{ActorId, VectorClock};
use lichtung_log::{CausalEvent, Op};

#[inline]
fn id_of(actor: &ActorId, seq: u64) -> String {
    format!("{}:{}", actor.as_str(), seq)
}

#[inline]
fn mid(msg_id: u64) -> Option<String> {
    Some(format!("m{}", msg_id))
}

/// `emit`: the sender's post-increment clock is already in `env.vclock`.
pub fn emit_event(actor: &ActorId, seq: u64, env: &Envelope) -> CausalEvent {
    CausalEvent {
        id: id_of(actor, seq),
        actor: actor.to_string(),
        seq,
        op: Op::Emit,
        vclock: env.vclock.to_string_map(),
        lamport: env.lamport.0,
        msg_id: mid(env.msg_id),
        src: Some(env.src.to_string()),
        dst: Some(env.dst.to_string()),
        value: None,
        payload_hash: None,
    }
}

/// `recv`: `clock` is the receiver's clock AFTER merge+increment; `lamport` after update.
pub fn recv_event(actor: &ActorId, seq: u64, clock: &VectorClock, lamport: u64, env: &Envelope) -> CausalEvent {
    CausalEvent {
        id: id_of(actor, seq),
        actor: actor.to_string(),
        seq,
        op: Op::Recv,
        vclock: clock.to_string_map(),
        lamport,
        msg_id: mid(env.msg_id),
        src: Some(env.src.to_string()),
        dst: Some(actor.to_string()),
        value: None,
        payload_hash: None,
    }
}

/// `compute`: a local event on the actor's world-line, no message.
pub fn compute_event(actor: &ActorId, seq: u64, clock: &VectorClock, lamport: u64) -> CausalEvent {
    CausalEvent {
        id: id_of(actor, seq),
        actor: actor.to_string(),
        seq,
        op: Op::Compute,
        vclock: clock.to_string_map(),
        lamport,
        msg_id: None,
        src: None,
        dst: None,
        value: None,
        payload_hash: None,
    }
}
