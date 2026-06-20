//! lichtung-replay: deterministic replay engine. Ingest a recorded causal log,
//! rebuild the happens-before poset, validate it, and linearize it into one
//! canonical timeline for visualization. (Layer 1: no handler re-execution.)

mod engine;
mod error;
mod extension;
mod poset;

pub use engine::{replay_log, PosetJson, Replayed};
pub use error::ReplayError;
pub use poset::Poset;

#[cfg(test)]
pub(crate) mod testlog {
    //! Shared test helper: build a small in-memory causal log.
    use lichtung_log::{CausalEvent, Op};
    use std::collections::BTreeMap;

    pub fn ev(actor: &str, seq: u64, op: Op, lamport: u64, vc: &[(&str, u64)]) -> CausalEvent {
        CausalEvent {
            id: format!("{actor}:{seq}"),
            actor: actor.to_string(),
            seq,
            op,
            vclock: vc.iter().map(|(k, v)| (k.to_string(), *v)).collect::<BTreeMap<_, _>>(),
            lamport,
            msg_id: None,
            src: None,
            dst: None,
            value: None,
            payload_hash: None,
        }
    }

    pub fn with_msg(mut e: CausalEvent, msg_id: &str, src: &str, dst: &str) -> CausalEvent {
        e.msg_id = Some(msg_id.to_string());
        e.src = Some(src.to_string());
        e.dst = Some(dst.to_string());
        e
    }
}
