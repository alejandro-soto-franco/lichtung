#![allow(unused_imports, dead_code)] // TODO(Task 4): remove

//! Layer 2 — handler re-execution. Re-drives real `Actor::handle` over a recorded
//! log in canonical order; payloads come from re-execution, the log supplies order
//! and verification. `ReplayDispatch` is the second `Dispatch` backend (prod is the
//! first), so the same actor code runs under both.

use lichtung_clock::{ActorId, Lamport, VectorClock};
use lichtung_core::{
    compute_event, emit_event, recv_event, Actor, Addr, AnySink, Context, Dispatch, Envelope,
    MailboxTx,
};
use lichtung_log::{CausalEvent, Op};
use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::engine::{replay_log, Replayed};
use crate::error::ReplayError;

/// Parse a wire `msg_id` ("m<digits>") back to its numeric id.
fn parse_mid(s: &str) -> Option<u64> {
    s.strip_prefix('m').and_then(|d| d.parse().ok())
}

/// Build a `VectorClock` from a recorded string-keyed clock (for reconstructing
/// the origin envelope's clock).
fn vclock_from(map: &BTreeMap<String, u64>) -> VectorClock {
    let mut vc = VectorClock::new();
    for (k, v) in map {
        vc.set(ActorId::from(k.as_str()), *v);
    }
    vc
}

/// Type-erased actor: stores a concrete `A` and dispatches `handle` by downcast.
trait ErasedActor: Send {
    fn started(&mut self, ctx: &mut Context);
    fn handle_any(&mut self, msg: Box<dyn Any + Send>, ctx: &mut Context);
}
struct TypedActor<A: Actor>(A);
impl<A: Actor> ErasedActor for TypedActor<A> {
    fn started(&mut self, ctx: &mut Context) {
        self.0.started(ctx)
    }
    fn handle_any(&mut self, msg: Box<dyn Any + Send>, ctx: &mut Context) {
        match msg.downcast::<A::Msg>() {
            Ok(m) => self.0.handle(*m, ctx),
            // A downcast failure means the rebuilt topology routed the wrong message
            // type to this actor — a usage error in the replay harness, surfaced loudly.
            Err(_) => panic!("replay topology mismatch: wrong message type for actor"),
        }
    }
}

/// No-op delivery sink. In replay, delivery is driven by the recorded log via the
/// driver, not by the mailbox; `Addr`s only need a valid id, so the sink is inert.
#[derive(Clone)]
struct NoopSink;
impl MailboxTx for NoopSink {
    fn try_send(&self, _env: Envelope) -> Result<(), Envelope> {
        Ok(())
    }
}

/// An index over the recorded log: events by id, and each actor's emits by seq.
struct RecordedIndex<'a> {
    by_id: HashMap<&'a str, &'a CausalEvent>,
    emits_by_actor: HashMap<&'a str, Vec<&'a CausalEvent>>,
}
impl<'a> RecordedIndex<'a> {
    fn build(log: &'a [CausalEvent]) -> Self {
        let mut by_id = HashMap::new();
        let mut emits_by_actor: HashMap<&str, Vec<&CausalEvent>> = HashMap::new();
        for e in log {
            by_id.insert(e.id.as_str(), e);
            if e.op == Op::Emit {
                emits_by_actor.entry(e.actor.as_str()).or_default().push(e);
            }
        }
        for v in emits_by_actor.values_mut() {
            v.sort_by_key(|e| e.seq);
        }
        RecordedIndex { by_id, emits_by_actor }
    }
}

/// A single re-execution / recording mismatch.
#[derive(Debug, Clone, PartialEq)]
pub struct Divergence {
    pub event_id: String,
    pub detail: String,
}

/// The outcome of a replay run.
pub enum ReplayOutcome {
    Ok,
    Diverged(Vec<Divergence>),
}

/// The single-threaded replay harness. Rebuild the identical topology, re-supply
/// the origin(s), then `replay(log)`.
pub struct ReplaySystem {
    actors: HashMap<ActorId, Box<dyn ErasedActor>>,
    origins: Vec<(ActorId, Box<dyn Any + Send>)>,
}

impl Default for ReplaySystem {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplaySystem {
    pub fn new() -> Self {
        ReplaySystem { actors: HashMap::new(), origins: Vec::new() }
    }

    /// Register an actor under the SAME name it had in the recorded run.
    pub fn spawn<A: Actor>(&mut self, name: &str, actor: A) -> Addr<A::Msg> {
        let id = ActorId::from(name);
        self.actors.insert(id.clone(), Box::new(TypedActor(actor)));
        let sink: Arc<dyn AnySink> = Arc::new(NoopSink);
        Addr::new(id, sink)
    }

    /// Re-supply an external origin message (the stimulus that `send_external`
    /// injected in prod; not recoverable from the log).
    pub fn origin<M: Send + 'static>(&mut self, to: &Addr<M>, msg: M) {
        self.origins.push((to.id().clone(), Box::new(msg)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Noop;
    impl Actor for Noop {
        type Msg = u32;
        fn handle(&mut self, _m: u32, _ctx: &mut Context) {}
    }

    #[test]
    fn spawn_and_origin_register() {
        let mut sys = ReplaySystem::new();
        let a = sys.spawn("a", Noop);
        assert_eq!(a.id().as_str(), "a");
        sys.origin(&a, 7u32);
        assert_eq!(sys.actors.len(), 1);
        assert_eq!(sys.origins.len(), 1);
    }

    #[test]
    fn recorded_index_groups_emits_by_actor_sorted() {
        let log = vec![
            CausalEvent {
                id: "x:2".into(), actor: "x".into(), seq: 2, op: Op::Emit,
                vclock: BTreeMap::new(), lamport: 2, msg_id: Some("m9".into()),
                src: Some("x".into()), dst: Some("y".into()), value: None, payload_hash: None,
            },
            CausalEvent {
                id: "x:1".into(), actor: "x".into(), seq: 1, op: Op::Emit,
                vclock: BTreeMap::new(), lamport: 1, msg_id: Some("m4".into()),
                src: Some("x".into()), dst: Some("z".into()), value: None, payload_hash: None,
            },
        ];
        let idx = RecordedIndex::build(&log);
        let xs = &idx.emits_by_actor["x"];
        assert_eq!(xs[0].seq, 1, "emits must be sorted by seq");
        assert_eq!(xs[1].seq, 2);
        assert_eq!(parse_mid(xs[0].msg_id.as_deref().unwrap()), Some(4));
    }
}
