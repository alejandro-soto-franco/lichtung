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

/// The result of a replay: the canonical replayed artifacts plus any fidelity
/// divergences detected by re-execution. Empty `divergences` == faithful replay.
pub struct ReplayOutcome {
    pub replayed: Replayed,
    pub divergences: Vec<Divergence>,
}
impl ReplayOutcome {
    pub fn is_faithful(&self) -> bool {
        self.divergences.is_empty()
    }
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

use std::collections::HashSet;

/// Mutable state threaded through one replay run. Borrowed by the driver for
/// recv-stamping and (disjointly) by `ReplayDispatch` for handler-produced events.
struct ReplayState<'a> {
    recorded: RecordedIndex<'a>,
    pending: HashMap<u64, Envelope>,
    send_count: HashMap<ActorId, usize>,
    divergences: Vec<Divergence>,
    produced: HashSet<String>,
}

/// Verify one re-produced event against the recording. A faithful event equals its
/// recorded counterpart exactly (same op/seq/vclock/lamport/msg_id/src/dst). Records
/// the event id as produced; appends a `Divergence` on any mismatch or absence.
fn verify(
    recorded: &RecordedIndex,
    produced: &CausalEvent,
    divergences: &mut Vec<Divergence>,
    produced_set: &mut HashSet<String>,
) {
    produced_set.insert(produced.id.clone());
    match recorded.by_id.get(produced.id.as_str()) {
        Some(rec) if **rec == *produced => {}
        Some(rec) => divergences.push(Divergence {
            event_id: produced.id.clone(),
            detail: format!(
                "re-executed event diverges from recording (recorded op {:?}, vclock {:?}; replayed op {:?}, vclock {:?})",
                rec.op, rec.vclock, produced.op, produced.vclock
            ),
        }),
        None => divergences.push(Divergence {
            event_id: produced.id.clone(),
            detail: "re-executed event has no recorded counterpart".into(),
        }),
    }
}

/// The replay `Dispatch` backend: assigns each send the RECORDED msg_id (matched by
/// the sender's program-order send index), verifies handler-produced events, and
/// stashes outgoing envelopes in `pending` for the driver to deliver at their recv.
struct ReplayDispatch<'a, 'b> {
    state: &'b mut ReplayState<'a>,
    current: ActorId,
}

impl<'a, 'b> Dispatch for ReplayDispatch<'a, 'b> {
    fn next_msg_id(&mut self) -> u64 {
        let n = self.state.send_count.entry(self.current.clone()).or_insert(0);
        *n += 1;
        let j = *n; // the j-th send by `current` (1-based)
        match self
            .state
            .recorded
            .emits_by_actor
            .get(self.current.as_str())
            .and_then(|v| v.get(j - 1))
            .and_then(|e| e.msg_id.as_deref())
            .and_then(parse_mid)
        {
            Some(m) => m,
            None => {
                self.state.divergences.push(Divergence {
                    event_id: format!("{}:send{}", self.current.as_str(), j),
                    detail: "handler produced more sends than the recording".into(),
                });
                // Unmatched sentinel; the resulting pending entry simply never gets
                // consumed by a recv, and the completeness check flags it.
                u64::MAX - j as u64
            }
        }
    }

    fn record(&mut self, event: CausalEvent) {
        verify(
            &self.state.recorded,
            &event,
            &mut self.state.divergences,
            &mut self.state.produced,
        );
    }

    fn deliver(&mut self, _sink: &dyn AnySink, env: Envelope) {
        self.state.pending.insert(env.msg_id, env);
    }
}

impl ReplaySystem {
    /// Re-execute the recorded run. Runs the Phase 1 engine for the canonical order,
    /// then re-drives `handle` over the recv events in that order — payloads from
    /// re-execution, every produced event verified against the recording.
    pub fn replay(mut self, log: &[CausalEvent]) -> Result<ReplayOutcome, ReplayError> {
        // Phase 1: canonical timeline + poset (also validates the log).
        let replayed = replay_log(log)?;

        let mut state = ReplayState {
            recorded: RecordedIndex::build(log),
            pending: HashMap::new(),
            send_count: HashMap::new(),
            divergences: Vec::new(),
            produced: HashSet::new(),
        };
        // Per-actor causal state (mirrors prod's per-task locals, persisted here).
        let mut clocks: HashMap<ActorId, (VectorClock, Lamport, u64)> = self
            .actors
            .keys()
            .map(|k| (k.clone(), (VectorClock::new(), Lamport::default(), 0)))
            .collect();

        // Seed origins: match each recorded `world` Emit to a re-supplied origin (in
        // seq order). Reconstruct the origin envelope from the recorded emit + the
        // user's message, mark the world emit produced, and stash it in `pending`.
        let world = ActorId::from("world");
        let world_emits: Vec<CausalEvent> = state
            .recorded
            .emits_by_actor
            .get(world.as_str())
            .map(|v| v.iter().map(|e| (*e).clone()).collect())
            .unwrap_or_default();
        for (k, we) in world_emits.iter().enumerate() {
            let m = we.msg_id.as_deref().and_then(parse_mid).unwrap_or(0);
            let (to_id, msg) = match self.origins.get_mut(k) {
                Some((id, boxed)) => (id.clone(), std::mem::replace(boxed, Box::new(()))),
                None => {
                    state.divergences.push(Divergence {
                        event_id: we.id.clone(),
                        detail: "recorded a world origin with no re-supplied origin message".into(),
                    });
                    continue;
                }
            };
            let env = Envelope {
                msg,
                vclock: vclock_from(&we.vclock),
                lamport: Lamport(we.lamport),
                msg_id: m,
                src: world.clone(),
                dst: to_id,
            };
            state.produced.insert(we.id.clone());
            state.pending.insert(m, env);
        }

        // `started()` pass: run each actor's `started()` once, in NAME-SORTED order,
        // so any messages an actor sends from `started()` are re-produced, verified,
        // and stashed in `pending`. Counters are cumulative with the recv loop below
        // (started-sends are an actor's first sends), so do NOT reset between passes.
        let mut names: Vec<ActorId> = self.actors.keys().cloned().collect();
        names.sort();
        for name in &names {
            self.drive(name, &mut clocks, &mut state, None);
        }

        // Walk the canonical timeline; drive `handle` on each recv. Emit/compute
        // events are produced (and verified) as side effects of handlers, so they
        // are skipped by the driver loop itself.
        for ev in &replayed.timeline {
            if ev.op != Op::Recv {
                continue;
            }
            let dst = ActorId::from(ev.actor.as_str());
            let m = match ev.msg_id.as_deref().and_then(parse_mid) {
                Some(m) => m,
                None => {
                    state.divergences.push(Divergence {
                        event_id: ev.id.clone(),
                        detail: "recv has no msg_id".into(),
                    });
                    continue;
                }
            };
            let env = match state.pending.remove(&m) {
                Some(e) => e,
                None => {
                    state.divergences.push(Divergence {
                        event_id: ev.id.clone(),
                        detail: "no re-produced message for this recv (upstream divergence)".into(),
                    });
                    continue;
                }
            };

            // Recv-stamp with the SAME §4 rule prod uses.
            let entry = clocks
                .entry(dst.clone())
                .or_insert_with(|| (VectorClock::new(), Lamport::default(), 0));
            entry.0.merge(&env.vclock);
            entry.0.increment(&dst);
            let lam = entry.1.update(env.lamport);
            entry.2 += 1;
            let produced_recv = recv_event(&dst, entry.2, &entry.0, lam, &env);
            verify(&state.recorded, &produced_recv, &mut state.divergences, &mut state.produced);

            if !self.actors.contains_key(&dst) {
                state.divergences.push(Divergence {
                    event_id: ev.id.clone(),
                    detail: "recv for an actor not rebuilt in the ReplaySystem".into(),
                });
                continue;
            }
            // Run the handler. The split-borrow lives in `drive`.
            self.drive(&dst, &mut clocks, &mut state, Some(env.msg));
        }

        // Completeness: every recorded event must have been re-produced.
        for e in log {
            if !state.produced.contains(&e.id) {
                state.divergences.push(Divergence {
                    event_id: e.id.clone(),
                    detail: "recorded event was not re-produced by re-execution".into(),
                });
            }
        }

        Ok(ReplayOutcome { replayed, divergences: state.divergences })
    }

    /// Drive one actor under a `ReplayDispatch`: either its `started()` (when `msg`
    /// is `None`) or one `handle` (when `msg` is `Some`). Centralizes the split
    /// three-way borrow — `self.actors` (the handler), `clocks[dst]`
    /// (clock/lamport/seq), and `state` (the dispatch backend) — into one place
    /// where the disjointness is over named locals, so the borrow checker accepts it.
    fn drive(
        &mut self,
        dst: &ActorId,
        clocks: &mut HashMap<ActorId, (VectorClock, Lamport, u64)>,
        state: &mut ReplayState<'_>,
        msg: Option<Box<dyn Any + Send>>,
    ) {
        let actor = self.actors.get_mut(dst).expect("actor exists");
        let entry = clocks
            .entry(dst.clone())
            .or_insert_with(|| (VectorClock::new(), Lamport::default(), 0));
        let (clock, lamport, seq) = (&mut entry.0, &mut entry.1, &mut entry.2);
        let mut backend = ReplayDispatch { state, current: dst.clone() };
        let mut ctx = Context::new(dst, clock, lamport, seq, &mut backend);
        match msg {
            Some(m) => actor.handle_any(m, &mut ctx),
            None => actor.started(&mut ctx),
        }
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

    fn rec_emit(actor: &str, seq: u64, msg_id: &str, dst: &str, lamport: u64, vc: &[(&str, u64)]) -> CausalEvent {
        CausalEvent {
            id: format!("{actor}:{seq}"), actor: actor.into(), seq, op: Op::Emit,
            vclock: vc.iter().map(|(k, v)| (k.to_string(), *v)).collect(),
            lamport, msg_id: Some(msg_id.into()), src: Some(actor.into()),
            dst: Some(dst.into()), value: None, payload_hash: None,
        }
    }

    #[test]
    fn next_msg_id_returns_recorded_id_by_send_index() {
        let log = vec![
            rec_emit("a", 1, "m5", "b", 1, &[("a", 1)]),
            rec_emit("a", 2, "m8", "c", 2, &[("a", 2)]),
        ];
        let mut state = ReplayState {
            recorded: RecordedIndex::build(&log),
            pending: HashMap::new(), send_count: HashMap::new(),
            divergences: Vec::new(), produced: HashSet::new(),
        };
        let mut d = ReplayDispatch { state: &mut state, current: ActorId::from("a") };
        assert_eq!(d.next_msg_id(), 5); // 1st send -> a's 1st recorded emit (m5)
        assert_eq!(d.next_msg_id(), 8); // 2nd send -> m8
    }

    #[test]
    fn record_flags_divergence_on_mismatch() {
        let log = vec![rec_emit("a", 1, "m5", "b", 1, &[("a", 1)])];
        let mut state = ReplayState {
            recorded: RecordedIndex::build(&log),
            pending: HashMap::new(), send_count: HashMap::new(),
            divergences: Vec::new(), produced: HashSet::new(),
        };
        // A re-produced "a:1" that differs from the recorded emit (wrong dst).
        let mut bad = rec_emit("a", 1, "m5", "WRONG", 1, &[("a", 1)]);
        bad.op = Op::Emit;
        let mut d = ReplayDispatch { state: &mut state, current: ActorId::from("a") };
        d.record(bad);
        assert_eq!(state.divergences.len(), 1);
        assert_eq!(state.divergences[0].event_id, "a:1");
    }

    // A relay actor that forwards n+1 to its `next`, and a sink that computes.
    struct Relay {
        next: Addr<u32>,
    }
    impl Actor for Relay {
        type Msg = u32;
        fn handle(&mut self, n: u32, ctx: &mut Context) {
            ctx.send(&self.next, n + 1);
        }
    }
    struct Sink;
    impl Actor for Sink {
        type Msg = u32;
        fn handle(&mut self, _n: u32, ctx: &mut Context) {
            ctx.compute();
        }
    }

    #[test]
    fn relay_log_re_executes_faithfully() {
        // Hand-built recording of: world -> relay (m1); relay -> sink (m2); sink computes.
        let log = vec![
            CausalEvent { id: "world:1".into(), actor: "world".into(), seq: 1, op: Op::Emit, vclock: [("world".to_string(),1)].into(), lamport: 1, msg_id: Some("m1".into()), src: Some("world".into()), dst: Some("relay".into()), value: None, payload_hash: None },
            CausalEvent { id: "relay:1".into(), actor: "relay".into(), seq: 1, op: Op::Recv, vclock: [("world".to_string(),1),("relay".to_string(),1)].into(), lamport: 2, msg_id: Some("m1".into()), src: Some("world".into()), dst: Some("relay".into()), value: None, payload_hash: None },
            CausalEvent { id: "relay:2".into(), actor: "relay".into(), seq: 2, op: Op::Emit, vclock: [("world".to_string(),1),("relay".to_string(),2)].into(), lamport: 3, msg_id: Some("m2".into()), src: Some("relay".into()), dst: Some("sink".into()), value: None, payload_hash: None },
            CausalEvent { id: "sink:1".into(), actor: "sink".into(), seq: 1, op: Op::Recv, vclock: [("world".to_string(),1),("relay".to_string(),2),("sink".to_string(),1)].into(), lamport: 4, msg_id: Some("m2".into()), src: Some("relay".into()), dst: Some("sink".into()), value: None, payload_hash: None },
            CausalEvent { id: "sink:2".into(), actor: "sink".into(), seq: 2, op: Op::Compute, vclock: [("world".to_string(),1),("relay".to_string(),2),("sink".to_string(),2)].into(), lamport: 5, msg_id: None, src: None, dst: None, value: None, payload_hash: None },
        ];
        let mut sys = ReplaySystem::new();
        let sink = sys.spawn("sink", Sink);
        let relay = sys.spawn("relay", Relay { next: sink });
        sys.origin(&relay, 1u32);
        let outcome = sys.replay(&log).unwrap();
        assert!(outcome.is_faithful(), "expected faithful replay, got {:?}", outcome.divergences);
        assert_eq!(outcome.replayed.timeline.len(), 5);
    }

    #[test]
    fn deliver_stashes_into_pending_by_msg_id() {
        let log: Vec<CausalEvent> = vec![];
        let mut state = ReplayState {
            recorded: RecordedIndex::build(&log),
            pending: HashMap::new(), send_count: HashMap::new(),
            divergences: Vec::new(), produced: HashSet::new(),
        };
        let env = Envelope {
            msg: Box::new(42u32), vclock: VectorClock::new(), lamport: Lamport(1),
            msg_id: 7, src: ActorId::from("a"), dst: ActorId::from("b"),
        };
        let mut d = ReplayDispatch { state: &mut state, current: ActorId::from("a") };
        d.deliver(&NoopSink, env);
        let got = state.pending.remove(&7).unwrap();
        assert_eq!(*got.msg.downcast::<u32>().unwrap(), 42);
    }
}
