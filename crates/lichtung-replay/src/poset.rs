use crate::error::ReplayError;
use lichtung_log::{CausalEvent, Op};
use std::collections::BTreeMap;

/// Strict product (vector-clock) order on string-keyed clocks: `a < b` iff
/// `a[k] <= b[k]` for every component and `a != b`. Missing components are 0.
fn hb_strict(a: &BTreeMap<String, u64>, b: &BTreeMap<String, u64>) -> bool {
    let mut saw_less = false;
    for k in a.keys().chain(b.keys()) {
        let x = a.get(k).copied().unwrap_or(0);
        let y = b.get(k).copied().unwrap_or(0);
        if x > y {
            return false; // a exceeds b somewhere -> not a <= b
        }
        if x < y {
            saw_less = true;
        }
    }
    saw_less
}

/// The reconstructed happens-before poset over a recorded log. Nodes are event
/// indices `0..events.len()`; `edges` are the DIRECT causal edges (program order
/// per actor + message `emit -> recv`).
#[derive(Debug)]
pub struct Poset<'a> {
    pub events: &'a [CausalEvent],
    pub edges: Vec<(usize, usize)>,
}

impl<'a> Poset<'a> {
    /// Reconstruct the poset. Edge sources:
    /// (a) per-actor program order: consecutive events of one actor by `seq`;
    /// (b) message order: each `emit` to its matching `recv` (same `msg_id`).
    pub fn build(events: &'a [CausalEvent]) -> Result<Self, ReplayError> {
        let mut edges = Vec::new();

        // (a) program-order edges
        let mut by_actor: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
        for (i, e) in events.iter().enumerate() {
            by_actor.entry(e.actor.as_str()).or_default().push(i);
        }
        for (actor, idxs) in &by_actor {
            let mut s = idxs.clone();
            s.sort_by_key(|&i| events[i].seq);
            for w in s.windows(2) {
                if events[w[0]].seq == events[w[1]].seq {
                    return Err(ReplayError::DuplicateSeq(
                        actor.to_string(),
                        events[w[0]].seq,
                    ));
                }
                edges.push((w[0], w[1]));
            }
        }

        // (b) message edges: emit -> recv paired by msg_id
        let mut emit_of: BTreeMap<&str, usize> = BTreeMap::new();
        for (i, e) in events.iter().enumerate() {
            if e.op == Op::Emit {
                if let Some(m) = &e.msg_id {
                    if emit_of.insert(m.as_str(), i).is_some() {
                        return Err(ReplayError::DuplicateEmit(m.clone()));
                    }
                }
            }
        }
        for (i, e) in events.iter().enumerate() {
            if e.op == Op::Recv {
                let m = e.msg_id.as_deref().unwrap_or("");
                let &emit_idx = emit_of
                    .get(m)
                    .ok_or_else(|| ReplayError::MissingEmit(e.id.clone(), m.to_string()))?;
                edges.push((emit_idx, i));
            }
        }

        Ok(Poset { events, edges })
    }

    /// Fidelity check: every reconstructed causal edge must be reflected in the
    /// recorded vector clocks (`a -> b` implies `V(a) < V(b)`). A log that fails
    /// this is not a faithful causal record — reject it loudly.
    pub fn validate(&self) -> Result<(), ReplayError> {
        for &(a, b) in &self.edges {
            if !hb_strict(&self.events[a].vclock, &self.events[b].vclock) {
                return Err(ReplayError::Inconsistent(
                    self.events[a].id.clone(),
                    self.events[b].id.clone(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testlog::{ev, with_msg};
    use lichtung_log::Op;

    #[test]
    fn builds_program_order_and_message_edges() {
        // world emit(m1) -> a recv(m1); a then emits(m2) -> b recv(m2).
        let events = vec![
            with_msg(ev("world", 1, Op::Emit, 1, &[("world", 1)]), "m1", "world", "a"),
            with_msg(ev("a", 1, Op::Recv, 2, &[("world", 1), ("a", 1)]), "m1", "world", "a"),
            with_msg(ev("a", 2, Op::Emit, 3, &[("world", 1), ("a", 2)]), "m2", "a", "b"),
            with_msg(ev("b", 1, Op::Recv, 4, &[("world", 1), ("a", 2), ("b", 1)]), "m2", "a", "b"),
        ];
        let p = Poset::build(&events).unwrap();
        // program order: a:1 -> a:2 (indices 1 -> 2)
        assert!(p.edges.contains(&(1, 2)), "program-order edge a:1->a:2 missing");
        // message edges: world emit(0) -> a recv(1); a emit(2) -> b recv(3)
        assert!(p.edges.contains(&(0, 1)), "message edge m1 missing");
        assert!(p.edges.contains(&(2, 3)), "message edge m2 missing");
    }

    #[test]
    fn recv_without_emit_is_missing_emit_error() {
        let events = vec![with_msg(ev("a", 1, Op::Recv, 1, &[("a", 1)]), "ghost", "x", "a")];
        let err = Poset::build(&events).unwrap_err();
        assert!(matches!(err, ReplayError::MissingEmit(_, _)));
    }

    #[test]
    fn valid_log_passes_validation() {
        let events = vec![
            with_msg(ev("world", 1, Op::Emit, 1, &[("world", 1)]), "m1", "world", "a"),
            with_msg(ev("a", 1, Op::Recv, 2, &[("world", 1), ("a", 1)]), "m1", "world", "a"),
        ];
        Poset::build(&events).unwrap().validate().unwrap();
    }

    #[test]
    fn inconsistent_vclock_on_an_edge_is_rejected() {
        // The recv's vclock does NOT dominate the emit's (missing the world:1 it must carry).
        let events = vec![
            with_msg(ev("world", 1, Op::Emit, 1, &[("world", 1)]), "m1", "world", "a"),
            with_msg(ev("a", 1, Op::Recv, 2, &[("a", 1)]), "m1", "world", "a"),
        ];
        let err = Poset::build(&events).unwrap().validate().unwrap_err();
        assert!(matches!(err, ReplayError::Inconsistent(_, _)));
    }

    #[test]
    fn duplicate_emit_msg_id_is_rejected() {
        let events = vec![
            with_msg(ev("a", 1, Op::Emit, 1, &[("a", 1)]), "dup", "a", "b"),
            with_msg(ev("a", 2, Op::Emit, 2, &[("a", 2)]), "dup", "a", "c"),
        ];
        assert!(matches!(Poset::build(&events).unwrap_err(), ReplayError::DuplicateEmit(_)));
    }

    #[test]
    fn duplicate_actor_seq_is_rejected() {
        let events = vec![
            ev("a", 1, Op::Compute, 1, &[("a", 1)]),
            ev("a", 1, Op::Compute, 2, &[("a", 1)]), // same (actor, seq)
        ];
        assert!(matches!(Poset::build(&events).unwrap_err(), ReplayError::DuplicateSeq(_, _)));
    }

    #[test]
    fn hb_strict_basics() {
        use std::collections::BTreeMap;
        let a: BTreeMap<String, u64> = [("x".to_string(), 1)].into();
        let b: BTreeMap<String, u64> = [("x".to_string(), 1), ("y".to_string(), 1)].into();
        assert!(super::hb_strict(&a, &b)); // a < b
        assert!(!super::hb_strict(&b, &a)); // not b < a
        assert!(!super::hb_strict(&a, &a)); // irreflexive
        let c: BTreeMap<String, u64> = [("y".to_string(), 1)].into();
        assert!(!super::hb_strict(&a, &c)); // concurrent {x:1} vs {y:1}
    }
}
