//! Replay engine assembly: poset -> validate -> linear extension -> viz artifacts.

use crate::error::ReplayError;
use crate::extension::linearize;
use crate::poset::Poset;
use lichtung_log::{write_events, CausalEvent};
use std::io::Write;

/// The viz contract emitted alongside the timeline. `events` are event ids in
/// canonical order; `edges` are direct happens-before edges; `concurrent` are the
/// incomparable (spacelike) pairs the viz draws as parallel.
#[derive(serde::Serialize)]
pub struct PosetJson {
    pub events: Vec<String>,
    pub edges: Vec<(String, String)>,
    pub concurrent: Vec<(String, String)>,
}

/// A replayed log: the canonical totally-ordered timeline plus its poset.
pub struct Replayed {
    pub timeline: Vec<CausalEvent>,
    pub poset_json: PosetJson,
}

impl Replayed {
    /// Write the timeline as JSON-lines (one event per line, extension order).
    pub fn write_timeline<W: Write>(&self, w: W) -> Result<(), ReplayError> {
        write_events(w, &self.timeline)?;
        Ok(())
    }

    /// Write the poset as a single JSON object.
    pub fn write_poset<W: Write>(&self, mut w: W) -> Result<(), ReplayError> {
        serde_json::to_writer(&mut w, &self.poset_json)?;
        Ok(())
    }
}

/// Ingest a recorded log -> validate fidelity -> emit the canonical replay.
pub fn replay_log(events: &[CausalEvent]) -> Result<Replayed, ReplayError> {
    let poset = Poset::build(events)?;
    poset.validate()?;
    let order = linearize(&poset)?;

    let timeline: Vec<CausalEvent> = order.iter().map(|&i| poset.events[i].clone()).collect();
    let edges: Vec<(String, String)> = poset
        .edges
        .iter()
        .map(|&(a, b)| (poset.events[a].id.clone(), poset.events[b].id.clone()))
        .collect();
    let concurrent = concurrent_pairs(&poset);
    let event_ids: Vec<String> = order.iter().map(|&i| poset.events[i].id.clone()).collect();

    Ok(Replayed {
        timeline,
        poset_json: PosetJson { events: event_ids, edges, concurrent },
    })
}

/// All incomparable (concurrent) event pairs, by reachability over the poset.
/// O(N²) in events; acceptable for demo-scale logs (the viz inputs). For very
/// large logs this is the obvious thing to optimize (bitset reachability).
fn concurrent_pairs(poset: &Poset) -> Vec<(String, String)> {
    let n = poset.events.len();
    let mut succ: Vec<Vec<usize>> = vec![Vec::new(); n];
    for &(a, b) in &poset.edges {
        succ[a].push(b);
    }
    let reach: Vec<Vec<bool>> = (0..n)
        .map(|s| {
            let mut seen = vec![false; n];
            let mut stack = vec![s];
            while let Some(u) = stack.pop() {
                for &v in &succ[u] {
                    if !seen[v] {
                        seen[v] = true;
                        stack.push(v);
                    }
                }
            }
            seen
        })
        .collect();

    let mut out = Vec::new();
    for a in 0..n {
        for b in (a + 1)..n {
            if !reach[a][b] && !reach[b][a] {
                out.push((poset.events[a].id.clone(), poset.events[b].id.clone()));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testlog::{ev, with_msg};
    use lichtung_log::Op;

    fn fanout_log() -> Vec<CausalEvent> {
        // world -> source; source -> a, source -> b (a,b concurrent); a,b -> sink.
        vec![
            with_msg(ev("world", 1, Op::Emit, 1, &[("world", 1)]), "m1", "world", "source"),
            with_msg(ev("source", 1, Op::Recv, 2, &[("world", 1), ("source", 1)]), "m1", "world", "source"),
            with_msg(ev("source", 2, Op::Emit, 3, &[("world", 1), ("source", 2)]), "m2", "source", "a"),
            with_msg(ev("source", 3, Op::Emit, 4, &[("world", 1), ("source", 3)]), "m3", "source", "b"),
            with_msg(ev("a", 1, Op::Recv, 5, &[("world", 1), ("source", 2), ("a", 1)]), "m2", "source", "a"),
            with_msg(ev("b", 1, Op::Recv, 6, &[("world", 1), ("source", 3), ("b", 1)]), "m3", "source", "b"),
            ev("a", 2, Op::Compute, 6, &[("world", 1), ("source", 2), ("a", 2)]),
            ev("b", 2, Op::Compute, 7, &[("world", 1), ("source", 3), ("b", 2)]),
        ]
    }

    #[test]
    fn timeline_is_deterministic_byte_identical() {
        let log = fanout_log();
        let r1 = replay_log(&log).unwrap();
        let r2 = replay_log(&log).unwrap();
        let mut b1 = Vec::new();
        let mut b2 = Vec::new();
        r1.write_timeline(&mut b1).unwrap();
        r2.write_timeline(&mut b2).unwrap();
        assert_eq!(b1, b2, "same log must replay byte-identically");
        assert_eq!(r1.timeline.len(), log.len());
    }

    #[test]
    fn timeline_respects_causal_order() {
        let log = fanout_log();
        let r = replay_log(&log).unwrap();
        let rank: std::collections::HashMap<&str, usize> =
            r.timeline.iter().enumerate().map(|(i, e)| (e.id.as_str(), i)).collect();
        // every recv must follow its emit
        assert!(rank["world:1"] < rank["source:1"]);
        assert!(rank["source:2"] < rank["a:1"]);
        assert!(rank["source:3"] < rank["b:1"]);
    }

    #[test]
    fn concurrent_workers_are_spacelike() {
        let log = fanout_log();
        let r = replay_log(&log).unwrap();
        let has = |x: &str, y: &str| {
            r.poset_json
                .concurrent
                .iter()
                .any(|(p, q)| (p == x && q == y) || (p == y && q == x))
        };
        // a's compute and b's compute are causally independent -> spacelike.
        assert!(has("a:2", "b:2"), "worker computes should be concurrent in poset.json");
    }

    #[test]
    fn poset_json_serializes() {
        let log = fanout_log();
        let r = replay_log(&log).unwrap();
        let mut buf = Vec::new();
        r.write_poset(&mut buf).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&buf).unwrap();
        assert!(v.get("events").is_some() && v.get("edges").is_some() && v.get("concurrent").is_some());
    }

    #[test]
    fn timeline_is_independent_of_input_order() {
        let log = fanout_log();
        let mut shuffled = log.clone();
        shuffled.reverse(); // a different input permutation of the SAME event set
        let mut a = Vec::new();
        let mut b = Vec::new();
        replay_log(&log).unwrap().write_timeline(&mut a).unwrap();
        replay_log(&shuffled).unwrap().write_timeline(&mut b).unwrap();
        assert_eq!(a, b, "canonical timeline must not depend on recorded input order");
    }
}
