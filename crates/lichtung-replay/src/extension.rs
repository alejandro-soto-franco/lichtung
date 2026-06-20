//! Linearization (Task 3).
use crate::error::ReplayError;
use crate::poset::Poset;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

/// Canonical linear extension of the poset: a deterministic topological sort.
/// Kahn's algorithm draining ready nodes smallest-first by `(lamport, actor, seq, idx)`.
/// The fixed key makes the output a pure function of the log.
pub fn linearize(poset: &Poset) -> Result<Vec<usize>, ReplayError> {
    let n = poset.events.len();
    let mut indeg = vec![0usize; n];
    let mut succ: Vec<Vec<usize>> = vec![Vec::new(); n];
    for &(a, b) in &poset.edges {
        succ[a].push(b);
        indeg[b] += 1;
    }

    let key = |i: usize| -> (u64, String, u64, usize) {
        let e = &poset.events[i];
        (e.lamport, e.actor.clone(), e.seq, i)
    };

    let mut heap: BinaryHeap<Reverse<(u64, String, u64, usize)>> = BinaryHeap::new();
    for i in 0..n {
        if indeg[i] == 0 {
            heap.push(Reverse(key(i)));
        }
    }

    let mut order = Vec::with_capacity(n);
    while let Some(Reverse((_, _, _, i))) = heap.pop() {
        order.push(i);
        for &j in &succ[i] {
            indeg[j] -= 1;
            if indeg[j] == 0 {
                heap.push(Reverse(key(j)));
            }
        }
    }

    if order.len() != n {
        return Err(ReplayError::Cycle(n - order.len()));
    }
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testlog::{ev, with_msg};
    use lichtung_log::Op;

    fn fanin_log() -> Vec<lichtung_log::CausalEvent> {
        // world -> a (m1); world -> b (m2); a and b are concurrent; both -> sink.
        vec![
            with_msg(ev("world", 1, Op::Emit, 1, &[("world", 1)]), "m1", "world", "a"),
            with_msg(ev("world", 2, Op::Emit, 2, &[("world", 2)]), "m2", "world", "b"),
            with_msg(ev("a", 1, Op::Recv, 3, &[("world", 1), ("a", 1)]), "m1", "world", "a"),
            with_msg(ev("b", 1, Op::Recv, 4, &[("world", 2), ("b", 1)]), "m2", "world", "b"),
        ]
    }

    #[test]
    fn order_respects_all_edges() {
        let events = fanin_log();
        let p = Poset::build(&events).unwrap();
        let order = linearize(&p).unwrap();
        let pos: std::collections::HashMap<usize, usize> =
            order.iter().enumerate().map(|(rank, &i)| (i, rank)).collect();
        for &(a, b) in &p.edges {
            assert!(pos[&a] < pos[&b], "edge {a}->{b} violated by the linear extension");
        }
    }

    #[test]
    fn is_deterministic() {
        let events = fanin_log();
        let p = Poset::build(&events).unwrap();
        assert_eq!(linearize(&p).unwrap(), linearize(&p).unwrap());
    }

    #[test]
    fn tie_break_orders_concurrent_by_lamport_then_actor() {
        // a:1 (lamport 3) and b:1 (lamport 4) are concurrent; a must precede b.
        let events = fanin_log();
        let p = Poset::build(&events).unwrap();
        let order = linearize(&p).unwrap();
        let id_order: Vec<&str> = order.iter().map(|&i| events[i].id.as_str()).collect();
        let ia = id_order.iter().position(|s| *s == "a:1").unwrap();
        let ib = id_order.iter().position(|s| *s == "b:1").unwrap();
        assert!(ia < ib, "lower-lamport concurrent event should come first");
    }
}
