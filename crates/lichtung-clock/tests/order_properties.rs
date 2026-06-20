use lichtung_clock::{ActorId, VectorClock};
use proptest::prelude::*;
use std::cmp::Ordering;

fn vclock_strategy() -> impl Strategy<Value = VectorClock> {
    proptest::collection::btree_map("[a-d]", 0u64..6, 0..4).prop_map(|m| {
        let mut vc = VectorClock::new();
        for (k, v) in m {
            vc.set(ActorId::from(k.as_str()), v);
        }
        vc
    })
}

proptest! {
    // Trichotomy-or-concurrent: exactly one of {<, >, =, ∥} holds.
    #[test]
    fn exactly_one_relation_holds(a in vclock_strategy(), b in vclock_strategy()) {
        let lt = a.happens_before(&b);
        let gt = b.happens_before(&a);
        let eq = a == b;
        let conc = a.concurrent(&b);
        let count = [lt, gt, eq, conc].iter().filter(|x| **x).count();
        prop_assert_eq!(count, 1);
    }

    // Irreflexive: nothing happens before itself.
    #[test]
    fn irreflexive(a in vclock_strategy()) {
        prop_assert!(!a.happens_before(&a));
    }

    // merge dominates both operands (a ≤ merge and b ≤ merge).
    #[test]
    fn merge_dominates(a in vclock_strategy(), b in vclock_strategy()) {
        let mut m = a.clone();
        m.merge(&b);
        let a_le = matches!(a.partial_cmp(&m), Some(Ordering::Less | Ordering::Equal));
        let b_le = matches!(b.partial_cmp(&m), Some(Ordering::Less | Ordering::Equal));
        prop_assert!(a_le);
        prop_assert!(b_le);
    }
}
