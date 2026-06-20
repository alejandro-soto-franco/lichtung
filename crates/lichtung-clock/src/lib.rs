//! Vector clocks and Lamport stamps: the exact happens-before embedding.

use std::collections::BTreeMap;
use std::cmp::Ordering;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, serde::Serialize, serde::Deserialize)]
pub struct ActorId(pub String);

impl From<&str> for ActorId {
    fn from(s: &str) -> Self {
        ActorId(s.to_string())
    }
}

#[derive(Clone, Eq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct VectorClock(BTreeMap<ActorId, u64>);

impl PartialEq for VectorClock {
    fn eq(&self, other: &Self) -> bool {
        // Two clocks are equal if all actors have the same timestamp (treating missing as 0)
        let all_keys: std::collections::BTreeSet<_> =
            self.0.keys().chain(other.0.keys()).collect();
        all_keys.iter().all(|k| self.get(k) == other.get(k))
    }
}

impl VectorClock {
    pub fn new() -> Self {
        VectorClock(BTreeMap::new())
    }

    pub fn get(&self, actor: &ActorId) -> u64 {
        self.0.get(actor).copied().unwrap_or(0)
    }

    pub fn set(&mut self, actor: ActorId, value: u64) {
        self.0.insert(actor, value);
    }

    pub fn increment(&mut self, actor: &ActorId) {
        *self.0.entry(actor.clone()).or_insert(0) += 1;
    }

    pub fn merge(&mut self, other: &VectorClock) {
        for (actor, &v) in &other.0 {
            let slot = self.0.entry(actor.clone()).or_insert(0);
            *slot = (*slot).max(v);
        }
    }

    pub fn happens_before(&self, other: &VectorClock) -> bool {
        self.partial_cmp(other) == Some(Ordering::Less)
    }

    pub fn concurrent(&self, other: &VectorClock) -> bool {
        self.partial_cmp(other).is_none()
    }
}

impl PartialOrd for VectorClock {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let mut saw_less = false;
        let mut saw_greater = false;
        for key in self.0.keys().chain(other.0.keys()) {
            let a = self.get(key);
            let b = other.get(key);
            if a < b {
                saw_less = true;
            } else if a > b {
                saw_greater = true;
            }
            if saw_less && saw_greater {
                return None; // incomparable ⇒ concurrent
            }
        }
        match (saw_less, saw_greater) {
            (false, false) => Some(Ordering::Equal),
            (true, false) => Some(Ordering::Less),
            (false, true) => Some(Ordering::Greater),
            (true, true) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vc(pairs: &[(&str, u64)]) -> VectorClock {
        let mut v = VectorClock::new();
        for (k, n) in pairs {
            v.set(ActorId::from(*k), *n);
        }
        v
    }

    #[test]
    fn increment_and_get() {
        let a = ActorId::from("a");
        let mut vc = VectorClock::new();
        assert_eq!(vc.get(&a), 0);
        vc.increment(&a);
        vc.increment(&a);
        assert_eq!(vc.get(&a), 2);
        assert_eq!(vc.get(&ActorId::from("b")), 0);
    }

    #[test]
    fn happens_before_is_strict_product_order() {
        let a = vc(&[("a", 1)]);
        let b = vc(&[("a", 1), ("b", 1)]);
        assert!(a.happens_before(&b));
        assert!(!b.happens_before(&a));
        assert!(!a.happens_before(&a)); // irreflexive
    }

    #[test]
    fn incomparable_clocks_are_concurrent() {
        let a = vc(&[("a", 1)]);
        let b = vc(&[("b", 1)]);
        assert!(a.concurrent(&b));
        assert!(!a.happens_before(&b));
        assert!(!b.happens_before(&a));
    }

    #[test]
    fn merge_takes_componentwise_max() {
        let mut a = vc(&[("a", 2), ("b", 1)]);
        let b = vc(&[("a", 1), ("b", 5)]);
        a.merge(&b);
        assert_eq!(a.get(&ActorId::from("a")), 2);
        assert_eq!(a.get(&ActorId::from("b")), 5);
    }
}
