//! Vector clocks and Lamport stamps: the exact happens-before embedding.

use std::collections::BTreeMap;
use std::cmp::Ordering;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct Lamport(pub u64);

impl Lamport {
    /// Local event: advance and return the new stamp.
    pub fn tick(&mut self) -> u64 {
        self.0 += 1;
        self.0
    }

    /// Receive a message stamped `other`: max then advance.
    pub fn update(&mut self, other: Lamport) -> u64 {
        self.0 = self.0.max(other.0) + 1;
        self.0
    }
}

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

    /// Wire form: actor-name → counter, matching the causal-log `vclock` object.
    pub fn to_string_map(&self) -> BTreeMap<String, u64> {
        self.0.iter().map(|(k, v)| (k.0.clone(), *v)).collect()
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

    #[test]
    fn lamport_tick_and_update() {
        let mut l = Lamport::default();
        assert_eq!(l.tick(), 1);
        assert_eq!(l.tick(), 2);
        // receiving a message stamped 5: max(2,5)+1 = 6
        assert_eq!(l.update(Lamport(5)), 6);
    }

    #[test]
    fn vclock_to_string_map() {
        let mut vc = VectorClock::new();
        vc.set(ActorId::from("rng"), 3);
        let m = vc.to_string_map();
        assert_eq!(m.get("rng"), Some(&3));
    }
}
