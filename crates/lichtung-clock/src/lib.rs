//! Vector clocks and Lamport stamps: the exact happens-before embedding.

use std::collections::BTreeMap;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, serde::Serialize, serde::Deserialize)]
pub struct ActorId(pub String);

impl From<&str> for ActorId {
    fn from(s: &str) -> Self {
        ActorId(s.to_string())
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct VectorClock(BTreeMap<ActorId, u64>);

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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
