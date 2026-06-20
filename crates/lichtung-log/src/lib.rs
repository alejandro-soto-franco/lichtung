//! The causal event log: `CausalEvent` + JSON-lines I/O.
//! Wire-compatible with concurrency-causality's `event.schema.json`.

use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Op {
    Emit,
    Recv,
    Compute,
    Report,
    Drop,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CausalEvent {
    pub id: String,
    pub actor: String,
    pub seq: u64,
    pub op: Op,
    pub vclock: BTreeMap<String, u64>,
    pub lamport: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub src: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dst: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_hash: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> CausalEvent {
        CausalEvent {
            id: "e1".into(),
            actor: "rng".into(),
            seq: 0,
            op: Op::Emit,
            vclock: BTreeMap::from([("rng".to_string(), 1)]),
            lamport: 1,
            msg_id: Some("m1".into()),
            src: Some("rng".into()),
            dst: Some("multiplier".into()),
            value: Some(0.5),
            payload_hash: None,
        }
    }

    #[test]
    fn round_trips_through_json() {
        let e = sample();
        let s = serde_json::to_string(&e).unwrap();
        let back: CausalEvent = serde_json::from_str(&s).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn op_serializes_lowercase() {
        assert_eq!(serde_json::to_string(&Op::Recv).unwrap(), "\"recv\"");
    }

    #[test]
    fn omits_none_optionals() {
        let mut e = sample();
        e.payload_hash = None;
        let s = serde_json::to_string(&e).unwrap();
        assert!(!s.contains("payload_hash"));
    }
}
