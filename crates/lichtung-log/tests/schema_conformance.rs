use lichtung_log::{CausalEvent, Op};
use std::collections::BTreeMap;

const SCHEMA: &str = include_str!("fixtures/event.schema.json");

fn events() -> Vec<CausalEvent> {
    vec![
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
        },
        CausalEvent {
            id: "e2".into(),
            actor: "multiplier".into(),
            seq: 1,
            op: Op::Recv,
            vclock: BTreeMap::from([("rng".to_string(), 1), ("multiplier".to_string(), 1)]),
            lamport: 2,
            msg_id: Some("m1".into()),
            src: Some("rng".into()),
            dst: Some("multiplier".into()),
            value: None,
            payload_hash: None,
        },
    ]
}

#[test]
fn every_event_validates_against_canonical_schema() {
    let schema: serde_json::Value = serde_json::from_str(SCHEMA).unwrap();
    let validator = jsonschema::validator_for(&schema).expect("schema compiles");
    for e in events() {
        let instance = serde_json::to_value(&e).unwrap();
        assert!(
            validator.is_valid(&instance),
            "event failed schema: {instance}"
        );
    }
}
