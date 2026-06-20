//! The causal event log: `CausalEvent` + JSON-lines I/O.
//! Wire-compatible with concurrency-causality's `event.schema.json`.

use std::collections::BTreeMap;
use std::io::{BufRead, Write};

#[derive(thiserror::Error, Debug)]
pub enum LogError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Write events as JSON-lines (one compact JSON object per line).
pub fn write_events<W: Write>(mut w: W, events: &[CausalEvent]) -> Result<(), LogError> {
    for e in events {
        let line = serde_json::to_string(e)?;
        w.write_all(line.as_bytes())?;
        w.write_all(b"\n")?;
    }
    Ok(())
}

/// Read JSON-lines, skipping blank lines.
pub fn read_events<R: BufRead>(r: R) -> Result<Vec<CausalEvent>, LogError> {
    let mut out = Vec::new();
    for line in r.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        out.push(serde_json::from_str(&line)?);
    }
    Ok(out)
}

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

    #[test]
    fn jsonl_write_then_read_is_identity() {
        let events = vec![sample(), {
            let mut e = sample();
            e.id = "e2".into();
            e.op = Op::Recv;
            e
        }];
        let mut buf: Vec<u8> = Vec::new();
        super::write_events(&mut buf, &events).unwrap();
        // exactly two lines, each valid JSON
        assert_eq!(buf.iter().filter(|b| **b == b'\n').count(), 2);
        let back = super::read_events(buf.as_slice()).unwrap();
        assert_eq!(events, back);
    }

    #[test]
    fn read_skips_blank_lines() {
        let input = b"\n\n";
        let back = super::read_events(&input[..]).unwrap();
        assert!(back.is_empty());
    }
}
