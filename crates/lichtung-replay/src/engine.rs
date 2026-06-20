//! Replay engine assembly (Task 4 fills this in).
use crate::error::ReplayError;
use lichtung_log::CausalEvent;

#[derive(serde::Serialize)]
pub struct PosetJson {
    pub events: Vec<String>,
    pub edges: Vec<(String, String)>,
    pub concurrent: Vec<(String, String)>,
}
pub struct Replayed {
    pub timeline: Vec<CausalEvent>,
    pub poset_json: PosetJson,
}
pub fn replay_log(_events: &[CausalEvent]) -> Result<Replayed, ReplayError> {
    unimplemented!("Task 4")
}
