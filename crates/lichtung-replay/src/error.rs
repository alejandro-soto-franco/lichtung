/// Errors from reconstructing or linearizing a recorded causal log.
#[derive(thiserror::Error, Debug)]
pub enum ReplayError {
    #[error("recv event {0} has no matching emit (msg_id {1:?})")]
    MissingEmit(String, String),
    #[error("causal inconsistency: edge {0} -> {1}, but their vclocks are not strictly ordered")]
    Inconsistent(String, String),
    #[error("poset has a cycle; {0} events could not be scheduled")]
    Cycle(usize),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}
