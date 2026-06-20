//! lichtung-prod: the lock-free multi-core production executor.
//! Records the causal log of a real concurrent run.

mod logwriter;
mod shared;

pub use logwriter::spawn_log_writer;
pub use shared::SharedRuntime;
