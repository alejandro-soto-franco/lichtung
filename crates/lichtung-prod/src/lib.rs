//! lichtung-prod: the lock-free multi-core production executor.
//! Records the causal log of a real concurrent run.

mod dispatch;
mod logwriter;
mod mailbox;
mod shared;
mod system;

pub use logwriter::spawn_log_writer;
pub use mailbox::{TokioMailbox, TokioRx, TokioTx};
pub use shared::SharedRuntime;
pub use system::System;
