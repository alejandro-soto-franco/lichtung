use lichtung_core::{Envelope, Mailbox, MailboxRx, MailboxTx};
use std::future::Future;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

/// Baseline lock-free mailbox: tokio's mpsc. The `Mailbox` trait keeps this
/// swappable (thingbuf/flume) per the spec; prod selects this flavor.
pub struct TokioMailbox;

#[derive(Clone)]
pub struct TokioTx(UnboundedSender<Envelope>);
pub struct TokioRx(UnboundedReceiver<Envelope>);

impl MailboxTx for TokioTx {
    #[inline]
    fn try_send(&self, env: Envelope) -> Result<(), Envelope> {
        self.0.send(env).map_err(|e| e.0)
    }
}

impl MailboxRx for TokioRx {
    #[inline]
    fn recv(&mut self) -> impl Future<Output = Option<Envelope>> + Send + '_ {
        self.0.recv()
    }
}

impl Mailbox for TokioMailbox {
    type Tx = TokioTx;
    type Rx = TokioRx;
    fn unbounded() -> (TokioTx, TokioRx) {
        let (tx, rx) = unbounded_channel();
        (TokioTx(tx), TokioRx(rx))
    }
}
