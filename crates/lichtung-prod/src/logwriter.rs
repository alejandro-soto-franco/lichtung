use lichtung_log::{CausalEvent, LogError};
use std::io::{BufWriter, Write};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;

/// Drain `rx`, serialize each event with `serde_json::to_writer` straight into a
/// `BufWriter` (no intermediate `String`), and flush only when the channel is
/// momentarily empty. One flush per drained burst, not per event.
pub fn spawn_log_writer<W>(
    mut rx: UnboundedReceiver<CausalEvent>,
    w: W,
) -> JoinHandle<Result<usize, LogError>>
where
    W: Write + Send + 'static,
{
    tokio::spawn(async move {
        let mut bw = BufWriter::new(w);
        let mut written = 0usize;
        while let Some(ev) = rx.recv().await {
            write_one(&mut bw, &ev)?;
            written += 1;
            // Drain everything currently queued before flushing.
            while let Ok(ev) = rx.try_recv() {
                write_one(&mut bw, &ev)?;
                written += 1;
            }
            bw.flush()?;
        }
        bw.flush()?;
        Ok(written)
    })
}

#[inline]
fn write_one<W: Write>(bw: &mut BufWriter<W>, ev: &CausalEvent) -> Result<(), LogError> {
    serde_json::to_writer(&mut *bw, ev)?;
    bw.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lichtung_log::Op;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct SharedBuf(Arc<Mutex<Vec<u8>>>);
    impl Write for SharedBuf {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn ev(n: u64) -> CausalEvent {
        CausalEvent {
            id: format!("a:{n}"),
            actor: "a".into(),
            seq: n,
            op: Op::Emit,
            vclock: BTreeMap::from([("a".to_string(), n)]),
            lamport: n,
            msg_id: Some(format!("m{n}")),
            src: Some("a".into()),
            dst: Some("b".into()),
            value: None,
            payload_hash: None,
        }
    }

    #[tokio::test]
    async fn writes_all_events_as_jsonl_and_reports_count() {
        let buf = SharedBuf(Arc::new(Mutex::new(Vec::new())));
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let h = spawn_log_writer(rx, buf.clone());
        for n in 1..=3 {
            tx.send(ev(n)).unwrap();
        }
        drop(tx); // close channel → writer finishes
        let count = h.await.unwrap().unwrap();
        assert_eq!(count, 3);
        let bytes = buf.0.lock().unwrap().clone();
        assert_eq!(bytes.iter().filter(|b| **b == b'\n').count(), 3);
        let back = lichtung_log::read_events(bytes.as_slice()).unwrap();
        assert_eq!(back.len(), 3);
        assert_eq!(back[0].id, "a:1");
    }
}
