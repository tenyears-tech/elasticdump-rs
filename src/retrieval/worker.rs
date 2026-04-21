use anyhow::{Result, anyhow};
use bytesize::ByteSize;
use indicatif::ProgressBar;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::{sync::mpsc, task::JoinHandle};

use super::{extract::ExtractedOutputBatch, messages::RetrievalMessage};

pub(crate) fn spawn_worker_task(
    id: usize,
    rx: mpsc::Receiver<RetrievalMessage>,
    processed_tx: mpsc::Sender<ExtractedOutputBatch>,
    processed_count: Arc<AtomicU64>,
    processed_bytes: Arc<AtomicU64>,
    output_bar: Option<ProgressBar>,
    start_time: Instant,
) -> JoinHandle<Result<()>> {
    tokio::task::spawn_blocking(move || {
        run_worker_loop(
            id,
            rx,
            processed_tx,
            processed_count,
            processed_bytes,
            output_bar,
            start_time,
        )
    })
}

pub(crate) fn run_worker_loop(
    id: usize,
    mut rx: mpsc::Receiver<RetrievalMessage>,
    processed_tx: mpsc::Sender<ExtractedOutputBatch>,
    processed_count: Arc<AtomicU64>,
    processed_bytes: Arc<AtomicU64>,
    output_bar: Option<ProgressBar>,
    start_time: Instant,
) -> Result<()> {
    while let Some(message) = rx.blocking_recv() {
        match message {
            RetrievalMessage::Batch(response_bytes) => {
                let processed = super::extract::build_output_batch(&response_bytes)?;
                let doc_count = processed.doc_count;
                let bytes_count = processed.buffer.len() as u64;

                processed_count.fetch_add(doc_count, Ordering::Relaxed);
                processed_bytes.fetch_add(bytes_count, Ordering::Relaxed);

                if let Some(ob) = &output_bar {
                    let current = processed_count.load(Ordering::Relaxed);
                    ob.set_position(current);

                    let bytes = processed_bytes.load(Ordering::Relaxed);
                    let elapsed_secs = start_time.elapsed().as_secs_f64().max(1e-6);
                    let bytes_per_sec = bytes as f64 / elapsed_secs;
                    ob.set_message(format!(
                        "{} @ {} /s",
                        ByteSize(bytes),
                        ByteSize(bytes_per_sec as u64)
                    ));
                }

                processed_tx.blocking_send(processed).map_err(|error| {
                    anyhow!("Worker {}: Failed to send processed batch: {}", id, error)
                })?;
            }
            RetrievalMessage::Done => break,
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::run_worker_loop;
    use crate::retrieval::messages::RetrievalMessage;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Instant,
    };
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn run_worker_loop_processes_batches_until_done() {
        let (tx, rx) = mpsc::channel(2);
        let (processed_tx, mut processed_rx) = mpsc::channel(2);

        let processed_count = Arc::new(AtomicU64::new(0));
        let processed_bytes = Arc::new(AtomicU64::new(0));
        let handle = tokio::task::spawn_blocking({
            let processed_count = Arc::clone(&processed_count);
            let processed_bytes = Arc::clone(&processed_bytes);
            move || {
                run_worker_loop(
                    0,
                    rx,
                    processed_tx,
                    processed_count,
                    processed_bytes,
                    None,
                    Instant::now(),
                )
            }
        });

        tx.send(RetrievalMessage::Batch(bytes::Bytes::from_static(
            br#"{"hits":{"hits":[{"_id":"1","_source":{"message":"a"}}]}}"#,
        )))
        .await
        .unwrap();
        tx.send(RetrievalMessage::Done).await.unwrap();

        let batch = processed_rx.recv().await.unwrap();
        assert_eq!(batch.doc_count, 1);
        assert_eq!(
            String::from_utf8(batch.buffer).unwrap(),
            "{\"_id\":\"1\",\"_source\":{\"message\":\"a\"}}\n"
        );

        handle.await.unwrap().unwrap();
        assert_eq!(processed_count.load(Ordering::Relaxed), 1);
        assert!(processed_bytes.load(Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn run_worker_loop_stops_cleanly_on_done_without_batches() {
        let (tx, rx) = mpsc::channel(1);
        let (processed_tx, mut processed_rx) = mpsc::channel(1);

        let handle = tokio::task::spawn_blocking(move || {
            run_worker_loop(
                1,
                rx,
                processed_tx,
                Arc::new(AtomicU64::new(0)),
                Arc::new(AtomicU64::new(0)),
                None,
                Instant::now(),
            )
        });

        tx.send(RetrievalMessage::Done).await.unwrap();

        handle.await.unwrap().unwrap();
        assert!(processed_rx.try_recv().is_err());
    }
}
