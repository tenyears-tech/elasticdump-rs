use anyhow::{Result, anyhow};
use bytesize::ByteSize;
use indicatif::ProgressBar;
use std::{
    any::Any,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Instant,
};
use tokio::sync::{mpsc, oneshot};

use super::{
    extract::ExtractedOutputBatch,
    messages::{BatchProcessingFailure, RetrievalMessage},
};

pub(crate) struct WorkerTask {
    completion_rx: oneshot::Receiver<Result<()>>,
}

impl WorkerTask {
    pub(crate) async fn wait(self) -> Result<()> {
        self.completion_rx
            .await
            .map_err(|_| anyhow!("Worker completion channel closed before reporting status"))?
    }
}

pub(crate) fn spawn_worker_tasks(
    worker_rxs: Vec<mpsc::Receiver<RetrievalMessage>>,
    processed_tx: mpsc::Sender<ExtractedOutputBatch>,
    processed_count: Arc<AtomicU64>,
    processed_bytes: Arc<AtomicU64>,
    output_bar: Option<ProgressBar>,
    start_time: Instant,
) -> Result<Vec<WorkerTask>> {
    spawn_worker_tasks_with(
        worker_rxs,
        processed_tx,
        processed_count,
        processed_bytes,
        output_bar,
        start_time,
        spawn_worker_task,
    )
}

pub(crate) fn spawn_worker_task(
    id: usize,
    rx: mpsc::Receiver<RetrievalMessage>,
    processed_tx: mpsc::Sender<ExtractedOutputBatch>,
    processed_count: Arc<AtomicU64>,
    processed_bytes: Arc<AtomicU64>,
    output_bar: Option<ProgressBar>,
    start_time: Instant,
) -> Result<WorkerTask> {
    let (completion_tx, completion_rx) = oneshot::channel();

    thread::Builder::new()
        .name(format!("retrieval-worker-{id}"))
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run_worker_loop(
                    id,
                    rx,
                    processed_tx,
                    processed_count,
                    processed_bytes,
                    output_bar,
                    start_time,
                )
            }))
            .unwrap_or_else(|panic_payload| Err(worker_thread_panic(id, panic_payload)));

            let _ = completion_tx.send(result);
        })
        .map_err(|error| anyhow!("Failed to spawn worker {id} thread: {error}"))?;

    Ok(WorkerTask { completion_rx })
}

fn spawn_worker_tasks_with<F>(
    worker_rxs: Vec<mpsc::Receiver<RetrievalMessage>>,
    processed_tx: mpsc::Sender<ExtractedOutputBatch>,
    processed_count: Arc<AtomicU64>,
    processed_bytes: Arc<AtomicU64>,
    output_bar: Option<ProgressBar>,
    start_time: Instant,
    mut spawn_worker: F,
) -> Result<Vec<WorkerTask>>
where
    F: FnMut(
        usize,
        mpsc::Receiver<RetrievalMessage>,
        mpsc::Sender<ExtractedOutputBatch>,
        Arc<AtomicU64>,
        Arc<AtomicU64>,
        Option<ProgressBar>,
        Instant,
    ) -> Result<WorkerTask>,
{
    worker_rxs
        .into_iter()
        .enumerate()
        .map(|(id, rx)| {
            spawn_worker(
                id,
                rx,
                processed_tx.clone(),
                Arc::clone(&processed_count),
                Arc::clone(&processed_bytes),
                output_bar.clone(),
                start_time,
            )
        })
        .collect()
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
            RetrievalMessage::Batch(job) => {
                let extracted =
                    match super::extract::extract_batch(&job.response_bytes, &job.search_type) {
                        Ok(extracted) => extracted,
                        Err(error) => {
                            let _ = job.reply_tx.send(Err(BatchProcessingFailure::from(anyhow!(
                                error.to_string()
                            ))));
                            return Err(error);
                        }
                    };

                let metadata = extracted.metadata;
                let processed = extracted.output;

                if let Err(error) = forward_processed_batch(
                    id,
                    processed,
                    &processed_tx,
                    &processed_count,
                    &processed_bytes,
                    output_bar.as_ref(),
                    start_time,
                ) {
                    let _ = job
                        .reply_tx
                        .send(Err(BatchProcessingFailure::with_metadata(
                            anyhow!(error.to_string()),
                            metadata,
                        )));
                    return Err(error);
                }

                let _ = job.reply_tx.send(Ok(metadata));
            }
            RetrievalMessage::Done => break,
        }
    }

    Ok(())
}

fn forward_processed_batch(
    id: usize,
    processed: ExtractedOutputBatch,
    processed_tx: &mpsc::Sender<ExtractedOutputBatch>,
    processed_count: &AtomicU64,
    processed_bytes: &AtomicU64,
    output_bar: Option<&ProgressBar>,
    start_time: Instant,
) -> Result<()> {
    let doc_count = processed.doc_count;
    let bytes_count = processed.buffer.len() as u64;

    if doc_count > 0 {
        processed_tx.blocking_send(processed).map_err(|error| {
            anyhow!("Worker {}: Failed to send processed batch: {}", id, error)
        })?;

        processed_count.fetch_add(doc_count, Ordering::Relaxed);
        processed_bytes.fetch_add(bytes_count, Ordering::Relaxed);
    }

    if let Some(ob) = output_bar {
        let current = processed_count.load(Ordering::Relaxed);
        ob.set_position(current);

        let bytes = processed_bytes.load(Ordering::Relaxed);
        let elapsed_secs = start_time.elapsed().as_secs_f64().max(1e-6);
        ob.set_message(format!(
            "{} @ {} /s",
            ByteSize(bytes),
            ByteSize((bytes as f64 / elapsed_secs) as u64)
        ));
    }

    Ok(())
}

fn worker_thread_panic(id: usize, panic_payload: Box<dyn Any + Send>) -> anyhow::Error {
    let panic_message = if let Some(message) = panic_payload.downcast_ref::<&'static str>() {
        (*message).to_owned()
    } else if let Some(message) = panic_payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_owned()
    };

    anyhow!("Worker {id} thread panicked: {panic_message}")
}

#[cfg(test)]
mod tests {
    use super::{WorkerTask, spawn_worker_task, spawn_worker_tasks_with};
    use crate::{
        cli::SearchType,
        retrieval::messages::{BatchJob, RetrievalMessage},
    };
    use anyhow::anyhow;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::Instant,
    };
    use tokio::sync::{mpsc, oneshot};

    #[tokio::test]
    async fn run_worker_loop_returns_metadata_and_output_from_one_batch() {
        let (tx, rx) = mpsc::channel(2);
        let (processed_tx, mut processed_rx) = mpsc::channel(2);

        let handle = spawn_worker_task(
            0,
            rx,
            processed_tx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            None,
            Instant::now(),
        )
        .unwrap();

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(RetrievalMessage::Batch(BatchJob {
            response_bytes: bytes::Bytes::from_static(
                br#"{"pit_id":"pit-next","hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"1","sort":[1,"a"],"_source":{"message":"a"}}]}}"#,
            ),
            search_type: SearchType::PointInTime,
            reply_tx,
        }))
        .await
        .unwrap();
        tx.send(RetrievalMessage::Done).await.unwrap();

        let metadata = reply_rx.await.unwrap().unwrap();
        assert_eq!(metadata.next_pit_id.as_deref(), Some("pit-next"));
        assert_eq!(metadata.last_sort_raw.as_deref(), Some(br#"[1,"a"]"#.as_slice()));

        let processed = processed_rx.recv().await.unwrap();
        assert_eq!(processed.doc_count, 1);
        assert_eq!(
            String::from_utf8(processed.buffer).unwrap(),
            "{\"_id\":\"1\",\"sort\":[1,\"a\"],\"_source\":{\"message\":\"a\"}}\n"
        );

        handle.wait().await.unwrap();
    }

    #[tokio::test]
    async fn run_worker_loop_ignores_dropped_metadata_receiver_after_forwarding_output() {
        let (tx, rx) = mpsc::channel(4);
        let (processed_tx, mut processed_rx) = mpsc::channel(4);

        let handle = spawn_worker_task(
            0,
            rx,
            processed_tx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            None,
            Instant::now(),
        )
        .unwrap();

        let (dropped_reply_tx, dropped_reply_rx) = oneshot::channel();
        drop(dropped_reply_rx);

        tx.send(RetrievalMessage::Batch(BatchJob {
            response_bytes: bytes::Bytes::from_static(
                br#"{"pit_id":"pit-first","hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"1","sort":[1,"a"],"_source":{"message":"a"}}]}}"#,
            ),
            search_type: SearchType::PointInTime,
            reply_tx: dropped_reply_tx,
        }))
        .await
        .unwrap();

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(RetrievalMessage::Batch(BatchJob {
            response_bytes: bytes::Bytes::from_static(
                br#"{"pit_id":"pit-second","hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"2","sort":[2,"b"],"_source":{"message":"b"}}]}}"#,
            ),
            search_type: SearchType::PointInTime,
            reply_tx,
        }))
        .await
        .unwrap();
        tx.send(RetrievalMessage::Done).await.unwrap();

        let metadata = reply_rx.await.unwrap().unwrap();
        assert_eq!(metadata.next_pit_id.as_deref(), Some("pit-second"));
        assert_eq!(metadata.last_sort_raw.as_deref(), Some(br#"[2,"b"]"#.as_slice()));

        let first_processed = processed_rx.recv().await.unwrap();
        assert_eq!(first_processed.doc_count, 1);
        assert_eq!(
            String::from_utf8(first_processed.buffer).unwrap(),
            "{\"_id\":\"1\",\"sort\":[1,\"a\"],\"_source\":{\"message\":\"a\"}}\n"
        );

        let second_processed = processed_rx.recv().await.unwrap();
        assert_eq!(second_processed.doc_count, 1);
        assert_eq!(
            String::from_utf8(second_processed.buffer).unwrap(),
            "{\"_id\":\"2\",\"sort\":[2,\"b\"],\"_source\":{\"message\":\"b\"}}\n"
        );

        handle.wait().await.unwrap();
    }

    #[tokio::test]
    async fn run_worker_loop_returns_scroll_metadata_for_empty_batch() {
        let (tx, rx) = mpsc::channel(2);
        let (processed_tx, mut processed_rx) = mpsc::channel(1);

        let handle = spawn_worker_task(
            0,
            rx,
            processed_tx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            None,
            Instant::now(),
        )
        .unwrap();

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(RetrievalMessage::Batch(BatchJob {
            response_bytes: bytes::Bytes::from_static(
                br#"{"_scroll_id":"scroll-next","hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}"#,
            ),
            search_type: SearchType::Scroll,
            reply_tx,
        }))
        .await
        .unwrap();
        tx.send(RetrievalMessage::Done).await.unwrap();

        let metadata = reply_rx.await.unwrap().unwrap();
        assert_eq!(metadata.next_scroll_id.as_deref(), Some("scroll-next"));
        assert!(metadata.next_pit_id.is_none());
        assert!(metadata.last_sort_raw.is_none());
        assert_eq!(metadata.total_hits.value, 0);
        assert!(metadata.total_hits.is_exact);
        assert_eq!(metadata.doc_count, 0);
        assert!(metadata.hits_are_empty);

        handle.wait().await.unwrap();
        assert!(processed_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn run_worker_loop_reports_extractor_error_to_reply_channel() {
        let (tx, rx) = mpsc::channel(1);
        let (processed_tx, mut processed_rx) = mpsc::channel(1);

        let handle = spawn_worker_task(
            0,
            rx,
            processed_tx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            None,
            Instant::now(),
        )
        .unwrap();

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(RetrievalMessage::Batch(BatchJob {
            response_bytes: bytes::Bytes::from_static(
                br#"{"pit_id":"pit-next","hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"1","_source":{"message":"a"}}]}}"#,
            ),
            search_type: SearchType::PointInTime,
            reply_tx,
        }))
        .await
        .unwrap();

        let error = reply_rx.await.unwrap().unwrap_err().to_string();
        assert!(error.contains("missing a usable sort value"));

        let worker_error = handle.wait().await.unwrap_err().to_string();
        assert!(worker_error.contains("missing a usable sort value"));
        assert!(processed_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn run_worker_loop_preserves_metadata_when_output_forwarding_fails() {
        let (tx, rx) = mpsc::channel(1);
        let (processed_tx, processed_rx) = mpsc::channel(1);
        drop(processed_rx);

        let handle = spawn_worker_task(
            0,
            rx,
            processed_tx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            None,
            Instant::now(),
        )
        .unwrap();

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(RetrievalMessage::Batch(BatchJob {
            response_bytes: bytes::Bytes::from_static(
                br#"{"pit_id":"pit-forward-failed","hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"1","sort":[5,"q"],"_source":{"message":"q"}}]}}"#,
            ),
            search_type: SearchType::PointInTime,
            reply_tx,
        }))
        .await
        .unwrap();

        let failure = reply_rx.await.unwrap().unwrap_err();
        let metadata = failure.metadata().expect("metadata should survive failure");
        assert_eq!(metadata.next_pit_id.as_deref(), Some("pit-forward-failed"));
        assert_eq!(metadata.last_sort_raw.as_deref(), Some(br#"[5,"q"]"#.as_slice()));
        assert_eq!(metadata.doc_count, 1);
        assert!(failure
            .into_error()
            .to_string()
            .contains("Failed to send processed batch"));

        let worker_error = handle.wait().await.unwrap_err().to_string();
        assert!(worker_error.contains("Failed to send processed batch"));
    }

    #[tokio::test]
    async fn run_worker_loop_processes_batch_jobs_until_done() {
        let (tx, rx) = mpsc::channel(2);
        let (processed_tx, mut processed_rx) = mpsc::channel(2);

        let processed_count = Arc::new(AtomicU64::new(0));
        let processed_bytes = Arc::new(AtomicU64::new(0));
        let handle = spawn_worker_task(
            0,
            rx,
            processed_tx,
            Arc::clone(&processed_count),
            Arc::clone(&processed_bytes),
            None,
            Instant::now(),
        )
        .unwrap();

        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(RetrievalMessage::Batch(BatchJob {
            response_bytes: bytes::Bytes::from_static(
                br#"{"_scroll_id":"scroll-next","hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"1","_source":{"message":"a"}}]}}"#,
            ),
            search_type: SearchType::Scroll,
            reply_tx,
        }))
        .await
        .unwrap();
        tx.send(RetrievalMessage::Done).await.unwrap();

        let metadata = reply_rx.await.unwrap().unwrap();
        assert_eq!(metadata.next_scroll_id.as_deref(), Some("scroll-next"));
        assert!(metadata.hits_are_empty == false);
        assert_eq!(metadata.doc_count, 1);

        let batch = processed_rx.recv().await.unwrap();
        assert_eq!(batch.doc_count, 1);
        assert_eq!(
            String::from_utf8(batch.buffer).unwrap(),
            "{\"_id\":\"1\",\"_source\":{\"message\":\"a\"}}\n"
        );

        handle.wait().await.unwrap();
        assert_eq!(processed_count.load(Ordering::Relaxed), 1);
        assert!(processed_bytes.load(Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn run_worker_loop_stops_cleanly_on_done_without_batches() {
        let (tx, rx) = mpsc::channel(1);
        let (processed_tx, mut processed_rx) = mpsc::channel(1);

        let handle = spawn_worker_task(
            1,
            rx,
            processed_tx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            None,
            Instant::now(),
        )
        .unwrap();

        tx.send(RetrievalMessage::Done).await.unwrap();

        handle.wait().await.unwrap();
        assert!(processed_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn spawn_worker_tasks_reports_start_failure() {
        let (_tx0, rx0) = mpsc::channel(1);
        let (_tx1, rx1) = mpsc::channel(1);
        let (processed_tx, _processed_rx) = mpsc::channel(1);

        let result = spawn_worker_tasks_with(
            vec![rx0, rx1],
            processed_tx,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            None,
            Instant::now(),
            |id,
             _rx,
             _processed_tx,
             _processed_count,
             _processed_bytes,
             _output_bar,
             _start_time| {
                if id == 1 {
                    return Err(anyhow!("injected worker start failure"));
                }

                let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
                let _ = completion_tx.send(Ok(()));
                Ok(WorkerTask { completion_rx })
            },
        );

        let error = match result {
            Ok(_) => panic!("worker startup unexpectedly succeeded"),
            Err(error) => error.to_string(),
        };

        assert!(error.contains("injected worker start failure"));
    }
}
