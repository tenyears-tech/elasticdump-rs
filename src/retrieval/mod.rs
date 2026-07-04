mod context;
mod extract;
mod messages;
mod pit;
mod pit_search;
mod progress;
mod retrieval_task;
mod scroll;
mod search_body;
mod slice_state;
mod worker;

use anyhow::Result;
use bytesize::ByteSize;
use elasticsearch::Elasticsearch;
use indicatif::{MultiProgress, ProgressDrawTarget};
use log::{debug, info};
use sonic_rs::{JsonValueMutTrait, json};
use std::future::Future;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::cli::{Cli, SearchType};

use self::context::RetrievalContext;
use self::messages::RetrievalMessage;
use self::pit::SharedPitCoordinator;
use self::progress::setup_progress_bars;
use self::retrieval_task::{RetryPolicy, is_cancelled};
use self::slice_state::SliceState;

/// Why the pipeline token was cancelled. `Interrupt` (Ctrl+C) makes the dump
/// fail after cleanup; `OutputClosed` (stdout reader went away) makes the
/// cancellation cascade count as an early, successful stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CancelReason {
    Interrupt,
    OutputClosed,
}

/// Aborts the wrapped task when dropped, so the Ctrl+C listener dies with
/// `dump_data` on every return path instead of leaking.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn create_output_target_and_shared_pit<
    O,
    P,
    OutputFut,
    AbortFut,
    PitFut,
    OutputFactory,
    AbortFactory,
    PitFactory,
>(
    search_type: &SearchType,
    create_output_target: OutputFactory,
    abort_output_target: AbortFactory,
    open_shared_pit: PitFactory,
) -> Result<(O, Option<P>)>
where
    OutputFut: Future<Output = Result<O>>,
    AbortFut: Future<Output = ()>,
    PitFut: Future<Output = Result<P>>,
    OutputFactory: FnOnce() -> OutputFut,
    AbortFactory: FnOnce(O) -> AbortFut,
    PitFactory: FnOnce() -> PitFut,
{
    let output_target = create_output_target().await?;
    let shared_pit = if matches!(search_type, SearchType::PointInTime) {
        match open_shared_pit().await {
            Ok(shared_pit) => Some(shared_pit),
            Err(error) => {
                abort_output_target(output_target).await;
                return Err(error);
            }
        }
    } else {
        None
    };

    Ok((output_target, shared_pit))
}

/// Pick the pipeline's root-cause error. The output task's error (e.g. disk
/// full) outranks retrieval errors, which outrank worker errors: the latter
/// two are usually "channel closed" cascades of the former. A `DumpCancelled`
/// cascade never outranks a real error regardless of position. Every
/// suppressed error is still logged.
fn select_root_cause(
    output_error: Option<anyhow::Error>,
    retrieval_error: Option<anyhow::Error>,
    worker_error: Option<anyhow::Error>,
) -> Option<anyhow::Error> {
    let mut root_cause: Option<anyhow::Error> = None;
    for error in [output_error, retrieval_error, worker_error]
        .into_iter()
        .flatten()
    {
        let replaces_current = match &root_cause {
            None => true,
            Some(current) => is_cancelled(current) && !is_cancelled(&error),
        };
        if replaces_current {
            if let Some(previous) = root_cause.replace(error) {
                log::error!("Suppressed pipeline error: {:#}", previous);
            }
        } else {
            log::error!("Suppressed pipeline error: {:#}", error);
        }
    }
    root_cause
}

/// Close the shared PIT as best-effort cleanup. Failures are only warned
/// about: the PIT expires on its own via its keep-alive, and a close failure
/// must never override the pipeline outcome.
async fn close_pit_best_effort(client: &Elasticsearch, shared_pit: &Option<SharedPitCoordinator>) {
    let Some(shared_pit) = shared_pit else {
        return;
    };

    let latest_id = shared_pit.latest_id();
    let close_response = client
        .close_point_in_time()
        .body(sonic_rs::json!({ "id": latest_id }))
        .send()
        .await;

    match close_response {
        Ok(response) => {
            if let Err(e) =
                retrieval_task::read_checked_response_bytes(response, 0, "PIT close").await
            {
                log::warn!("Failed to close PIT (it will expire via keep-alive): {e}");
            }
        }
        Err(e) => {
            log::warn!("Failed to close PIT (it will expire via keep-alive): {e}");
        }
    }
}

/// Main function to dump data from Elasticsearch
pub async fn dump_data(client: &Elasticsearch, index: &str, args: Cli) -> Result<()> {
    debug!("Starting data dump operation for index: {}", index);
    let start_time = Instant::now();

    // Pipeline-wide cancellation: fired by Ctrl+C, output failures, and
    // failing slices. `cancel_reason` records WHY (set at most once) so the
    // final outcome can distinguish an interrupt from a closed stdout reader.
    let cancel = CancellationToken::new();
    let cancel_reason: Arc<OnceLock<CancelReason>> = Arc::new(OnceLock::new());

    // First Ctrl+C cancels the pipeline for an orderly shutdown (scroll/PIT
    // cleanup, staged-output abort); a second one force-quits with the
    // conventional 130 exit status.
    let signal_task = tokio::spawn({
        let cancel = cancel.clone();
        let reason = cancel_reason.clone();
        async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                let _ = reason.set(CancelReason::Interrupt);
                log::warn!(
                    "Interrupt received; cancelling dump and cleaning up (press Ctrl+C again to force-quit)"
                );
                cancel.cancel();
                if tokio::signal::ctrl_c().await.is_ok() {
                    std::process::exit(130);
                }
            }
        }
    });
    let _signal_guard = AbortOnDrop(signal_task);

    let retry = RetryPolicy {
        attempts: args.retry_attempts,
        base_delay: Duration::from_millis(args.retry_delay_ms),
    };

    // Prepare search body from user input
    let search_body = search_body::prepare_search_body(&args).await?;

    // Create channels for the pipeline
    let buffer_size = args.buffer_size;
    let slices = args.slices;

    // Setup for sliced search
    let use_sliced_scroll = slices > 0;
    let num_slices = if use_sliced_scroll { slices } else { 1 };

    // Extra workers beyond the slice count would still receive round-robined
    // jobs, but each slice keeps at most one unreplied job in flight, so
    // concurrent extraction can never exceed num_slices; more workers add
    // threads without adding parallelism.
    let workers = args.workers.min(num_slices);
    if workers < args.workers {
        info!(
            "Clamping worker count from {} to {} to match the {} search slice(s)",
            args.workers, workers, num_slices
        );
    }

    debug!(
        "Setting up pipeline with {} workers, buffer size {}, {} slices",
        workers, buffer_size, num_slices
    );

    // Create a channel for each worker
    let mut worker_txs = Vec::with_capacity(workers);
    let mut worker_rxs = Vec::with_capacity(workers);

    for _ in 0..workers {
        let (tx, rx) = mpsc::channel(buffer_size);
        worker_txs.push(tx);
        worker_rxs.push(rx);
    }

    // Create a channel for processed results
    let (processed_tx, mut processed_rx) = mpsc::channel(buffer_size);

    // Shared counters for stats
    let processed_count = Arc::new(AtomicU64::new(0));
    let processed_bytes = Arc::new(AtomicU64::new(0));
    let retrieved_count = Arc::new(AtomicU64::new(0));
    let retrieved_bytes = Arc::new(AtomicU64::new(0));

    // Setup progress bars if not in quiet mode
    let (_multi_progress, input_bar, output_bar) = if !args.quiet {
        let mp = MultiProgress::with_draw_target(ProgressDrawTarget::stderr());
        let (ib, ob) = setup_progress_bars(&mp);
        (Some(mp), ib, ob)
    } else {
        (None, None, None)
    };

    let mut estimated_total_hits = 0u64;
    let mut total_hits_are_exact = true;

    // Start worker tasks before any staged output or retrieval work begins.
    let worker_tasks = worker::spawn_worker_tasks(
        worker_rxs,
        processed_tx.clone(),
        Arc::clone(&processed_count),
        Arc::clone(&processed_bytes),
        output_bar.clone(),
        start_time,
    )?;

    let (output_target, shared_pit) = match create_output_target_and_shared_pit(
        &args.search_type,
        || crate::output::create_output_target(&args),
        |output_target| async move { output_target.abort().await },
        || {
            SharedPitCoordinator::open(
                client,
                index,
                &args.pit_keep_alive,
                num_slices,
                retry,
                cancel.clone(),
            )
        },
    )
    .await
    {
        Ok(targets) => targets,
        // Ctrl+C while the PIT open is retrying with backoff surfaces here as
        // a DumpCancelled; report it as the interrupt it is.
        Err(error) => {
            if matches!(cancel_reason.get(), Some(CancelReason::Interrupt)) && is_cancelled(&error)
            {
                return Err(anyhow::anyhow!("dump interrupted by user"));
            }
            return Err(error);
        }
    };

    // Drop the sender to signal no more processing will happen after worker clones are done.
    drop(processed_tx);

    // Output task. Any write failure cancels the pipeline so retrieval stops
    // promptly instead of dumping into a dead sink. A broken pipe on stdout is
    // the reader legitimately closing early (e.g. `| head`): record it, then
    // drain-and-discard so blocked workers/slices can unwind and observe the
    // cancellation.
    let output_task = tokio::spawn({
        let cancel = cancel.clone();
        let cancel_reason = Arc::clone(&cancel_reason);
        async move {
            let mut output_target = output_target;

            // Process output as it comes in
            while let Some(processed) = processed_rx.recv().await {
                // Write the entire buffer from the processed batch
                if let Err(e) = output_target.write_all(&processed.buffer).await {
                    if output_target.is_stdout() && crate::output::is_broken_pipe(&e) {
                        let _ = cancel_reason.set(CancelReason::OutputClosed);
                        cancel.cancel();
                        while processed_rx.recv().await.is_some() {}
                        return Ok(output_target);
                    }
                    cancel.cancel();
                    output_target.abort().await;
                    return Err(anyhow::anyhow!("Failed to write batch buffer: {}", e));
                }
            }

            if let Err(e) = output_target.flush().await {
                if output_target.is_stdout() && crate::output::is_broken_pipe(&e) {
                    let _ = cancel_reason.set(CancelReason::OutputClosed);
                    cancel.cancel();
                    return Ok(output_target);
                }
                cancel.cancel();
                output_target.abort().await;
                return Err(anyhow::anyhow!("Failed to flush writer: {}", e));
            }
            Ok(output_target)
        }
    });

    let total_hits_count = Arc::new(AtomicU64::new(0));
    let ctx = RetrievalContext {
        client: client.clone(),
        index: Arc::<str>::from(index.to_owned()),
        worker_txs: worker_txs.clone(),
        total_hits_count: Arc::clone(&total_hits_count),
        input_bar: input_bar.clone(),
        output_bar: output_bar.clone(),
        retrieved_count: Arc::clone(&retrieved_count),
        retrieved_bytes: Arc::clone(&retrieved_bytes),
        start_time,
        cancel: cancel.clone(),
        retry,
    };

    debug!(
        "Starting {} retrieval task{}",
        num_slices,
        if num_slices > 1 { "s" } else { "" }
    );

    // Start retrieval tasks for each slice
    let mut retrieval_tasks = Vec::with_capacity(num_slices);

    for slice_id in 0..num_slices {
        let mut slice_search_body = search_body.clone();
        if use_sliced_scroll {
            slice_search_body.as_object_mut().unwrap().insert(
                &"slice",
                json!({
                    "id": slice_id,
                    "max": num_slices
                }),
            );
            info!("Starting slice {}/{}", slice_id + 1, num_slices);
        }

        let slice_state = SliceState::new(slice_id, slice_id % workers, slice_search_body);
        let task = retrieval_task::spawn_retrieval_task(
            ctx.clone(),
            slice_state,
            args.search_type.clone(),
            args.scroll.clone(),
            args.pit_keep_alive.clone(),
            shared_pit.clone(),
        );

        retrieval_tasks.push(task);
    }

    // First retrieval error becomes a root-cause candidate; the rest are
    // logged. A real failure always displaces a `DumpCancelled` cascade, no
    // matter which slice finished first.
    let mut retrieval_error: Option<anyhow::Error> = None;

    // Wait for all retrieval tasks to complete and get total hits
    for task in retrieval_tasks {
        let error = match task.await {
            Ok(Ok(slice_hits)) => {
                estimated_total_hits += slice_hits.value;
                total_hits_are_exact &= slice_hits.is_exact;
                continue;
            }
            // `context` (not a reformatting `anyhow!`) keeps the DumpCancelled
            // sentinel downcastable for the final outcome selection.
            Ok(Err(e)) => e.context("Retrieval task failed"),
            Err(e) => anyhow::anyhow!("Retrieval task panicked: {}", e),
        };

        let replaces_current = match &retrieval_error {
            None => true,
            Some(current) => is_cancelled(current) && !is_cancelled(&error),
        };
        if replaces_current {
            if let Some(previous) = retrieval_error.replace(error) {
                log::error!(
                    "Suppressed additional retrieval task failure: {:#}",
                    previous
                );
            }
        } else {
            log::error!("Suppressed additional retrieval task failure: {:#}", error);
        }
    }

    // Final update of progress bar total
    if let Some(ib) = &input_bar {
        debug!(
            "Input bar final length: {}, Total hits: {}",
            ib.length().unwrap_or(0),
            estimated_total_hits
        );
        let final_input_length = if total_hits_are_exact {
            estimated_total_hits
        } else {
            estimated_total_hits.max(retrieved_count.load(Ordering::Relaxed))
        };
        if ib.length().unwrap_or(0) != final_input_length {
            ib.set_length(final_input_length);
            ib.set_message("Retrieving...");
        }
    }
    if let Some(ob) = &output_bar {
        let final_output_length = if total_hits_are_exact {
            estimated_total_hits
        } else {
            estimated_total_hits.max(processed_count.load(Ordering::Relaxed))
        };
        if ob.length().unwrap_or(0) != final_output_length {
            ob.set_length(final_output_length);
        }
    }

    // Signal that we're done retrieving by sending Done to all workers
    for (i, tx) in worker_txs.iter().enumerate() {
        if let Err(e) = tx.send(RetrievalMessage::Done).await {
            log::warn!("Failed to send Done signal to worker {}: {}", i, e);
        }
    }

    // Wait for all workers to finish. First failure becomes a root-cause
    // candidate; the rest are logged.
    let mut worker_error: Option<anyhow::Error> = None;
    for (i, task) in worker_tasks.into_iter().enumerate() {
        if let Err(e) = task.wait().await {
            let error = anyhow::anyhow!("Worker {} processing failed: {}", i, e);
            if worker_error.is_none() {
                worker_error = Some(error);
            } else {
                log::error!("Suppressed additional worker failure: {:#}", error);
            }
        }
    }

    // Wait for output to finish
    let mut completed_output_target = None;
    let output_error = match output_task.await {
        Ok(Ok(output_target)) => {
            completed_output_target = Some(output_target);
            None
        }
        Ok(Err(e)) => Some(anyhow::anyhow!("Output task failed: {}", e)),
        Err(e) => Some(anyhow::anyhow!("Output task panicked: {}", e)),
    };

    let mut root_cause = select_root_cause(output_error, retrieval_error, worker_error);

    // The stdout reader closing early (e.g. `| head`) is a legitimate way to
    // end a dump: the resulting DumpCancelled cascade counts as success. A
    // real error that surfaced alongside it still wins.
    if matches!(cancel_reason.get(), Some(CancelReason::OutputClosed))
        && root_cause.as_ref().is_some_and(is_cancelled)
    {
        root_cause = None;
    }

    // Ctrl+C makes the dump fail deterministically after the normal
    // error-path cleanup; a concurrent real error still takes priority over
    // the generic interrupt message.
    if matches!(cancel_reason.get(), Some(CancelReason::Interrupt)) {
        let error = match root_cause {
            Some(error) if !is_cancelled(&error) => error,
            Some(cascade) => {
                log::debug!(
                    "Suppressed cancellation cascade after interrupt: {:#}",
                    cascade
                );
                anyhow::anyhow!("dump interrupted by user")
            }
            None => anyhow::anyhow!("dump interrupted by user"),
        };
        if let Some(output_target) = completed_output_target {
            output_target.abort().await;
        }
        close_pit_best_effort(client, &shared_pit).await;
        return Err(error);
    }

    if let Some(error) = root_cause {
        if let Some(output_target) = completed_output_target {
            output_target.abort().await;
        }
        close_pit_best_effort(client, &shared_pit).await;
        return Err(error);
    }

    if matches!(cancel_reason.get(), Some(CancelReason::OutputClosed)) {
        log::info!("stdout closed by reader; stopping dump early");
    }

    let output_target = completed_output_target.ok_or_else(|| {
        anyhow::anyhow!("Output task completed without returning an output target")
    })?;

    // Make the dump durable FIRST; PIT cleanup afterwards can no longer
    // destroy it and is best-effort only.
    output_target.finalize().await?;
    close_pit_best_effort(client, &shared_pit).await;

    let elapsed = start_time.elapsed();
    let count = processed_count.load(Ordering::Relaxed);
    let bytes = processed_bytes.load(Ordering::Relaxed);
    let elapsed_secs = elapsed.as_secs_f64().max(1e-6);
    let bytes_per_sec = bytes as f64 / elapsed_secs;
    let docs_per_sec = count as f64 / elapsed_secs;

    // Finish progress bars
    let retrieved_docs = retrieved_count.load(Ordering::Relaxed);
    let final_retrieved_bytes = retrieved_bytes.load(Ordering::Relaxed);
    let retrieved_bytes_per_sec = final_retrieved_bytes as f64 / elapsed_secs;
    if let Some(ib) = input_bar {
        let summary = if total_hits_are_exact {
            format!(
                "Retrieved {} docs ({} @ {} /s)",
                retrieved_docs,
                ByteSize(final_retrieved_bytes),
                ByteSize(retrieved_bytes_per_sec as u64)
            )
        } else {
            format!(
                "Retrieved {} docs ({} @ {} /s, Elasticsearch reported total >= {})",
                retrieved_docs,
                ByteSize(final_retrieved_bytes),
                ByteSize(retrieved_bytes_per_sec as u64),
                estimated_total_hits
            )
        };
        ib.finish_with_message(summary);
    }
    if let Some(ob) = output_bar {
        ob.finish_with_message(format!(
            "Processed {} docs ({} @ {} /s)",
            count,
            ByteSize(bytes),
            ByteSize(bytes_per_sec as u64)
        ));
    }

    debug!(
        "Final stats: {} documents, {} bytes, {:?} elapsed",
        count, bytes, elapsed
    );

    info!(
        "Dump completed: {} documents ({}) in {:.2?} ({:.0} docs/sec, {}/sec)",
        count,
        ByteSize(bytes),
        elapsed,
        docs_per_sec,
        ByteSize(bytes_per_sec as u64)
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{create_output_target_and_shared_pit, select_root_cause};
    use anyhow::anyhow;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[test]
    fn select_root_cause_prefers_output_error_over_cascades() {
        let root = select_root_cause(
            Some(anyhow!("disk full")),
            Some(anyhow!("send batch failed")),
            Some(anyhow!("processed channel closed")),
        )
        .expect("root cause expected");

        assert_eq!(root.to_string(), "disk full");
    }

    #[test]
    fn select_root_cause_falls_back_to_retrieval_then_worker_error() {
        let root = select_root_cause(None, Some(anyhow!("retrieval")), Some(anyhow!("worker")))
            .expect("root cause expected");
        assert_eq!(root.to_string(), "retrieval");

        let root =
            select_root_cause(None, None, Some(anyhow!("worker"))).expect("root cause expected");
        assert_eq!(root.to_string(), "worker");
    }

    #[test]
    fn select_root_cause_is_none_without_errors() {
        assert!(select_root_cause(None, None, None).is_none());
    }

    #[test]
    fn select_root_cause_prefers_real_errors_over_cancellation_cascades() {
        use super::retrieval_task::DumpCancelled;

        // A cancellation cascade in a higher-priority slot must not mask the
        // real failure that triggered it.
        let root = select_root_cause(
            Some(anyhow::Error::new(DumpCancelled).context("Output task failed")),
            Some(anyhow!("HTTP 500 from slice 2")),
            None,
        )
        .expect("root cause expected");
        assert!(root.to_string().contains("HTTP 500"));

        // With nothing but cascades, the first one is still reported.
        let root = select_root_cause(
            None,
            Some(anyhow::Error::new(DumpCancelled).context("Retrieval task failed")),
            None,
        )
        .expect("root cause expected");
        assert!(super::retrieval_task::is_cancelled(&root));
    }

    #[tokio::test]
    async fn create_output_target_and_shared_pit_skips_pit_when_output_fails() {
        let pit_opened = Arc::new(AtomicBool::new(false));

        let error = create_output_target_and_shared_pit(
            &crate::cli::SearchType::PointInTime,
            || async { Err::<(), anyhow::Error>(anyhow!("output target failed")) },
            |_output_target| async {},
            {
                let pit_opened = Arc::clone(&pit_opened);
                move || async move {
                    pit_opened.store(true, Ordering::Relaxed);
                    Ok::<_, anyhow::Error>("pit-opened")
                }
            },
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(error.contains("output target failed"));
        assert!(!pit_opened.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn create_output_target_and_shared_pit_aborts_output_when_pit_open_fails() {
        let output_aborted = Arc::new(AtomicBool::new(false));

        let error = create_output_target_and_shared_pit(
            &crate::cli::SearchType::PointInTime,
            || async { Ok::<_, anyhow::Error>("output-target") },
            {
                let output_aborted = Arc::clone(&output_aborted);
                move |_output_target| async move {
                    output_aborted.store(true, Ordering::Relaxed);
                }
            },
            || async { Err::<&'static str, anyhow::Error>(anyhow!("pit open failed")) },
        )
        .await
        .unwrap_err()
        .to_string();

        assert!(error.contains("pit open failed"));
        assert!(output_aborted.load(Ordering::Relaxed));
    }
}
