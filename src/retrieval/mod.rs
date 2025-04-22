mod retrieval_task;
mod progress;
mod messages;
mod search_body;

use anyhow::Result;
use bytesize::ByteSize;
use elasticsearch::Elasticsearch;
use log::{debug, info};
use indicatif::{MultiProgress, ProgressDrawTarget};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use std::time::Instant;

use crate::cli::Cli;

use self::messages::RetrievalMessage;
use self::progress::setup_progress_bars;

/// Main function to dump data from Elasticsearch
pub async fn dump_data<W: AsyncWrite + Unpin + Send + 'static>(
    client: &Elasticsearch,
    index: &str,
    args: Cli,
    writer: W,
) -> Result<()> {
    debug!("Starting data dump operation for index: {}", index);
    let start_time = Instant::now();

    // Prepare search body from user input
    let search_body = search_body::prepare_search_body(&args).await?;
    
    // Create channels for the pipeline
    let workers = args.workers;
    let buffer_size = args.buffer_size;
    let slices = args.slices;

    debug!(
        "Setting up pipeline with {} workers, buffer size {}, {} slices",
        workers,
        buffer_size,
        if slices > 0 { slices } else { 1 }
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

    // Setup for sliced search
    let use_sliced_scroll = slices > 0;
    let num_slices = if use_sliced_scroll { slices } else { 1 };
    let mut total_hits = 0u64;

    // Clone references for retrieval tasks
    let client_ref = client.clone();
    let total_hits_count = Arc::new(AtomicU64::new(0));

    debug!(
        "Starting {} retrieval task{}",
        num_slices,
        if num_slices > 1 { "s" } else { "" }
    );

    // Start retrieval tasks for each slice
    let mut retrieval_tasks = Vec::with_capacity(num_slices);

    for slice_id in 0..num_slices {
        let task = retrieval_task::spawn_retrieval_task(
            slice_id,
            client_ref.clone(),
            index.to_string(),
            worker_txs.clone(),
            search_body.clone(),
            args.search_type.clone(),
            args.scroll.clone(),
            args.pit_keep_alive.clone(),
            use_sliced_scroll,
            num_slices,
            Arc::clone(&total_hits_count),
            input_bar.clone(),
            output_bar.clone(),
            Arc::clone(&retrieved_count),
            Arc::clone(&retrieved_bytes),
            start_time.clone(),
        );

        retrieval_tasks.push(task);
    }

    // Start worker tasks
    let worker_tasks: Vec<_> = worker_rxs
        .into_iter()
        .enumerate()
        .map(|(id, mut rx)| {
            let processed_tx = processed_tx.clone();
            let processed_count = Arc::clone(&processed_count);
            let processed_bytes = Arc::clone(&processed_bytes);
            let output_bar = output_bar.clone();
            let start_time = start_time.clone();

            tokio::spawn(async move {
                while let Some(message) = rx.recv().await {
                    match message {
                        RetrievalMessage::Batch(batch) => {
                            // Move CPU-bound work to blocking thread pool
                            let processed =
                                tokio::task::spawn_blocking(move || crate::processing::process_batch(&batch))
                                    .await??;

                            // Update counters
                            let doc_count = processed.doc_count;
                            let bytes_count = processed.buffer.len() as u64;

                            processed_count.fetch_add(doc_count, Ordering::Relaxed);
                            processed_bytes.fetch_add(bytes_count, Ordering::Relaxed);

                            // Update progress bar
                            if let Some(ob) = &output_bar {
                                let current = processed_count.load(Ordering::Relaxed);
                                ob.set_position(current);

                                let bytes = processed_bytes.load(Ordering::Relaxed);
                                let elapsed_secs = start_time.elapsed().as_secs_f64().max(1e-6);
                                let bytes_per_sec = bytes as f64 / elapsed_secs;
                                ob.set_message(format!(
                                    "{} @ {} /s",
                                    ByteSize(current),
                                    ByteSize(bytes_per_sec as u64)
                                ));
                            }

                            // Send to output channel
                            if let Err(e) = processed_tx.send(processed).await {
                                return Err(anyhow::anyhow!(
                                    "Worker {}: Failed to send processed batch: {}",
                                    id,
                                    e
                                ));
                            }
                        }
                        RetrievalMessage::Done => {
                            // This worker is done
                            break;
                        }
                    }
                }
                Ok(())
            })
        })
        .collect();

    // Drop the sender to signal no more processing will happen
    drop(processed_tx);

    // Output task
    let output_task = tokio::spawn(async move {
        let mut writer = writer;

        // Process output as it comes in
        while let Some(processed) = processed_rx.recv().await {
            // Write the entire buffer from the processed batch
            if let Err(e) = writer.write_all(&processed.buffer).await {
                return Err(anyhow::anyhow!("Failed to write batch buffer: {}", e));
            }
        }

        // single flush after all batches are written
        if let Err(e) = writer.flush().await {
            return Err(anyhow::anyhow!("Failed to flush writer: {}", e));
        }
        Ok(())
    });

    // Wait for all retrieval tasks to complete and get total hits
    for task in retrieval_tasks {
        match task.await {
            Ok(result) => match result {
                Ok(slice_hits) => {
                    total_hits += slice_hits;
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Retrieval task failed: {}", e));
                }
            },
            Err(e) => {
                return Err(anyhow::anyhow!("Retrieval task panicked: {}", e));
            }
        }
    }

    // Final update of progress bar total
    if let Some(ib) = &input_bar {
        debug!(
            "Input bar final length: {}, Total hits: {}",
            ib.length().unwrap_or(0),
            total_hits
        );
        if ib.length().unwrap_or(0) != total_hits {
            ib.set_length(total_hits);
            ib.set_message("Retrieving...");
        }
    }
    if let Some(ob) = &output_bar {
        if ob.length().unwrap_or(0) != total_hits {
            ob.set_length(total_hits);
        }
    }

    // Signal that we're done retrieving by sending Done to all workers
    for (i, tx) in worker_txs.iter().enumerate() {
        if let Err(e) = tx.send(RetrievalMessage::Done).await {
            log::warn!("Failed to send Done signal to worker {}: {}", i, e);
        }
    }

    // Wait for all workers to finish
    for (i, task) in worker_tasks.into_iter().enumerate() {
        match task.await {
            Ok(Ok(())) => {} // Worker finished successfully
            Ok(Err(e)) => return Err(anyhow::anyhow!("Worker {} processing failed: {}", i, e)), 
            Err(e) => return Err(anyhow::anyhow!("Worker {} task panicked: {}", i, e)), 
        }
    }

    // Wait for output to finish
    match output_task.await {
        Ok(result) => {
            if let Err(e) = result {
                return Err(anyhow::anyhow!("Output task failed: {}", e));
            }
        }
        Err(e) => {
            return Err(anyhow::anyhow!("Output task panicked: {}", e));
        }
    }

    let elapsed = start_time.elapsed();
    let count = processed_count.load(Ordering::Relaxed);
    let bytes = processed_bytes.load(Ordering::Relaxed);

    // Finish progress bars
    let final_retrieved_bytes = retrieved_bytes.load(Ordering::Relaxed);
    if let Some(ib) = input_bar {
        ib.finish_with_message(format!(
            "Retrieved {} docs ({})",
            total_hits,
            ByteSize(final_retrieved_bytes)
        ));
    }
    if let Some(ob) = output_bar {
        ob.finish_with_message(format!("Processed {} docs ({})", count, ByteSize(bytes)));
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
        count as f64 / elapsed.as_secs_f64(),
        ByteSize((bytes as f64 / elapsed.as_secs_f64()) as u64)
    );

    Ok(())
} 
