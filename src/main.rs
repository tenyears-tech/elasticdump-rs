use anyhow::{Context, Result, anyhow};
use base64::prelude::*;
use bytesize::ByteSize;
use clap::{Parser, ValueEnum};
use elasticsearch::{
    ClearScrollParts, Elasticsearch, OpenPointInTimeParts, ScrollParts, SearchParts,
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
};
use http::header::{ACCEPT_ENCODING, AUTHORIZATION, HeaderMap, HeaderValue};
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use log::{debug, info, warn};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::sync::mpsc;
use tokio::{
    fs as tokio_fs,
    io::{AsyncWrite, AsyncWriteExt, BufWriter as TokioBufWriter, stdout as tokio_stdout},
};
use url::Url;

#[derive(Debug, Clone, ValueEnum)]
enum DumpType {
    #[clap(name = "data")]
    Data,
}

#[derive(Debug, Clone, ValueEnum)]
enum SearchType {
    #[clap(name = "scroll")]
    Scroll,
    #[clap(name = "pit")]
    PointInTime,
}

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about = "A blazing fast Elasticsearch data dumper")]
struct Cli {
    /// Elasticsearch input URL (e.g. http://user:pass@localhost:9200/my_index)
    #[clap(long, required = true)]
    input: String,

    /// Output file path or stdout if '$'
    #[clap(long, required = true)]
    output: String,

    /// Type of operation, only 'data' is supported
    #[clap(long, default_value = "data")]
    r#type: DumpType,

    /// Number of documents to scroll per batch
    #[clap(long, default_value = "10000")]
    limit: usize,

    /// Optional JSON query string or @/path/to/file.json to filter documents
    #[clap(long("searchBody"))]
    search_body: Option<String>,

    /// Username for basic auth (optional, overrides username in --input URL)
    #[clap(long)]
    username: Option<String>,

    /// Password for basic auth (optional, overrides password in --input URL)
    #[clap(long)]
    password: Option<String>,

    /// Scroll timeout
    #[clap(long("scrollTime"), default_value = "10m")]
    scroll: String,

    /// Search type (scroll or point-in-time)
    #[clap(long("searchType"), default_value = "scroll")]
    search_type: SearchType,

    /// Point in Time keep alive value (when using pit search type)
    #[clap(long("pitKeepAlive"), default_value = "10m")]
    pit_keep_alive: String,

    /// Overwrite output file if it exists
    #[clap(long)]
    overwrite: bool,

    /// Quiet mode, suppress progress output
    #[clap(long)]
    quiet: bool,

    /// Debug mode, enable verbose logging
    #[clap(long)]
    debug: bool,

    /// Number of processing workers
    #[clap(long, default_value = "4")]
    workers: usize,

    /// Number of parallel slices for the Elasticsearch sliced scroll API (0 disables sliced scroll)
    #[clap(long, default_value = "0")]
    slices: usize,

    /// Buffer size for channels
    #[clap(long("bufferSize"), default_value = "16")]
    buffer_size: usize,

    /// Enable Elasticsearch response compression (default: disabled)
    #[clap(long("esCompress"))]
    es_compress: bool,
}

// Define message types for our channels
#[derive(Clone)]
enum RetrievalMessage {
    Batch(Arc<Value>),
    Done,
}

struct ProcessedBatch {
    buffer: Vec<u8>,
    doc_count: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    // Configure logger based on debug flag
    if args.debug {
        env_logger::Builder::from_default_env()
            .filter_level(log::LevelFilter::Debug)
            .init();
        debug!("Debug logging enabled");
    } else {
        env_logger::init();
    }

    // Parse input URL
    let input_url = Url::parse(&args.input).context("Failed to parse input URL")?;
    debug!("Parsed input URL: {}", input_url);

    // --- Host Extraction ---
    let host_str = input_url.host_str().unwrap_or("localhost");
    let port_str = input_url
        .port()
        .map_or("".to_string(), |p| format!(":{}", p));
    let host_url_str = format!("{}://{}{}", input_url.scheme(), host_str, port_str);
    let host_url = Url::parse(&host_url_str)?;
    debug!("Extracted host URL: {}", host_url);

    // --- Index Extraction ---
    let mut path_segments = input_url
        .path_segments()
        .map(|segments| segments.collect::<Vec<_>>())
        .unwrap_or_default();
    path_segments.retain(|segment| !segment.is_empty());
    if path_segments.is_empty() {
        return Err(anyhow!("No index specified in the input URL path"));
    }
    let index = path_segments[0];
    debug!("Using index: {}", index);

    info!(
        "Dumping data from index: {} at {}",
        index,
        host_url.as_str()
    );

    // --- Authentication ---
    // Priority: Flags > URL > None
    let url_username = input_url.username();
    let url_password = input_url.password();

    let auth_username = args.username.as_deref().or_else(|| {
        if !url_username.is_empty() {
            Some(url_username)
        } else {
            None
        }
    });
    let auth_password = args.password.as_deref().or(url_password);

    // --- Client Setup ---
    debug!(
        "Setting up Elasticsearch client connection to {}",
        host_url.as_str()
    );
    let conn_pool = SingleNodeConnectionPool::new(host_url.clone());
    let mut transport_builder = TransportBuilder::new(conn_pool);

    let mut headers = HeaderMap::new();

    if let (Some(user), Some(pass)) = (auth_username, auth_password) {
        info!("Using basic authentication for user: {}", user);
        let auth_str = format!("{}:{}", user, pass);
        let auth_val = format!("Basic {}", BASE64_STANDARD.encode(auth_str));
        headers.insert(AUTHORIZATION, HeaderValue::from_str(&auth_val)?);
        debug!("Adding authorization header");
    } else if auth_username.is_some() || auth_password.is_some() {
        warn!("Partial basic auth credentials provided (username or password missing), ignoring.");
    }

    // Add Accept-Encoding: identity by default, unless --esCompress is set
    if !args.es_compress {
        debug!("Disabling response compression by setting Accept-Encoding: identity (default)");
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    } else {
        debug!("Allowing Elasticsearch response compression (--esCompress specified)");
    }

    // Set headers on the builder if any were added
    if !headers.is_empty() {
        transport_builder = transport_builder.headers(headers);
    }

    debug!("Building Elasticsearch transport");
    let transport = transport_builder
        .build()
        .context("Failed to build Elasticsearch transport")?;
    let client = Elasticsearch::new(transport);
    debug!("Elasticsearch client created successfully");

    // --- Output Writer Setup ---
    debug!("Setting up output writer");
    if args.output == "$" {
        // Write to stdout
        debug!("Using stdout for output");
        let stdout = tokio_stdout();
        let writer = TokioBufWriter::new(stdout);
        dump_data(&client, index, args.clone(), writer).await?;
    } else {
        // Write to file
        debug!("Using file for output: {}", args.output);
        let mut open_options = tokio_fs::OpenOptions::new();
        open_options.write(true).create(true);
        if args.overwrite {
            debug!("Output file will be overwritten if it exists");
            open_options.truncate(true);
        } else {
            debug!("Output file will fail if it already exists");
            open_options.create_new(true);
        }

        let file = open_options.open(&args.output).await.with_context(|| {
            if !args.overwrite && std::path::Path::new(&args.output).exists() {
                format!(
                    "Output file '{}' already exists. Use --overwrite to replace it.",
                    args.output
                )
            } else {
                format!("Failed to create/open output file: {}", args.output)
            }
        })?;

        let writer = TokioBufWriter::new(file);
        debug!("Output file opened successfully");
        dump_data(&client, index, args.clone(), writer).await?;
    }

    Ok(())
}

async fn dump_data<W: AsyncWrite + Unpin + Send + 'static>(
    client: &Elasticsearch,
    index: &str,
    args: Cli,
    writer: W,
) -> Result<()> {
    debug!("Starting data dump operation for index: {}", index);
    let start_time = Instant::now();

    // --- Prepare Search Body ---
    let search_body_json = if let Some(search_body_str) = args.search_body {
        if search_body_str.starts_with('@') {
            let file_path = &search_body_str[1..];
            debug!("Loading search body from file: {}", file_path);
            let content = tokio_fs::read_to_string(file_path)
                .await
                .with_context(|| format!("Failed to read search body from file: {}", file_path))?;
            serde_json::from_str(&content)?
        } else {
            debug!("Parsing search body from command line parameter");
            serde_json::from_str(&search_body_str)
                .context("Failed to parse search body JSON string")?
        }
    } else {
        debug!("No search body provided, using empty query");
        json!({})
    };

    // Ensure we have a JSON object, wrap if necessary (e.g., if just a query string was provided)
    let mut final_search_body = if !search_body_json.is_object() {
        debug!("Search body is not an object, wrapping in a query object");
        json!({ "query": search_body_json })
    } else {
        search_body_json
    };

    // --- Initial Scroll Search Setup ---
    let scroll_ttl = &args.scroll;
    let scroll_size = args.limit;
    let search_body_obj = final_search_body.as_object_mut().unwrap(); // Safe because we ensured it's an object

    // Add size parameter to search body
    search_body_obj.insert("size".to_string(), json!(scroll_size));
    debug!(
        "Final search body: {}",
        serde_json::to_string(&search_body_obj).unwrap_or_default()
    );

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

    // --- Progress Bar Setup (if not quiet and not stdout) ---
    let progress_bar = if !args.quiet {
        // We don't know the total hits yet, we'll update it later
        let pb = ProgressBar::with_draw_target(Some(0), ProgressDrawTarget::stderr());
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({per_sec}, {msg}) {eta}")
                .unwrap()
                .progress_chars("#>-"),
        );
        Some(pb)
    } else {
        None
    };

    // --- Sliced Scroll Setup ---
    let use_sliced_scroll = slices > 0;
    let num_slices = if use_sliced_scroll { slices } else { 1 };
    let mut total_hits = 0u64;

    // Clone references for retrieval tasks
    let client_ref = client.clone();

    debug!(
        "Starting {} retrieval task{}",
        num_slices,
        if num_slices > 1 { "s" } else { "" }
    );

    // Start retrieval tasks for each slice
    let mut retrieval_tasks = Vec::with_capacity(num_slices);
    let total_hits_count = Arc::new(AtomicU64::new(0));

    for slice_id in 0..num_slices {
        // Clone necessary values for the task
        let client = client_ref.clone();
        let index = index.to_string();
        let worker_txs = worker_txs.clone();
        let scroll_ttl_str = scroll_ttl.clone();
        let pit_keep_alive = args.pit_keep_alive.clone();
        let mut search_body = search_body_obj.clone();
        let search_type = args.search_type.clone();
        let total_hits_count = Arc::clone(&total_hits_count);
        let progress_bar = progress_bar.clone();

        // Set up slice if using sliced scroll
        if use_sliced_scroll {
            search_body.insert(
                "slice".to_string(),
                json!({
                    "id": slice_id,
                    "max": num_slices
                }),
            );
            info!("Starting slice {}/{}", slice_id + 1, num_slices);
        }

        let task = tokio::spawn(async move {
            debug!("Slice {}: Starting retrieval task", slice_id);
            // Initial search setup varies based on search type
            let response_result = match search_type {
                SearchType::Scroll => {
                    debug!(
                        "Slice {}: Initiating scroll search with size {}",
                        slice_id, search_body["size"]
                    );
                    // Use Scroll API
                    client
                        .search(SearchParts::Index(&[&index]))
                        .scroll(&scroll_ttl_str)
                        .body(&search_body)
                        .send()
                        .await
                }
                SearchType::PointInTime => {
                    debug!(
                        "Slice {}: Opening point in time with keep_alive {}",
                        slice_id, &pit_keep_alive
                    );
                    // Open a Point in Time first
                    let pit_response = client
                        .open_point_in_time(OpenPointInTimeParts::Index(&[&index]))
                        .keep_alive(&pit_keep_alive)
                        .send()
                        .await;

                    let pit_id = match pit_response {
                        Ok(resp) => {
                            debug!("Slice {}: PIT opened successfully", slice_id);
                            let pit_json: Value = match resp.json().await {
                                Ok(json) => json,
                                Err(e) => {
                                    return Err(anyhow!(
                                        "Slice {}: Failed to parse PIT open response: {}",
                                        slice_id,
                                        e
                                    ));
                                }
                            };

                            match pit_json.get("id").and_then(Value::as_str) {
                                Some(id) => {
                                    debug!("Slice {}: Got PIT ID: {}", slice_id, id);
                                    id.to_string()
                                }
                                None => {
                                    return Err(anyhow!(
                                        "Slice {}: No PIT ID found in response",
                                        slice_id
                                    ));
                                }
                            }
                        }
                        Err(e) => {
                            return Err(anyhow!("Slice {}: Failed to open PIT: {}", slice_id, e));
                        }
                    };

                    // Add PIT to search body
                    search_body.insert(
                        "pit".to_string(),
                        json!({
                            "id": pit_id,
                            "keep_alive": pit_keep_alive
                        }),
                    );

                    // Add default sort to make search_after work reliably
                    // Without explicit sort, search_after pagination won't work properly
                    if !search_body.contains_key("sort") {
                        search_body.insert("sort".to_string(), json!(["_id"]));
                    }

                    // Execute search with PIT ID
                    client
                        .search(SearchParts::None)
                        .body(&search_body)
                        .send()
                        .await
                }
            };

            let response = match response_result {
                Ok(r) => {
                    debug!("Slice {}: Initial search request successful", slice_id);
                    r
                }
                Err(e) => {
                    return Err(anyhow!(
                        "Slice {}: Failed to initiate search: {} - this might indicate connection issues or invalid credentials",
                        slice_id,
                        e
                    ));
                }
            };

            let search_response: Value = match response.json().await {
                Ok(json) => {
                    debug!("Slice {}: Successfully parsed initial response", slice_id);
                    json
                }
                Err(e) => {
                    return Err(anyhow!(
                        "Slice {}: Failed to parse initial response: {} - this might indicate malformed JSON or unexpected response format",
                        slice_id,
                        e
                    ));
                }
            };

            // Extract ID for continued searches (scroll_id or pit.id)
            let (id_opt, is_pit) = match search_type {
                SearchType::Scroll => {
                    let scroll_id = search_response
                        .get("_scroll_id")
                        .and_then(Value::as_str)
                        .map(|id| id.to_string());

                    if let Some(id) = &scroll_id {
                        debug!("Slice {}: Got scroll_id: {}", slice_id, id);
                    } else {
                        debug!("Slice {}: No scroll_id found in response", slice_id);
                    }

                    (scroll_id, false)
                }
                SearchType::PointInTime => {
                    // For PIT we already have the ID from the open_point_in_time call
                    // We need to extract it from search_body since we added it there
                    (
                        search_body
                            .get("pit")
                            .and_then(|pit| pit.get("id"))
                            .and_then(Value::as_str)
                            .map(|id| id.to_string()),
                        true,
                    )
                }
            };

            // Get total hits for this slice
            let slice_total_hits = search_response["hits"]["total"]["value"]
                .as_u64()
                .unwrap_or_else(|| {
                    search_response["hits"]["hits"]
                        .as_array()
                        .map_or(0, |h| h.len()) as u64
                });

            debug!("Slice {}: Total hits: {}", slice_id, slice_total_hits);

            // Update the shared total hits counter
            let previous_total = total_hits_count.fetch_add(slice_total_hits, Ordering::Relaxed);
            let new_total = previous_total + slice_total_hits;

            // Update progress bar length if this is the last slice to report
            if let Some(pb) = &progress_bar {
                pb.set_length(new_total);
                debug!("Updated progress bar length to: {}", new_total);
            }

            // Process the initial batch regardless
            let response_data = Arc::new(search_response);
            let next_worker = slice_id % worker_txs.len();
            if let Err(e) = worker_txs[next_worker]
                .send(RetrievalMessage::Batch(response_data.clone()))
                .await
            {
                // Clean up PIT if needed before returning
                if is_pit {
                    if let Some(id) = &id_opt {
                        let _ = client
                            .close_point_in_time()
                            .body(json!({ "id": id }))
                            .send()
                            .await;
                    }
                }
                return Err(anyhow!(
                    "Failed to send initial batch for slice {}: {}",
                    slice_id,
                    e
                ));
            }

            let mut next_worker = (next_worker + 1) % worker_txs.len();

            // If no ID, return early
            let id = match id_opt {
                Some(id) => id,
                None => {
                    return Err(anyhow!(
                        "No ID found for slice {} to continue search",
                        slice_id
                    ));
                }
            };

            // Continue searching for this slice
            let mut retrieved_hits = 0u64;
            if let Some(initial_hits) = response_data["hits"]["hits"].as_array() {
                retrieved_hits += initial_hits.len() as u64;
            }

            // Create a search_after param from the initial response to continue pagination
            let mut search_after = None;
            if is_pit {
                if let Some(last_hit) = response_data["hits"]["hits"]
                    .as_array()
                    .and_then(|hits| hits.last())
                {
                    if let Some(sort) = last_hit.get("sort") {
                        search_after = Some(sort.clone());
                    }
                }
            }

            loop {
                debug!("Slice {}: Fetching next batch", slice_id);
                let next_response = match search_type {
                    SearchType::Scroll => {
                        // Create scroll request body with scroll_id and ttl
                        let scroll_body = json!({
                            "scroll": scroll_ttl_str,
                            "scroll_id": id
                        });

                        debug!("Slice {}: Scrolling with ID {}", slice_id, id);
                        client
                            .scroll(ScrollParts::None)
                            .body(scroll_body)
                            .send()
                            .await
                    }
                    SearchType::PointInTime => {
                        debug!("Slice {}: Continuing PIT search with ID {}", slice_id, id);
                        // For PIT, we need to create a new search body with:
                        // 1. The PIT ID
                        // 2. search_after from last result
                        // 3. Any other search params (query, etc.)
                        let mut next_pit_body = search_body.clone();

                        // Make sure we have a sort parameter (required for search_after)
                        if !next_pit_body.contains_key("sort") {
                            debug!(
                                "Slice {}: Adding default sort by _id for PIT search",
                                slice_id
                            );
                            next_pit_body.insert("sort".to_string(), json!(["_id"]));
                        }

                        // Add search_after from the last response if available
                        if let Some(sort_values) = &search_after {
                            debug!("Slice {}: Using search_after from last result", slice_id);
                            next_pit_body.insert("search_after".to_string(), sort_values.clone());
                        } else {
                            debug!("Slice {}: No search_after values available", slice_id);
                        }

                        client
                            .search(SearchParts::None)
                            .body(&next_pit_body)
                            .send()
                            .await
                    }
                };

                let next_response = match next_response {
                    Ok(res) => {
                        debug!("Slice {}: Next batch request successful", slice_id);
                        res
                    }
                    Err(e) => {
                        // Attempt to clean up before returning
                        match search_type {
                            SearchType::Scroll => {
                                debug!(
                                    "Slice {}: Attempting to clean up scroll context after error",
                                    slice_id
                                );
                                let clear_scroll_body = json!({ "scroll_id": [id.clone()] });
                                let _ = client
                                    .clear_scroll(ClearScrollParts::None)
                                    .body(clear_scroll_body)
                                    .send()
                                    .await;
                            }
                            SearchType::PointInTime => {
                                debug!(
                                    "Slice {}: Attempting to clean up PIT after error",
                                    slice_id
                                );
                                let _ = client
                                    .close_point_in_time()
                                    .body(json!({ "id": id }))
                                    .send()
                                    .await;
                            }
                        }
                        return Err(anyhow!(
                            "Slice {}: Search continuation error: {}",
                            slice_id,
                            e
                        ));
                    }
                };

                let next_response_json: Value = match next_response.json().await {
                    Ok(json) => json,
                    Err(e) => {
                        return Err(anyhow!(
                            "Slice {}: Failed to parse continuation response: {}",
                            slice_id,
                            e
                        ));
                    }
                };

                // Check if we have any hits
                let hits = next_response_json["hits"]["hits"].as_array();
                if hits.map_or(true, |h| h.is_empty()) {
                    info!("Search finished for slice {}, no more documents.", slice_id);
                    break; // No more hits
                }

                // Count hits
                if let Some(hits_array) = hits {
                    let batch_size = hits_array.len();
                    retrieved_hits += batch_size as u64;
                    debug!(
                        "Slice {}: Retrieved batch with {} documents (total: {})",
                        slice_id, batch_size, retrieved_hits
                    );
                }

                // For PIT, update search_after for next iteration from the last hit's sort values
                if is_pit {
                    if let Some(last_hit) = hits.and_then(|h| h.last()) {
                        if let Some(sort) = last_hit.get("sort") {
                            search_after = Some(sort.clone());
                        }
                    }
                }

                // Send the batch to the next worker in round-robin fashion
                let batch = RetrievalMessage::Batch(Arc::new(next_response_json));
                if let Err(e) = worker_txs[next_worker].send(batch).await {
                    return Err(anyhow!(
                        "Failed to send batch to worker {} for slice {}: {}",
                        next_worker,
                        slice_id,
                        e
                    ));
                }

                // Move to the next worker
                next_worker = (next_worker + 1) % worker_txs.len();
            }

            // Clean up search context at the end
            match search_type {
                SearchType::Scroll => {
                    debug!(
                        "Slice {}: Cleaning up scroll context with ID {}",
                        slice_id, id
                    );
                    let clear_scroll_body = json!({
                        "scroll_id": [id]
                    });

                    let clear_response = client
                        .clear_scroll(ClearScrollParts::None)
                        .body(clear_scroll_body)
                        .send()
                        .await;

                    if let Err(e) = clear_response {
                        warn!("Slice {}: Failed to clear scroll context: {}", slice_id, e);
                    } else {
                        debug!("Slice {}: Successfully cleared scroll context", slice_id);
                    }
                }
                SearchType::PointInTime => {
                    let close_pit_body = json!({
                        "id": id
                    });

                    let close_response = client
                        .close_point_in_time()
                        .body(close_pit_body)
                        .send()
                        .await;

                    if let Err(e) = close_response {
                        warn!("Failed to close PIT for slice {}: {}", slice_id, e);
                    }
                }
            }

            info!(
                "Slice {} completed, retrieved {} documents",
                slice_id, retrieved_hits
            );
            Ok(slice_total_hits)
        });

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
            let pb = progress_bar.clone();

            tokio::spawn(async move {
                while let Some(message) = rx.recv().await {
                    match message {
                        RetrievalMessage::Batch(batch) => {
                            // Move CPU-bound work to blocking thread pool
                            let processed =
                                tokio::task::spawn_blocking(move || process_batch(&batch))
                                    .await??;

                            // Update counters
                            let doc_count = processed.doc_count;
                            let bytes_count = processed.buffer.len() as u64;

                            processed_count.fetch_add(doc_count, Ordering::Relaxed);
                            processed_bytes.fetch_add(bytes_count, Ordering::Relaxed);

                            // Update progress bar
                            if let Some(pb) = &pb {
                                let current = processed_count.load(Ordering::Relaxed);
                                pb.set_position(current);

                                let bytes = processed_bytes.load(Ordering::Relaxed);
                                let elapsed_secs = start_time.elapsed().as_secs_f64();
                                let bytes_per_sec = bytes as f64 / elapsed_secs;
                                pb.set_message(format!(
                                    "{} @ {}/s",
                                    ByteSize(bytes),
                                    ByteSize(bytes_per_sec as u64)
                                ));
                            }

                            // Send to output channel
                            if let Err(e) = processed_tx.send(processed).await {
                                return Err(anyhow!(
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
                return Err(anyhow!("Failed to write batch buffer: {}", e));
            }
        }

        // single flush after all batches are written
        if let Err(e) = writer.flush().await {
            return Err(anyhow!("Failed to flush writer: {}", e));
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
                    return Err(anyhow!("Retrieval task failed: {}", e));
                }
            },
            Err(e) => {
                return Err(anyhow!("Retrieval task panicked: {}", e));
            }
        }
    }

    // Final update of progress bar total
    if let Some(pb) = &progress_bar {
        debug!(
            "Setting final progress bar length to total hits: {}",
            total_hits
        );
        pb.set_length(total_hits);
        info!("Total documents to process: {}", total_hits);
    }

    // Signal that we're done retrieving by sending Done to all workers
    for (i, tx) in worker_txs.iter().enumerate() {
        if let Err(e) = tx.send(RetrievalMessage::Done).await {
            warn!("Failed to send Done signal to worker {}: {}", i, e);
        }
    }

    // Wait for all workers to finish
    for (i, task) in worker_tasks.into_iter().enumerate() {
        if let Err(e) = task.await {
            return Err(anyhow!("Worker {} task panicked: {}", i, e));
        }
    }

    // Wait for output to finish
    match output_task.await {
        Ok(result) => {
            if let Err(e) = result {
                return Err(anyhow!("Output task failed: {}", e));
            }
        }
        Err(e) => {
            return Err(anyhow!("Output task panicked: {}", e));
        }
    }

    if let Some(pb) = progress_bar {
        let count = processed_count.load(Ordering::Relaxed);
        let bytes = processed_bytes.load(Ordering::Relaxed);
        pb.finish_with_message(format!("Dumped {} documents ({})", count, ByteSize(bytes)));
    }

    let elapsed = start_time.elapsed();
    let count = processed_count.load(Ordering::Relaxed);
    let bytes = processed_bytes.load(Ordering::Relaxed);

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

fn process_batch(response: &Value) -> Result<ProcessedBatch> {
    let hits = match response["hits"]["hits"].as_array() {
        Some(hits) => hits,
        None => {
            // No hits array, return empty batch
            return Ok(ProcessedBatch {
                buffer: Vec::new(),
                doc_count: 0,
            });
        }
    };

    if hits.is_empty() {
        // Hits array is empty, return empty batch
        return Ok(ProcessedBatch {
            buffer: Vec::new(),
            doc_count: 0,
        });
    }

    // Estimate buffer size: (average doc size + newline) * num docs
    // This is a rough guess, Vec will reallocate if needed.
    // Assuming average doc size ~ 1KB for initial allocation.
    let estimated_size = hits.len() * (1024 + 1);
    let mut buffer = Vec::with_capacity(estimated_size);

    for hit in hits {
        // Serialize the entire hit document
        serde_json::to_writer(&mut buffer, hit)?;
        // Append a newline character
        buffer.push(b'\n');
    }

    let doc_count = hits.len() as u64; // Use hits.len() for doc count

    // Shrink buffer potentially? Optional optimization if memory is tight.
    // buffer.shrink_to_fit();

    Ok(ProcessedBatch {
        buffer, // The buffer now contains all serialized docs + newlines
        doc_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_batch() {
        // Create test data
        let test_response = json!({
            "hits": {
                "hits": [
                    {
                        "_id": "doc1",
                        "_index": "test_index",
                        "_type": "_doc",
                        "_score": 1.0,
                        "_source": {
                            "id": 1,
                            "name": "Document 1"
                        }
                    },
                    {
                        "_id": "doc2",
                        "_index": "test_index",
                        "_type": "_doc",
                        "_score": 1.0,
                        "_source": {
                            "id": 2,
                            "name": "Document 2"
                        }
                    }
                ]
            }
        });

        // Process the batch
        let processed = process_batch(&test_response).unwrap();

        // Verify the count
        assert_eq!(processed.doc_count, 2, "Should have processed 2 documents");

        // Verify the buffer content
        assert!(!processed.buffer.is_empty(), "Buffer should not be empty");

        // Split the buffer by newline and validate each line as JSON
        let lines: Vec<_> = processed
            .buffer
            .split(|&c| c == b'\n')
            .filter(|line| !line.is_empty())
            .collect();
        assert_eq!(lines.len(), 2, "Should have 2 lines in the buffer");

        // Convert lines (Vec<u8>) to strings for comparison
        let line1_str = std::str::from_utf8(lines[0]).expect("Line 1 should be valid UTF-8");
        let line2_str = std::str::from_utf8(lines[1]).expect("Line 2 should be valid UTF-8");

        // Parse back to Value to compare
        let parsed1: Value = serde_json::from_str(line1_str).expect("Line 1 should be valid JSON");
        let parsed2: Value = serde_json::from_str(line2_str).expect("Line 2 should be valid JSON");

        // Verify the full document structure
        assert_eq!(parsed1["_id"], "doc1", "First document ID mismatch");
        assert_eq!(
            parsed1["_index"], "test_index",
            "First document index mismatch"
        );
        assert_eq!(
            parsed1["_source"]["id"], 1,
            "First document source id mismatch"
        );
        assert_eq!(
            parsed1["_source"]["name"], "Document 1",
            "First document source name mismatch"
        );

        assert_eq!(parsed2["_id"], "doc2", "Second document ID mismatch");
        assert_eq!(
            parsed2["_index"], "test_index",
            "Second document index mismatch"
        );
        assert_eq!(
            parsed2["_source"]["id"], 2,
            "Second document source id mismatch"
        );
        assert_eq!(
            parsed2["_source"]["name"], "Document 2",
            "Second document source name mismatch"
        );
    }

    #[test]
    fn test_sliced_scroll_config() {
        // Create basic search body
        let mut search_body = json!({
            "query": {"match_all": {}},
            "size": 100
        })
        .as_object()
        .unwrap()
        .clone();

        // Configure for slice 1 of 4
        let slice_id = 1;
        let num_slices = 4;

        // Add slice configuration
        search_body.insert(
            "slice".to_string(),
            json!({
                "id": slice_id,
                "max": num_slices
            }),
        );

        // Verify slice configuration is correctly added
        assert!(
            search_body.contains_key("slice"),
            "Should contain slice key"
        );
        assert_eq!(
            search_body["slice"]["id"], slice_id,
            "Slice ID should match"
        );
        assert_eq!(
            search_body["slice"]["max"], num_slices,
            "Num slices should match"
        );

        // Make sure original query is preserved
        assert!(
            search_body.contains_key("query"),
            "Query should be preserved"
        );
        assert!(
            search_body["query"].is_object(),
            "Query should be an object"
        );
        assert!(
            search_body["query"]["match_all"].is_object(),
            "match_all should be present"
        );

        // Verify size is preserved
        assert_eq!(search_body["size"], 100, "Size should be preserved");
    }

    #[test]
    fn test_pit_search_config() {
        // Create basic search body
        let mut search_body = json!({
            "query": {"match_all": {}},
            "size": 100
        })
        .as_object()
        .unwrap()
        .clone();

        // Add PIT configuration
        let pit_id = "test_pit_id";
        let keep_alive = "1m";

        search_body.insert(
            "pit".to_string(),
            json!({
                "id": pit_id,
                "keep_alive": keep_alive
            }),
        );

        // Verify PIT configuration is correctly added
        assert!(search_body.contains_key("pit"), "Should contain pit key");
        assert_eq!(search_body["pit"]["id"], pit_id, "PIT ID should match");
        assert_eq!(
            search_body["pit"]["keep_alive"], keep_alive,
            "keep_alive should match"
        );

        // Add search_after for pagination
        let search_after = json!([12345, "test_sort_value"]);
        search_body.insert("search_after".to_string(), search_after.clone());

        // Verify search_after is properly added
        assert!(
            search_body.contains_key("search_after"),
            "Should contain search_after key"
        );
        assert_eq!(
            search_body["search_after"], search_after,
            "search_after should match"
        );

        // Make sure original query is preserved
        assert!(
            search_body.contains_key("query"),
            "Query should be preserved"
        );
        assert!(
            search_body["query"].is_object(),
            "Query should be an object"
        );
        assert!(
            search_body["query"]["match_all"].is_object(),
            "match_all should be present"
        );

        // Verify size is preserved
        assert_eq!(search_body["size"], 100, "Size should be preserved");
    }
}
