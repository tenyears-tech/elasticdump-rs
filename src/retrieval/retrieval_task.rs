use anyhow::{Result, anyhow};
use bytesize::ByteSize;
use elasticsearch::{
    ClearScrollParts, Elasticsearch, OpenPointInTimeParts, ScrollParts, SearchParts,
};
use indicatif::ProgressBar;
use log::{debug, info, warn};
use sonic_rs::{JsonContainerTrait, JsonValueMutTrait, JsonValueTrait, Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::sync::mpsc::Sender;

use super::messages::RetrievalMessage;
use crate::cli::SearchType;

fn latest_scroll_id(response: &Value) -> Option<String> {
    response.get("_scroll_id").as_str().map(|id| id.to_string())
}

fn latest_pit_id(response: &Value) -> Option<String> {
    response.get("pit_id").as_str().map(|id| id.to_string())
}

fn refresh_search_id(search_type: &SearchType, response: &Value, id: &mut String) {
    let latest_id = match search_type {
        SearchType::Scroll => latest_scroll_id(response),
        SearchType::PointInTime => latest_pit_id(response),
    };

    if let Some(new_id) = latest_id {
        *id = new_id;
    }
}

async fn cleanup_search_context(
    client: &Elasticsearch,
    search_type: &SearchType,
    id: &str,
    slice_id: usize,
) {
    match search_type {
        SearchType::Scroll => {
            let clear_scroll_body = json!({ "scroll_id": [id] });
            if let Err(e) = client
                .clear_scroll(ClearScrollParts::None)
                .body(clear_scroll_body)
                .send()
                .await
            {
                warn!("Slice {}: Failed to clear scroll context: {}", slice_id, e);
            }
        }
        SearchType::PointInTime => {
            if let Err(e) = client
                .close_point_in_time()
                .body(json!({ "id": id }))
                .send()
                .await
            {
                warn!("Failed to close PIT for slice {}: {}", slice_id, e);
            }
        }
    }
}

/// Spawn a retrieval task for a specific slice
pub fn spawn_retrieval_task(
    slice_id: usize,
    client: Elasticsearch,
    index: String,
    worker_txs: Vec<Sender<RetrievalMessage>>,
    mut search_body: Value,
    search_type: SearchType,
    scroll_ttl: String,
    pit_keep_alive: String,
    use_sliced_scroll: bool,
    num_slices: usize,
    total_hits_count: Arc<AtomicU64>,
    input_bar: Option<ProgressBar>,
    output_bar: Option<ProgressBar>,
    retrieved_count: Arc<AtomicU64>,
    retrieved_bytes: Arc<AtomicU64>,
    start_time: Instant,
) -> tokio::task::JoinHandle<Result<u64>> {
    // Setup for specific slice
    let mut search_body_obj = search_body.as_object_mut().unwrap().clone();
    if use_sliced_scroll {
        search_body_obj.insert(
            &"slice",
            json!({
                "id": slice_id,
                "max": num_slices
            }),
        );
        info!("Starting slice {}/{}", slice_id + 1, num_slices);
    }

    tokio::spawn(async move {
        debug!("Slice {}: Starting retrieval task", slice_id);
        // Initial search setup varies based on search type
        let response_result = match search_type {
            SearchType::Scroll => {
                debug!(
                    "Slice {}: Initiating scroll search with size {}",
                    slice_id, search_body_obj["size"]
                );
                // Use Scroll API
                client
                    .search(SearchParts::Index(&[&index]))
                    .scroll(&scroll_ttl)
                    .body(&search_body_obj)
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
                        let pit_json: serde_json::Value = match resp.json().await {
                            Ok(json) => json,
                            Err(e) => {
                                return Err(anyhow!(
                                    "Slice {}: Failed to parse PIT open response: {}",
                                    slice_id,
                                    e
                                ));
                            }
                        };

                        match pit_json.get("id").and_then(serde_json::Value::as_str) {
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
                search_body_obj.insert(
                    &"pit",
                    json!({
                        "id": pit_id,
                        "keep_alive": pit_keep_alive
                    }),
                );

                // Add default sort to make search_after work reliably
                if !search_body_obj.contains_key(&"sort") {
                    search_body_obj.insert(&"sort", json!(["_id"]));
                }
                debug!(
                    "Search body: {}",
                    sonic_rs::to_string(&search_body_obj).unwrap_or_default()
                );

                // Execute search with PIT ID
                client
                    .search(SearchParts::None)
                    .body(&search_body_obj)
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

        // Read bytes first to get accurate size
        let response_bytes = match response.bytes().await {
            Ok(bytes) => bytes,
            Err(e) => {
                return Err(anyhow!(
                    "Slice {}: Failed to read initial response bytes: {}",
                    slice_id,
                    e
                ));
            }
        };

        let initial_bytes = response_bytes.len() as u64;
        debug!(
            "Slice {}: Read {} bytes for initial response",
            slice_id, initial_bytes
        );

        // Now parse the JSON from bytes
        let search_response: Value = match sonic_rs::from_slice(&response_bytes) {
            Ok(json) => {
                debug!(
                    "Slice {}: Successfully parsed initial response from bytes",
                    slice_id
                );
                json
            }
            Err(e) => {
                return Err(anyhow!(
                    "Slice {}: Failed to parse initial response from bytes: {} - this might indicate malformed JSON or unexpected response format",
                    slice_id,
                    e
                ));
            }
        };

        // Extract ID for continued searches (scroll_id or pit.id)
        let (id_opt, is_pit) = match search_type {
            SearchType::Scroll => {
                let scroll_id = latest_scroll_id(&search_response);

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
                let latest_id = latest_pit_id(&search_response).or_else(|| {
                    search_body_obj
                        .get(&"pit")
                        .and_then(|pit| pit.get(&"id"))
                        .as_str()
                        .map(|id| id.to_string())
                });
                (latest_id, true)
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
        if let (Some(ib), Some(ob)) = (&input_bar, &output_bar) {
            ib.set_length(new_total);
            ob.set_length(new_total);
            debug!("Updated progress bar lengths to: {}", new_total);
        }

        // Process the initial batch regardless
        let response_data = Arc::new(search_response);
        let initial_hits = response_data["hits"]["hits"]
            .as_array()
            .map_or(0, |h| h.len()) as u64;

        // Increment retrieved count and update input bar for initial batch
        let current_retrieved =
            retrieved_count.fetch_add(initial_hits, Ordering::Relaxed) + initial_hits;
        let current_bytes_retrieved =
            retrieved_bytes.fetch_add(initial_bytes, Ordering::Relaxed) + initial_bytes;
        if let Some(ib) = &input_bar {
            ib.set_position(current_retrieved);
            let elapsed_secs = start_time.elapsed().as_secs_f64().max(1e-6);
            let bytes_per_sec = current_bytes_retrieved as f64 / elapsed_secs;
            ib.set_message(format!(
                "{} @ {} /s",
                ByteSize(current_bytes_retrieved),
                ByteSize(bytes_per_sec as u64)
            ));
        }

        let next_worker = slice_id % worker_txs.len();
        if let Err(e) = worker_txs[next_worker]
            .send(RetrievalMessage::Batch(response_data.clone()))
            .await
        {
            if let Some(id) = &id_opt {
                cleanup_search_context(&client, &search_type, id, slice_id).await;
            }
            return Err(anyhow!(
                "Failed to send initial batch for slice {}: {}",
                slice_id,
                e
            ));
        }

        let mut next_worker = (next_worker + 1) % worker_txs.len();

        // If no ID, return early
        let mut id = match id_opt {
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
                        "scroll": scroll_ttl,
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
                    let mut next_pit_body = search_body_obj.clone();

                    // Update the PIT ID in the search body
                    if next_pit_body.contains_key(&"pit") {
                        if let Some(pit_obj) = next_pit_body["pit"].as_object_mut() {
                            pit_obj.insert(&"id", json!(id));
                            debug!("Slice {}: Updated PIT ID in search body", slice_id);
                        }
                    } else {
                        // If pit object doesn't exist, create it
                        next_pit_body.insert(
                            &"pit",
                            json!({
                                "id": id,
                                "keep_alive": pit_keep_alive
                            }),
                        );
                        debug!("Slice {}: Added PIT ID to search body", slice_id);
                    }

                    // Make sure we have a sort parameter (required for search_after)
                    if !next_pit_body.contains_key(&"sort") {
                        debug!(
                            "Slice {}: Adding default sort by _id for PIT search",
                            slice_id
                        );
                        next_pit_body.insert(&"sort", json!(["_id"]));
                    }

                    // Add search_after from the last response if available
                    if let Some(sort_values) = &search_after {
                        debug!("Slice {}: Using search_after from last result", slice_id);
                        next_pit_body.insert(&"search_after", sort_values.clone());
                    } else {
                        debug!("Slice {}: No search_after values available", slice_id);
                    }
                    debug!(
                        "Next PIT body: {}",
                        sonic_rs::to_string(&next_pit_body).unwrap_or_default()
                    );

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
                    cleanup_search_context(&client, &search_type, &id, slice_id).await;
                    return Err(anyhow!(
                        "Slice {}: Search continuation error: {}",
                        slice_id,
                        e
                    ));
                }
            };

            // Read bytes first to get accurate size
            let next_response_bytes = match next_response.bytes().await {
                Ok(bytes) => bytes,
                Err(e) => {
                    cleanup_search_context(&client, &search_type, &id, slice_id).await;
                    return Err(anyhow!(
                        "Slice {}: Failed to read continuation response bytes: {}",
                        slice_id,
                        e
                    ));
                }
            };

            let batch_bytes = next_response_bytes.len() as u64;
            debug!(
                "Slice {}: Read {} bytes for continuation response",
                slice_id, batch_bytes
            );

            // Now parse the JSON from bytes
            let next_response_json: Value = match sonic_rs::from_slice(&next_response_bytes) {
                Ok(json) => json,
                Err(e) => {
                    cleanup_search_context(&client, &search_type, &id, slice_id).await;
                    return Err(anyhow!(
                        "Slice {}: Failed to parse continuation response from bytes: {}",
                        slice_id,
                        e
                    ));
                }
            };

            debug!(
                "Slice {}: Next response JSON: {}",
                slice_id,
                sonic_rs::to_string(&next_response_json).unwrap_or_default()
            );

            // Refresh the search identifier before any early exit so cleanup uses the latest value.
            refresh_search_id(&search_type, &next_response_json, &mut id);

            // Check if we have any hits
            let hits = next_response_json["hits"]["hits"].as_array();
            if hits.map_or(true, |h| h.is_empty()) {
                info!("Search finished for slice {}, no more documents.", slice_id);
                break; // No more hits
            }

            // Update search_after with sort values from the last hit for next pagination
            if is_pit {
                if let Some(hits_array) = hits {
                    if let Some(last_hit) = hits_array.last() {
                        debug!(
                            "Slice {}: Updating search_after with new values from last hit",
                            slice_id
                        );
                        search_after = Some(last_hit.get("sort").cloned().unwrap_or_default());
                    }
                }
            }

            // Count hits
            let mut batch_size = 0;
            if let Some(hits_array) = hits {
                batch_size = hits_array.len();
                retrieved_hits += batch_size as u64;
                debug!(
                    "Slice {}: Retrieved batch with {} documents (total: {})",
                    slice_id, batch_size, retrieved_hits
                );
            }

            // Increment retrieved count and update input bar for subsequent batches
            let current_retrieved =
                retrieved_count.fetch_add(batch_size as u64, Ordering::Relaxed) + batch_size as u64;
            let current_bytes_retrieved =
                retrieved_bytes.fetch_add(batch_bytes, Ordering::Relaxed) + batch_bytes;
            if let Some(ib) = &input_bar {
                ib.set_position(current_retrieved);
                let elapsed_secs = start_time.elapsed().as_secs_f64().max(1e-6);
                let bytes_per_sec = current_bytes_retrieved as f64 / elapsed_secs;
                ib.set_message(format!(
                    "{} @ {} /s",
                    ByteSize(current_bytes_retrieved),
                    ByteSize(bytes_per_sec as u64)
                ));
            }

            // Send the batch to the next worker in round-robin fashion
            let batch = RetrievalMessage::Batch(Arc::new(next_response_json));
            if let Err(e) = worker_txs[next_worker].send(batch).await {
                cleanup_search_context(&client, &search_type, &id, slice_id).await;
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

        cleanup_search_context(&client, &search_type, &id, slice_id).await;

        info!(
            "Slice {} completed, retrieved {} documents",
            slice_id, retrieved_hits
        );
        Ok(slice_total_hits)
    })
}

#[cfg(test)]
mod tests {
    use sonic_rs::json;

    #[test]
    fn latest_pit_id_prefers_search_response_id() {
        let response = json!({
            "pit_id": "pit-from-search",
            "hits": {
                "hits": []
            }
        });

        assert_eq!(
            super::latest_pit_id(&response).as_deref(),
            Some("pit-from-search")
        );
    }

    #[test]
    fn latest_scroll_id_reads_continuation_response_id() {
        let response = json!({
            "_scroll_id": "scroll-from-response",
            "hits": {
                "hits": []
            }
        });

        assert_eq!(
            super::latest_scroll_id(&response).as_deref(),
            Some("scroll-from-response")
        );
    }
}
