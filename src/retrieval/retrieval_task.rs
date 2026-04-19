use anyhow::{Result, anyhow};
use bytesize::ByteSize;
use elasticsearch::{
    ClearScrollParts, Elasticsearch, ScrollParts, SearchParts, http::response::Response,
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

use super::{messages::RetrievalMessage, pit::SharedPitCoordinator};
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

pub(crate) async fn read_checked_response_bytes(
    response: Response,
    slice_id: usize,
    operation: &str,
) -> Result<Vec<u8>> {
    let status = response.status_code();
    let response_bytes = response.bytes().await.map_err(|e| {
        anyhow!(
            "Slice {}: Failed to read {} response bytes: {}",
            slice_id,
            operation,
            e
        )
    })?;
    let response_bytes = response_bytes.to_vec();

    if !status.is_success() {
        let response_body = String::from_utf8_lossy(&response_bytes);
        return Err(anyhow!(
            "Slice {}: Elasticsearch {} failed with HTTP {}: {}",
            slice_id,
            operation,
            status.as_u16(),
            response_body
        ));
    }

    Ok(response_bytes)
}

fn ensure_pit_sort(search_body: &mut Value) {
    let body = search_body
        .as_object_mut()
        .expect("PIT search body must be an object");
    if !body.contains_key(&"sort") {
        body.insert(&"sort", json!(["_shard_doc"]));
    }
}

async fn abort_shared_pit(shared_pit: &Option<SharedPitCoordinator>, error: &anyhow::Error) {
    if let Some(shared_pit) = shared_pit {
        shared_pit.abort(error.to_string()).await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TotalHitsEstimate {
    pub(crate) value: u64,
    pub(crate) is_exact: bool,
}

fn extract_total_hits_estimate(response: &Value) -> TotalHitsEstimate {
    let value = response["hits"]["total"]["value"].as_u64().unwrap_or_else(|| {
        response["hits"]["hits"]
            .as_array()
            .map_or(0, |hits| hits.len() as u64)
    });
    let relation = response["hits"]["total"]["relation"].as_str().unwrap_or("eq");

    TotalHitsEstimate {
        value,
        is_exact: relation == "eq",
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
        SearchType::PointInTime => {}
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
    shared_pit: Option<SharedPitCoordinator>,
    use_sliced_scroll: bool,
    num_slices: usize,
    total_hits_count: Arc<AtomicU64>,
    input_bar: Option<ProgressBar>,
    output_bar: Option<ProgressBar>,
    retrieved_count: Arc<AtomicU64>,
    retrieved_bytes: Arc<AtomicU64>,
    start_time: Instant,
) -> tokio::task::JoinHandle<Result<TotalHitsEstimate>> {
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
        let mut pit_generation = None;
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
                let lease = shared_pit
                    .as_ref()
                    .expect("PIT mode requires shared coordinator")
                    .acquire()
                    .await;
                pit_generation = Some(lease.generation);

                let mut initial_pit_body = json!(search_body_obj.clone());
                ensure_pit_sort(&mut initial_pit_body);
                initial_pit_body.as_object_mut().unwrap().insert(
                    &"pit",
                    json!({
                        "id": lease.id,
                        "keep_alive": pit_keep_alive
                    }),
                );

                debug!(
                    "Search body: {}",
                    sonic_rs::to_string(&initial_pit_body).unwrap_or_default()
                );

                // Execute search with PIT ID
                client
                    .search(SearchParts::None)
                    .body(&initial_pit_body)
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
                let error = anyhow!(
                    "Slice {}: Failed to initiate search: {} - this might indicate connection issues or invalid credentials",
                    slice_id,
                    e
                );
                abort_shared_pit(&shared_pit, &error).await;
                return Err(error);
            }
        };

        // Read bytes first to get accurate size
        let response_bytes =
            match read_checked_response_bytes(response, slice_id, "initial search").await {
                Ok(bytes) => bytes,
                Err(e) => {
                    abort_shared_pit(&shared_pit, &e).await;
                    return Err(e);
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
                let error = anyhow!(
                    "Slice {}: Failed to parse initial response from bytes: {} - this might indicate malformed JSON or unexpected response format",
                    slice_id,
                    e
                );
                abort_shared_pit(&shared_pit, &error).await;
                return Err(error);
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
                let latest_id = latest_pit_id(&search_response);
                (latest_id, true)
            }
        };

        // Get total hits for this slice
        let slice_total_hits = extract_total_hits_estimate(&search_response);

        debug!(
            "Slice {}: Total hits estimate: {} (exact: {})",
            slice_id, slice_total_hits.value, slice_total_hits.is_exact
        );

        // Update the shared total hits counter
        let previous_total = total_hits_count.fetch_add(slice_total_hits.value, Ordering::Relaxed);
        let new_total = previous_total + slice_total_hits.value;

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
        if is_pit {
            shared_pit
                .as_ref()
                .expect("PIT mode requires shared coordinator")
                .observe_returned_id(id_opt.as_deref())
                .await;
        }
        if let Err(e) = worker_txs[next_worker]
            .send(RetrievalMessage::Batch(response_data.clone()))
            .await
        {
            abort_shared_pit(&shared_pit, &anyhow!(e.to_string())).await;
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

        let mut id = match (search_type.clone(), id_opt) {
            (SearchType::Scroll, Some(id)) => id,
            (SearchType::Scroll, None) => {
                return Err(anyhow!(
                    "No ID found for slice {} to continue search",
                    slice_id
                ));
            }
            (SearchType::PointInTime, Some(id)) => id,
            (SearchType::PointInTime, None) => {
                let lease = shared_pit
                    .as_ref()
                    .expect("PIT mode requires shared coordinator")
                    .acquire()
                    .await;
                pit_generation = Some(lease.generation);
                lease.id
            }
        };

        if is_pit {
            let shared_pit = shared_pit
                .as_ref()
                .expect("PIT mode requires shared coordinator");
            let hits_are_empty = initial_hits == 0;
            if let Err(e) = shared_pit
                .complete_round(
                    pit_generation.expect("PIT generation should be set"),
                    latest_pit_id(response_data.as_ref()),
                    hits_are_empty,
                )
                .await
            {
                abort_shared_pit(&Some(shared_pit.clone()), &e).await;
                return Err(e);
            }

            if hits_are_empty {
                info!("Search finished for slice {}, no more documents.", slice_id);
                info!(
                    "Slice {} completed, retrieved {} documents",
                    slice_id, retrieved_hits
                );
                return Ok(slice_total_hits);
            }

            match shared_pit
                .wait_for_generation(pit_generation.expect("PIT generation should be set") + 1)
                .await
            {
                Ok(next_lease) => {
                    pit_generation = Some(next_lease.generation);
                    id = next_lease.id;
                }
                Err(e) => return Err(e),
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
                    let mut next_pit_body = json!(search_body_obj.clone());
                    ensure_pit_sort(&mut next_pit_body);
                    next_pit_body.as_object_mut().unwrap().insert(
                        &"pit",
                        json!({
                            "id": id,
                            "keep_alive": pit_keep_alive
                        }),
                    );

                    // Add search_after from the last response if available
                    if let Some(sort_values) = &search_after {
                        debug!("Slice {}: Using search_after from last result", slice_id);
                        next_pit_body
                            .as_object_mut()
                            .unwrap()
                            .insert(&"search_after", sort_values.clone());
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
                    let error = anyhow!("Slice {}: Search continuation error: {}", slice_id, e);
                    abort_shared_pit(&shared_pit, &error).await;
                    cleanup_search_context(&client, &search_type, &id, slice_id).await;
                    return Err(error);
                }
            };

            // Read bytes first to get accurate size
            let next_response_bytes =
                match read_checked_response_bytes(next_response, slice_id, "continuation search")
                    .await
                {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        abort_shared_pit(&shared_pit, &e).await;
                        cleanup_search_context(&client, &search_type, &id, slice_id).await;
                        return Err(e);
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
                    let error = anyhow!(
                        "Slice {}: Failed to parse continuation response from bytes: {}",
                        slice_id,
                        e
                    );
                    abort_shared_pit(&shared_pit, &error).await;
                    cleanup_search_context(&client, &search_type, &id, slice_id).await;
                    return Err(error);
                }
            };

            debug!(
                "Slice {}: Next response JSON: {}",
                slice_id,
                sonic_rs::to_string(&next_response_json).unwrap_or_default()
            );

            if !is_pit {
                // Refresh the search identifier before any early exit so cleanup uses the latest value.
                refresh_search_id(&search_type, &next_response_json, &mut id);
            }

            // Check if we have any hits
            let hits = next_response_json["hits"]["hits"].as_array();
            let hits_are_empty = hits.map_or(true, |h| h.is_empty());

            // Update search_after with sort values from the last hit for next pagination
            if is_pit && !hits_are_empty {
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

            let returned_pit_id = if is_pit {
                latest_pit_id(&next_response_json)
            } else {
                None
            };
            if is_pit {
                shared_pit
                    .as_ref()
                    .expect("PIT mode requires shared coordinator")
                    .observe_returned_id(returned_pit_id.as_deref())
                    .await;
            }

            // Send the batch to the next worker in round-robin fashion
            let batch = RetrievalMessage::Batch(Arc::new(next_response_json));
            if let Err(e) = worker_txs[next_worker].send(batch).await {
                abort_shared_pit(&shared_pit, &anyhow!(e.to_string())).await;
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

            if is_pit {
                let shared_pit = shared_pit
                    .as_ref()
                    .expect("PIT mode requires shared coordinator");
                if let Err(e) = shared_pit
                    .complete_round(
                        pit_generation.expect("PIT generation should be set"),
                        returned_pit_id,
                        hits_are_empty,
                    )
                    .await
                {
                    abort_shared_pit(&Some(shared_pit.clone()), &e).await;
                    return Err(e);
                }

                if hits_are_empty {
                    info!("Search finished for slice {}, no more documents.", slice_id);
                    break;
                }

                match shared_pit
                    .wait_for_generation(pit_generation.expect("PIT generation should be set") + 1)
                    .await
                {
                    Ok(next_lease) => {
                        pit_generation = Some(next_lease.generation);
                        id = next_lease.id;
                    }
                    Err(e) => return Err(e),
                }
            } else if hits_are_empty {
                info!("Search finished for slice {}, no more documents.", slice_id);
                break;
            }
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

    #[test]
    fn ensure_pit_sort_sets_shard_doc_when_missing() {
        let mut body = json!({
            "query": { "match_all": {} },
            "pit": { "id": "pit-id", "keep_alive": "1m" }
        });

        super::ensure_pit_sort(&mut body);

        assert_eq!(body["sort"], json!(["_shard_doc"]));
    }

    #[test]
    fn ensure_pit_sort_preserves_user_supplied_sort() {
        let mut body = json!({
            "query": { "match_all": {} },
            "pit": { "id": "pit-id", "keep_alive": "1m" },
            "sort": [{ "created_at": "asc" }]
        });

        super::ensure_pit_sort(&mut body);

        assert_eq!(body["sort"], json!([{ "created_at": "asc" }]));
    }

    #[test]
    fn extract_total_hits_estimate_marks_gte_as_inexact() {
        let response = json!({
            "hits": {
                "total": { "value": 10000, "relation": "gte" },
                "hits": []
            }
        });

        let estimate = super::extract_total_hits_estimate(&response);
        assert_eq!(
            estimate,
            super::TotalHitsEstimate {
                value: 10000,
                is_exact: false,
            }
        );
    }

    #[test]
    fn extract_total_hits_estimate_marks_eq_as_exact() {
        let response = json!({
            "hits": {
                "total": { "value": 42, "relation": "eq" },
                "hits": []
            }
        });

        let estimate = super::extract_total_hits_estimate(&response);
        assert_eq!(
            estimate,
            super::TotalHitsEstimate {
                value: 42,
                is_exact: true,
            }
        );
    }
}
