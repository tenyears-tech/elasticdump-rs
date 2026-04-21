use anyhow::{Result, anyhow};
use elasticsearch::SearchParts;
use log::{debug, info};
use sonic_rs::{JsonValueMutTrait, Value, json};

use crate::cli::SearchType;

use super::{
    context::RetrievalContext,
    extract::{self, BatchMetadata},
    pit::SharedPitCoordinator,
    retrieval_task::{
        TotalHitsEstimate, abort_shared_pit, dispatch_response_batch, ensure_pit_sort,
        read_checked_response_bytes, record_total_hits,
    },
    slice_state::SliceState,
};

pub(crate) fn build_pit_search_body(
    base_body: &Value,
    pit_id: &str,
    pit_keep_alive: &str,
    search_after: Option<&[u8]>,
) -> Result<Value> {
    let mut body = base_body.clone();
    let object = body.as_object_mut().expect("PIT body must be an object");
    object.insert(
        &"pit",
        json!({ "id": pit_id, "keep_alive": pit_keep_alive }),
    );
    if let Some(search_after) = search_after {
        let search_after_value: Value = sonic_rs::from_slice(search_after)?;
        object.insert(&"search_after", search_after_value);
    }
    Ok(body)
}

pub(crate) fn update_pit_search_body(
    body: &mut Value,
    pit_id: &str,
    pit_keep_alive: &str,
    search_after: Option<&[u8]>,
) -> Result<()> {
    let object = body.as_object_mut().expect("PIT body must be an object");
    object.insert(
        &"pit",
        json!({ "id": pit_id, "keep_alive": pit_keep_alive }),
    );

    match search_after {
        Some(search_after) => {
            let search_after_value: Value = sonic_rs::from_slice(search_after)?;
            object.insert(&"search_after", search_after_value);
        }
        None => {
            object.remove(&"search_after");
        }
    }

    Ok(())
}

pub(crate) fn apply_pit_batch_metadata(state: &mut SliceState, metadata: &BatchMetadata) {
    state.current_id = metadata.next_pit_id.clone();
    state.update_search_after_from_raw(metadata.last_sort_raw.as_deref());
}

pub(crate) async fn run_pit_slice(
    ctx: &RetrievalContext,
    state: &mut SliceState,
    pit_keep_alive: &str,
    shared_pit: Option<SharedPitCoordinator>,
) -> Result<TotalHitsEstimate> {
    let shared_pit = shared_pit.expect("PIT mode requires shared coordinator");
    ensure_pit_sort(&mut state.search_body);

    let lease = shared_pit.acquire().await;
    state.pit_generation = Some(lease.generation);

    let mut request_body =
        build_pit_search_body(&state.search_body, &lease.id, pit_keep_alive, None)?;
    debug!(
        "Search body: {}",
        sonic_rs::to_string(&request_body).unwrap_or_default()
    );

    let response = match ctx
        .client
        .search(SearchParts::None)
        .body(&request_body)
        .send()
        .await
    {
        Ok(response) => {
            debug!(
                "Slice {}: Initial search request successful",
                state.slice_id
            );
            response
        }
        Err(error) => {
            let search_error = anyhow!(
                "Slice {}: Failed to initiate search: {} - this might indicate connection issues or invalid credentials",
                state.slice_id,
                error
            );
            abort_shared_pit(&Some(shared_pit.clone()), &search_error).await;
            return Err(search_error);
        }
    };

    let response_bytes =
        match read_checked_response_bytes(response, state.slice_id, "initial search").await {
            Ok(bytes) => bytes,
            Err(error) => {
                abort_shared_pit(&Some(shared_pit.clone()), &error).await;
                return Err(error);
            }
        };
    let initial_bytes = response_bytes.len() as u64;
    debug!(
        "Slice {}: Read {} bytes for initial response",
        state.slice_id, initial_bytes
    );

    let metadata = match extract::extract_batch_metadata(&response_bytes, &SearchType::PointInTime)
    {
        Ok(metadata) => metadata,
        Err(error) => {
            abort_shared_pit(&Some(shared_pit.clone()), &error).await;
            return Err(error);
        }
    };

    let slice_total_hits = record_total_hits(ctx, metadata.total_hits);
    debug!(
        "Slice {}: Total hits estimate: {} (exact: {})",
        state.slice_id, slice_total_hits.value, slice_total_hits.is_exact
    );

    apply_pit_batch_metadata(state, &metadata);
    shared_pit
        .observe_returned_id(state.current_id.as_deref())
        .await;

    let initial_hits_are_empty = match dispatch_response_batch(
        ctx,
        state,
        response_bytes,
        metadata.doc_count,
        metadata.hits_are_empty,
        initial_bytes,
    )
    .await
    {
        Ok(hits_are_empty) => hits_are_empty,
        Err(error) => {
            abort_shared_pit(&Some(shared_pit.clone()), &anyhow!(error.to_string())).await;
            return Err(error);
        }
    };

    if let Err(error) = shared_pit
        .complete_round(
            state.pit_generation.expect("PIT generation should be set"),
            state.current_id.clone(),
            initial_hits_are_empty,
        )
        .await
    {
        abort_shared_pit(&Some(shared_pit.clone()), &error).await;
        return Err(error);
    }

    if initial_hits_are_empty {
        info!(
            "Search finished for slice {}, no more documents.",
            state.slice_id
        );
        info!(
            "Slice {} completed, retrieved {} documents",
            state.slice_id, state.retrieved_hits
        );
        return Ok(slice_total_hits);
    }

    let mut current_request_id = match shared_pit
        .wait_for_generation(state.pit_generation.expect("PIT generation should be set") + 1)
        .await
    {
        Ok(next_lease) => {
            state.pit_generation = Some(next_lease.generation);
            state.current_id = Some(next_lease.id.clone());
            next_lease.id
        }
        Err(error) => return Err(error),
    };

    loop {
        debug!("Slice {}: Fetching next batch", state.slice_id);
        if let Err(error) = update_pit_search_body(
            &mut request_body,
            &current_request_id,
            pit_keep_alive,
            state.search_after.as_deref(),
        ) {
            abort_shared_pit(&Some(shared_pit.clone()), &error).await;
            return Err(error);
        }
        debug!(
            "Next PIT body: {}",
            sonic_rs::to_string(&request_body).unwrap_or_default()
        );

        let next_response = match ctx
            .client
            .search(SearchParts::None)
            .body(&request_body)
            .send()
            .await
        {
            Ok(response) => {
                debug!("Slice {}: Next batch request successful", state.slice_id);
                response
            }
            Err(error) => {
                let search_error = anyhow!(
                    "Slice {}: Search continuation error: {}",
                    state.slice_id,
                    error
                );
                abort_shared_pit(&Some(shared_pit.clone()), &search_error).await;
                return Err(search_error);
            }
        };

        let next_response_bytes =
            match read_checked_response_bytes(next_response, state.slice_id, "continuation search")
                .await
            {
                Ok(bytes) => bytes,
                Err(error) => {
                    abort_shared_pit(&Some(shared_pit.clone()), &error).await;
                    return Err(error);
                }
            };

        let batch_bytes = next_response_bytes.len() as u64;
        debug!(
            "Slice {}: Read {} bytes for continuation response",
            state.slice_id, batch_bytes
        );

        let metadata =
            match extract::extract_batch_metadata(&next_response_bytes, &SearchType::PointInTime) {
                Ok(metadata) => metadata,
                Err(error) => {
                    abort_shared_pit(&Some(shared_pit.clone()), &error).await;
                    return Err(error);
                }
            };

        debug!(
            "Slice {}: Continuation metadata: pit_id={:?}, doc_count={}, hits_are_empty={}",
            state.slice_id, metadata.next_pit_id, metadata.doc_count, metadata.hits_are_empty
        );

        apply_pit_batch_metadata(state, &metadata);
        shared_pit
            .observe_returned_id(state.current_id.as_deref())
            .await;

        let hits_are_empty = match dispatch_response_batch(
            ctx,
            state,
            next_response_bytes,
            metadata.doc_count,
            metadata.hits_are_empty,
            batch_bytes,
        )
        .await
        {
            Ok(hits_are_empty) => hits_are_empty,
            Err(error) => {
                abort_shared_pit(&Some(shared_pit.clone()), &anyhow!(error.to_string())).await;
                return Err(error);
            }
        };

        if let Err(error) = shared_pit
            .complete_round(
                state.pit_generation.expect("PIT generation should be set"),
                state.current_id.clone(),
                hits_are_empty,
            )
            .await
        {
            abort_shared_pit(&Some(shared_pit.clone()), &error).await;
            return Err(error);
        }

        if hits_are_empty {
            info!(
                "Search finished for slice {}, no more documents.",
                state.slice_id
            );
            break;
        }

        match shared_pit
            .wait_for_generation(state.pit_generation.expect("PIT generation should be set") + 1)
            .await
        {
            Ok(next_lease) => {
                state.pit_generation = Some(next_lease.generation);
                current_request_id = next_lease.id;
                state.current_id = Some(current_request_id.clone());
            }
            Err(error) => return Err(error),
        }
    }

    info!(
        "Slice {} completed, retrieved {} documents",
        state.slice_id, state.retrieved_hits
    );

    Ok(slice_total_hits)
}

#[cfg(test)]
mod tests {
    use super::{apply_pit_batch_metadata, build_pit_search_body, update_pit_search_body};
    use crate::retrieval::extract::BatchMetadata;
    use crate::retrieval::retrieval_task::TotalHitsEstimate;
    use crate::retrieval::slice_state::SliceState;
    use sonic_rs::json;

    #[test]
    fn build_pit_search_body_includes_pit_sort_and_search_after() {
        let body = build_pit_search_body(
            &json!({
                "query": { "match_all": {} },
                "sort": ["_shard_doc"],
                "size": 100
            }),
            "pit-123",
            "1m",
            Some(br#"["_shard_doc",77]"#),
        )
        .unwrap();

        assert_eq!(
            body,
            json!({
                "query": { "match_all": {} },
                "sort": ["_shard_doc"],
                "size": 100,
                "pit": { "id": "pit-123", "keep_alive": "1m" },
                "search_after": ["_shard_doc", 77]
            })
        );
    }

    #[test]
    fn build_pit_search_body_embeds_raw_search_after_json() {
        let body = build_pit_search_body(
            &json!({
                "query": { "match_all": {} },
                "sort": ["_shard_doc"],
                "size": 100
            }),
            "pit-123",
            "1m",
            Some(br#"[2,{"nested":true}]"#),
        )
        .unwrap();

        assert_eq!(
            body,
            json!({
                "query": { "match_all": {} },
                "sort": ["_shard_doc"],
                "size": 100,
                "pit": { "id": "pit-123", "keep_alive": "1m" },
                "search_after": [2, {"nested": true}]
            })
        );
    }

    #[test]
    fn update_pit_search_body_replaces_search_after_without_rebuilding_query_fields() {
        let mut body = build_pit_search_body(
            &json!({
                "query": { "match_all": {} },
                "sort": ["_shard_doc"],
                "size": 100
            }),
            "pit-123",
            "1m",
            None,
        )
        .unwrap();

        update_pit_search_body(&mut body, "pit-456", "2m", Some(br#"[7,{"nested":true}]"#))
            .unwrap();

        assert_eq!(
            body,
            json!({
                "query": { "match_all": {} },
                "sort": ["_shard_doc"],
                "size": 100,
                "pit": { "id": "pit-456", "keep_alive": "2m" },
                "search_after": [7, {"nested": true}]
            })
        );
    }

    #[test]
    fn update_pit_search_body_clears_stale_search_after() {
        let mut body = build_pit_search_body(
            &json!({
                "query": { "match_all": {} },
                "sort": ["_shard_doc"],
                "size": 100
            }),
            "pit-123",
            "1m",
            Some(br#"["_shard_doc",77]"#),
        )
        .unwrap();

        update_pit_search_body(&mut body, "pit-789", "1m", None).unwrap();

        assert_eq!(
            body,
            json!({
                "query": { "match_all": {} },
                "sort": ["_shard_doc"],
                "size": 100,
                "pit": { "id": "pit-789", "keep_alive": "1m" }
            })
        );
    }

    #[test]
    fn apply_pit_batch_metadata_updates_state_from_raw_metadata() {
        let mut state = SliceState::new(0, 0, json!({"size": 10}));
        state.current_id = Some("pit-old".to_string());
        state.update_search_after_from_raw(Some(br#"[1,"a"]"#));

        let metadata = BatchMetadata {
            total_hits: TotalHitsEstimate {
                value: 2,
                is_exact: true,
            },
            next_scroll_id: None,
            next_pit_id: Some("pit-new".to_string()),
            last_sort_raw: Some(br#"[2,{"nested":true}]"#.to_vec()),
            doc_count: 2,
            hits_are_empty: false,
        };

        apply_pit_batch_metadata(&mut state, &metadata);

        assert_eq!(state.current_id.as_deref(), Some("pit-new"));
        assert_eq!(
            state.search_after.as_deref(),
            Some(br#"[2,{"nested":true}]"#.as_slice())
        );
    }
}
