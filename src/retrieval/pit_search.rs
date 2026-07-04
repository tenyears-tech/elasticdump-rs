use anyhow::Result;
use elasticsearch::SearchParts;
use log::{debug, info};
use sonic_rs::{JsonValueMutTrait, JsonValueTrait, Value, json};

use crate::cli::SearchType;

use super::{
    context::RetrievalContext,
    extract::BatchMetadata,
    pit::{PitAbortGuard, SharedPitCoordinator},
    retrieval_task::{
        DumpCancelled, RetryMode, TotalHitsEstimate, abort_shared_pit, ensure_pit_sort,
        process_response_batch, record_total_hits, send_checked_with_retry,
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

/// Requested page size of the slice's search body, used to detect an
/// exhausted slice early. Falls back to 0 ("size unknown, never finish
/// early") if the size is missing or not an integer, so an unexpected body
/// shape degrades to empty-round termination instead of truncating the dump.
fn pit_batch_size(search_body: &Value) -> u64 {
    search_body["size"].as_u64().unwrap_or(0)
}

/// Under `search_after` pagination a page shorter than the requested size
/// proves the slice is exhausted, so the guaranteed trailing empty round can
/// be skipped.
fn pit_slice_finished(metadata: &BatchMetadata, batch_size: u64) -> bool {
    metadata.hits_are_empty || metadata.doc_count < batch_size
}

pub(crate) async fn run_pit_slice(
    ctx: &RetrievalContext,
    state: &mut SliceState,
    pit_keep_alive: &str,
    shared_pit: Option<SharedPitCoordinator>,
) -> Result<TotalHitsEstimate> {
    let shared_pit = shared_pit.expect("PIT mode requires shared coordinator");
    // If this slice unwinds without defusing (panic or error return), the
    // guard aborts the coordinator so siblings parked in wait_for_generation
    // are released instead of waiting forever.
    let abort_guard = PitAbortGuard::new(shared_pit.clone());
    ensure_pit_sort(&mut state.search_body);
    let batch_size = pit_batch_size(&state.search_body);

    if ctx.cancel.is_cancelled() {
        return Err(anyhow::Error::new(DumpCancelled));
    }

    let lease = shared_pit.acquire();
    state.pit_generation = Some(lease.generation);

    let mut request_body =
        build_pit_search_body(&state.search_body, &lease.id, pit_keep_alive, None)?;
    debug!(
        "Search body: {}",
        sonic_rs::to_string(&request_body).unwrap_or_default()
    );

    // PIT search_after requests are idempotent (the cursor lives in the
    // request, not on the server), so full retries are safe.
    let response_bytes = match send_checked_with_retry(
        ctx.retry,
        &ctx.cancel,
        RetryMode::Idempotent,
        state.slice_id,
        "initial search",
        || {
            ctx.client
                .search(SearchParts::None)
                .allow_partial_search_results(false)
                .body(&request_body)
                .send()
        },
    )
    .await
    {
        Ok(bytes) => bytes,
        Err(error) => {
            abort_shared_pit(&Some(shared_pit.clone()), &error);
            return Err(error);
        }
    };
    let initial_bytes = response_bytes.len() as u64;
    debug!(
        "Slice {}: Read {} bytes for initial response",
        state.slice_id, initial_bytes
    );

    let metadata = match process_response_batch(
        ctx,
        state,
        response_bytes,
        &SearchType::PointInTime,
        initial_bytes,
    )
    .await
    {
        Ok(metadata) => metadata,
        Err(error) => {
            if let Some(metadata) = error.metadata() {
                apply_pit_batch_metadata(state, metadata);
                shared_pit.observe_returned_id(state.current_id.as_deref());
            }
            let error = error.into_error();
            abort_shared_pit(&Some(shared_pit.clone()), &error);
            return Err(error);
        }
    };

    let slice_total_hits = record_total_hits(ctx, metadata.total_hits);
    debug!(
        "Slice {}: Total hits estimate: {} (exact: {})",
        state.slice_id, slice_total_hits.value, slice_total_hits.is_exact
    );

    apply_pit_batch_metadata(state, &metadata);
    shared_pit.observe_returned_id(state.current_id.as_deref());

    let initial_slice_finished = pit_slice_finished(&metadata, batch_size);

    if let Err(error) = shared_pit.complete_round(
        state.pit_generation.expect("PIT generation should be set"),
        state.current_id.clone(),
        initial_slice_finished,
    ) {
        abort_shared_pit(&Some(shared_pit.clone()), &error);
        return Err(error);
    }

    if initial_slice_finished {
        info!(
            "Search finished for slice {}, no more documents.",
            state.slice_id
        );
        info!(
            "Slice {} completed, retrieved {} documents",
            state.slice_id, state.retrieved_hits
        );
        abort_guard.defuse();
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
        if ctx.cancel.is_cancelled() {
            return Err(anyhow::Error::new(DumpCancelled));
        }

        debug!("Slice {}: Fetching next batch", state.slice_id);
        if let Err(error) = update_pit_search_body(
            &mut request_body,
            &current_request_id,
            pit_keep_alive,
            state.search_after.as_deref(),
        ) {
            abort_shared_pit(&Some(shared_pit.clone()), &error);
            return Err(error);
        }
        debug!(
            "Next PIT body: {}",
            sonic_rs::to_string(&request_body).unwrap_or_default()
        );

        let next_response_bytes = match send_checked_with_retry(
            ctx.retry,
            &ctx.cancel,
            RetryMode::Idempotent,
            state.slice_id,
            "continuation search",
            || {
                ctx.client
                    .search(SearchParts::None)
                    .allow_partial_search_results(false)
                    .body(&request_body)
                    .send()
            },
        )
        .await
        {
            Ok(bytes) => bytes,
            Err(error) => {
                abort_shared_pit(&Some(shared_pit.clone()), &error);
                return Err(error);
            }
        };

        let batch_bytes = next_response_bytes.len() as u64;
        debug!(
            "Slice {}: Read {} bytes for continuation response",
            state.slice_id, batch_bytes
        );

        let metadata = match process_response_batch(
            ctx,
            state,
            next_response_bytes,
            &SearchType::PointInTime,
            batch_bytes,
        )
        .await
        {
            Ok(metadata) => metadata,
            Err(error) => {
                if let Some(metadata) = error.metadata() {
                    apply_pit_batch_metadata(state, metadata);
                    shared_pit.observe_returned_id(state.current_id.as_deref());
                }
                let error = error.into_error();
                abort_shared_pit(&Some(shared_pit.clone()), &error);
                return Err(error);
            }
        };

        debug!(
            "Slice {}: Continuation metadata: pit_id={:?}, doc_count={}, hits_are_empty={}",
            state.slice_id, metadata.next_pit_id, metadata.doc_count, metadata.hits_are_empty
        );

        apply_pit_batch_metadata(state, &metadata);
        shared_pit.observe_returned_id(state.current_id.as_deref());

        let slice_finished = pit_slice_finished(&metadata, batch_size);

        if let Err(error) = shared_pit.complete_round(
            state.pit_generation.expect("PIT generation should be set"),
            state.current_id.clone(),
            slice_finished,
        ) {
            abort_shared_pit(&Some(shared_pit.clone()), &error);
            return Err(error);
        }

        if slice_finished {
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

    abort_guard.defuse();
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

    fn metadata_with_docs(doc_count: u64, hits_are_empty: bool) -> BatchMetadata {
        BatchMetadata {
            total_hits: TotalHitsEstimate {
                value: doc_count,
                is_exact: true,
            },
            next_scroll_id: None,
            next_pit_id: Some("pit-x".to_string()),
            last_sort_raw: None,
            doc_count,
            hits_are_empty,
        }
    }

    #[test]
    fn pit_slice_finished_on_short_page_or_empty_hits_but_not_full_page() {
        assert!(!super::pit_slice_finished(
            &metadata_with_docs(10, false),
            10
        ));
        assert!(super::pit_slice_finished(&metadata_with_docs(3, false), 10));
        assert!(super::pit_slice_finished(&metadata_with_docs(0, true), 10));
    }

    #[test]
    fn pit_batch_size_reads_size_and_disables_early_finish_when_missing() {
        assert_eq!(super::pit_batch_size(&json!({"size": 500})), 500);

        // Missing or malformed size must never finish a slice early; the
        // slice then falls back to empty-round termination.
        let unknown = super::pit_batch_size(&json!({"query": {"match_all": {}}}));
        assert!(!super::pit_slice_finished(
            &metadata_with_docs(10, false),
            unknown
        ));
    }
}
