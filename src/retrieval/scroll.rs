use anyhow::{Result, anyhow};
use elasticsearch::{ScrollParts, SearchParts};
use log::{debug, info};
use sonic_rs::{Value, json};

use crate::cli::SearchType;

use super::{
    context::RetrievalContext,
    extract::BatchMetadata,
    retrieval_task::{
        TotalHitsEstimate, cleanup_search_context, process_response_batch,
        read_checked_response_bytes, record_total_hits,
    },
    slice_state::SliceState,
};

pub(crate) fn build_scroll_request_body(scroll_ttl: &str, scroll_id: &str) -> Value {
    json!({
        "scroll": scroll_ttl,
        "scroll_id": scroll_id,
    })
}

pub(crate) fn apply_scroll_batch_metadata(state: &mut SliceState, metadata: &BatchMetadata) {
    // Keep the previous scroll id when a response lacks one: overwriting it
    // with None would make both continuation and cleanup impossible.
    if let Some(next_scroll_id) = &metadata.next_scroll_id {
        state.current_id = Some(next_scroll_id.clone());
    }
}

pub(crate) async fn run_scroll_slice(
    ctx: &RetrievalContext,
    state: &mut SliceState,
    scroll_ttl: &str,
) -> Result<TotalHitsEstimate> {
    debug!(
        "Slice {}: Initiating scroll search with size {}",
        state.slice_id, state.search_body["size"]
    );

    let response = match ctx
        .client
        .search(SearchParts::Index(&[ctx.index.as_ref()]))
        .scroll(scroll_ttl)
        .allow_partial_search_results(false)
        .body(&state.search_body)
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
            return Err(anyhow!(
                "Slice {}: Failed to initiate search: {} - this might indicate connection issues or invalid credentials",
                state.slice_id,
                error
            ));
        }
    };

    let response_bytes =
        read_checked_response_bytes(response, state.slice_id, "initial search").await?;
    let initial_bytes = response_bytes.len() as u64;
    debug!(
        "Slice {}: Read {} bytes for initial response",
        state.slice_id, initial_bytes
    );

    let metadata = match process_response_batch(
        ctx,
        state,
        response_bytes,
        &SearchType::Scroll,
        initial_bytes,
    )
    .await
    {
        Ok(metadata) => metadata,
        Err(error) => {
            if let Some(metadata) = error.metadata() {
                apply_scroll_batch_metadata(state, metadata);
            }
            if let Some(scroll_id) = state.current_id.as_deref() {
                cleanup_search_context(&ctx.client, &SearchType::Scroll, scroll_id, state.slice_id)
                    .await;
            }
            return Err(error.into_error());
        }
    };
    apply_scroll_batch_metadata(state, &metadata);
    if let Some(id) = &state.current_id {
        debug!("Slice {}: Got scroll_id: {}", state.slice_id, id);
    } else {
        debug!("Slice {}: No scroll_id found in response", state.slice_id);
    }

    let slice_total_hits = record_total_hits(ctx, metadata.total_hits);
    debug!(
        "Slice {}: Total hits estimate: {} (exact: {})",
        state.slice_id, slice_total_hits.value, slice_total_hits.is_exact
    );

    let mut done = metadata.hits_are_empty;

    while !done {
        let next_response = match ctx
            .client
            .scroll(ScrollParts::None)
            .body(build_scroll_request_body(
                scroll_ttl,
                state.current_id.as_deref().ok_or_else(|| {
                    anyhow!(
                        "No ID found for slice {} to continue search",
                        state.slice_id
                    )
                })?,
            ))
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
                if let Some(scroll_id) = state.current_id.as_deref() {
                    cleanup_search_context(
                        &ctx.client,
                        &SearchType::Scroll,
                        scroll_id,
                        state.slice_id,
                    )
                    .await;
                }
                return Err(search_error);
            }
        };

        let next_response_bytes =
            match read_checked_response_bytes(next_response, state.slice_id, "continuation search")
                .await
            {
                Ok(bytes) => bytes,
                Err(error) => {
                    if let Some(scroll_id) = state.current_id.as_deref() {
                        cleanup_search_context(
                            &ctx.client,
                            &SearchType::Scroll,
                            scroll_id,
                            state.slice_id,
                        )
                        .await;
                    }
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
            &SearchType::Scroll,
            batch_bytes,
        )
        .await
        {
            Ok(metadata) => metadata,
            Err(error) => {
                if let Some(metadata) = error.metadata() {
                    apply_scroll_batch_metadata(state, metadata);
                }
                if let Some(scroll_id) = state.current_id.as_deref() {
                    cleanup_search_context(
                        &ctx.client,
                        &SearchType::Scroll,
                        scroll_id,
                        state.slice_id,
                    )
                    .await;
                }
                return Err(error.into_error());
            }
        };
        apply_scroll_batch_metadata(state, &metadata);

        done = metadata.hits_are_empty;

        if done {
            info!(
                "Search finished for slice {}, no more documents.",
                state.slice_id
            );
        }
    }

    if let Some(scroll_id) = state.current_id.as_deref() {
        cleanup_search_context(&ctx.client, &SearchType::Scroll, scroll_id, state.slice_id).await;
    }

    info!(
        "Slice {} completed, retrieved {} documents",
        state.slice_id, state.retrieved_hits
    );

    Ok(slice_total_hits)
}

#[cfg(test)]
mod tests {
    use super::super::{
        extract::BatchMetadata, retrieval_task::TotalHitsEstimate, slice_state::SliceState,
    };
    use super::build_scroll_request_body;
    use sonic_rs::json;

    #[test]
    fn build_scroll_request_body_includes_scroll_id_and_ttl() {
        assert_eq!(
            build_scroll_request_body("5m", "scroll-123"),
            json!({ "scroll": "5m", "scroll_id": "scroll-123" })
        );
    }

    #[test]
    fn apply_scroll_batch_metadata_updates_current_id() {
        let mut state = SliceState::new(0, 0, json!({"size": 10}));
        state.current_id = Some("scroll-old".to_string());

        let metadata = BatchMetadata {
            total_hits: TotalHitsEstimate {
                value: 2,
                is_exact: true,
            },
            next_scroll_id: Some("scroll-new".to_string()),
            next_pit_id: None,
            last_sort_raw: None,
            doc_count: 2,
            hits_are_empty: false,
        };

        super::apply_scroll_batch_metadata(&mut state, &metadata);

        assert_eq!(state.current_id.as_deref(), Some("scroll-new"));
    }

    #[test]
    fn apply_scroll_batch_metadata_keeps_previous_id_when_response_lacks_one() {
        let mut state = SliceState::new(0, 0, json!({"size": 10}));
        state.current_id = Some("scroll-live".into());

        let metadata = BatchMetadata {
            total_hits: TotalHitsEstimate {
                value: 2,
                is_exact: true,
            },
            next_scroll_id: None,
            next_pit_id: None,
            last_sort_raw: None,
            doc_count: 2,
            hits_are_empty: false,
        };

        super::apply_scroll_batch_metadata(&mut state, &metadata);

        assert_eq!(state.current_id.as_deref(), Some("scroll-live"));
    }
}
