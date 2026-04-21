use anyhow::{Result, anyhow};
use elasticsearch::{ScrollParts, SearchParts};
use log::{debug, info};
use sonic_rs::{JsonContainerTrait, Value, json};

use crate::cli::SearchType;

use super::{
    context::RetrievalContext,
    retrieval_task::{
        TotalHitsEstimate, cleanup_search_context, dispatch_response_batch,
        extract_total_hits_estimate, latest_scroll_id, read_checked_response_bytes,
        record_total_hits, refresh_search_id,
    },
    slice_state::SliceState,
};

pub(crate) fn build_scroll_request_body(scroll_ttl: &str, scroll_id: &str) -> Value {
    json!({
        "scroll": scroll_ttl,
        "scroll_id": scroll_id,
    })
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

    let search_response: Value = sonic_rs::from_slice(&response_bytes).map_err(|error| {
        anyhow!(
            "Slice {}: Failed to parse initial response from bytes: {} - this might indicate malformed JSON or unexpected response format",
            state.slice_id,
            error
        )
    })?;
    debug!(
        "Slice {}: Successfully parsed initial response from bytes",
        state.slice_id
    );

    state.current_id = latest_scroll_id(&search_response);
    if let Some(id) = &state.current_id {
        debug!("Slice {}: Got scroll_id: {}", state.slice_id, id);
    } else {
        debug!("Slice {}: No scroll_id found in response", state.slice_id);
    }

    let slice_total_hits = record_total_hits(ctx, extract_total_hits_estimate(&search_response));
    debug!(
        "Slice {}: Total hits estimate: {} (exact: {})",
        state.slice_id, slice_total_hits.value, slice_total_hits.is_exact
    );
    let hits = search_response["hits"]["hits"].as_array();
    let doc_count = hits.map_or(0, |items| items.len() as u64);
    let hits_are_empty = hits.is_none_or(|items| items.is_empty());

    let mut done = match dispatch_response_batch(
        ctx,
        state,
        response_bytes,
        doc_count,
        hits_are_empty,
        initial_bytes,
    )
    .await
    {
        Ok(hits_are_empty) => hits_are_empty,
        Err(error) => {
            if let Some(scroll_id) = state.current_id.as_deref() {
                cleanup_search_context(&ctx.client, &SearchType::Scroll, scroll_id, state.slice_id)
                    .await;
            }
            return Err(error);
        }
    };

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

        let next_json: Value = match sonic_rs::from_slice(&next_response_bytes) {
            Ok(json) => json,
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
                return Err(anyhow!(
                    "Slice {}: Failed to parse continuation response from bytes: {}",
                    state.slice_id,
                    error
                ));
            }
        };

        debug!(
            "Slice {}: Next response JSON: {}",
            state.slice_id,
            sonic_rs::to_string(&next_json).unwrap_or_default()
        );

        if let Some(current_id) = state.current_id.as_mut() {
            refresh_search_id(&SearchType::Scroll, &next_json, current_id);
        }
        let hits = next_json["hits"]["hits"].as_array();
        let doc_count = hits.map_or(0, |items| items.len() as u64);
        let hits_are_empty = hits.is_none_or(|items| items.is_empty());

        done = match dispatch_response_batch(
            ctx,
            state,
            next_response_bytes,
            doc_count,
            hits_are_empty,
            batch_bytes,
        )
        .await
        {
            Ok(hits_are_empty) => hits_are_empty,
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
    use super::build_scroll_request_body;
    use sonic_rs::json;

    #[test]
    fn build_scroll_request_body_includes_scroll_id_and_ttl() {
        assert_eq!(
            build_scroll_request_body("5m", "scroll-123"),
            json!({ "scroll": "5m", "scroll_id": "scroll-123" })
        );
    }
}
