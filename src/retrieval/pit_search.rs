use anyhow::{Result, anyhow};
use elasticsearch::SearchParts;
use log::{debug, info};
use sonic_rs::{JsonContainerTrait, JsonValueMutTrait, Value, json};
use std::sync::Arc;

use super::{
    context::RetrievalContext,
    pit::SharedPitCoordinator,
    retrieval_task::{
        TotalHitsEstimate, abort_shared_pit, dispatch_response_batch, ensure_pit_sort,
        extract_total_hits_estimate, latest_pit_id, read_checked_response_bytes, record_total_hits,
    },
    slice_state::SliceState,
};

pub(crate) fn build_pit_search_body(
    base_body: &Value,
    pit_id: &str,
    pit_keep_alive: &str,
    search_after: Option<&Value>,
) -> Value {
    let mut body = base_body.clone();
    let object = body.as_object_mut().expect("PIT body must be an object");
    object.insert(
        &"pit",
        json!({ "id": pit_id, "keep_alive": pit_keep_alive }),
    );
    if let Some(search_after) = search_after {
        object.insert(&"search_after", search_after.clone());
    }
    body
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

    let initial_body = build_pit_search_body(&state.search_body, &lease.id, pit_keep_alive, None);
    debug!(
        "Search body: {}",
        sonic_rs::to_string(&initial_body).unwrap_or_default()
    );

    let response = match ctx
        .client
        .search(SearchParts::None)
        .body(&initial_body)
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

    let search_response: Value = match sonic_rs::from_slice(&response_bytes) {
        Ok(json) => {
            debug!(
                "Slice {}: Successfully parsed initial response from bytes",
                state.slice_id
            );
            json
        }
        Err(error) => {
            let parse_error = anyhow!(
                "Slice {}: Failed to parse initial response from bytes: {} - this might indicate malformed JSON or unexpected response format",
                state.slice_id,
                error
            );
            abort_shared_pit(&Some(shared_pit.clone()), &parse_error).await;
            return Err(parse_error);
        }
    };

    let slice_total_hits = record_total_hits(ctx, extract_total_hits_estimate(&search_response));
    debug!(
        "Slice {}: Total hits estimate: {} (exact: {})",
        state.slice_id, slice_total_hits.value, slice_total_hits.is_exact
    );

    let response_data = Arc::new(search_response);
    state.current_id = latest_pit_id(response_data.as_ref());
    state.update_search_after_from_hits(
        response_data["hits"]["hits"]
            .as_array()
            .map(|items| &items[..]),
    );
    shared_pit
        .observe_returned_id(state.current_id.as_deref())
        .await;

    let initial_hits_are_empty =
        match dispatch_response_batch(ctx, state, response_data.clone(), initial_bytes).await {
            Ok(hits_are_empty) => hits_are_empty,
            Err(error) => {
                abort_shared_pit(&Some(shared_pit.clone()), &anyhow!(error.to_string())).await;
                return Err(error);
            }
        };

    if let Err(error) = shared_pit
        .complete_round(
            state.pit_generation.expect("PIT generation should be set"),
            latest_pit_id(response_data.as_ref()),
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
        let next_body = build_pit_search_body(
            &state.search_body,
            &current_request_id,
            pit_keep_alive,
            state.search_after.as_ref(),
        );
        debug!(
            "Next PIT body: {}",
            sonic_rs::to_string(&next_body).unwrap_or_default()
        );

        let next_response = match ctx
            .client
            .search(SearchParts::None)
            .body(&next_body)
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

        let next_json: Value = match sonic_rs::from_slice(&next_response_bytes) {
            Ok(json) => json,
            Err(error) => {
                let parse_error = anyhow!(
                    "Slice {}: Failed to parse continuation response from bytes: {}",
                    state.slice_id,
                    error
                );
                abort_shared_pit(&Some(shared_pit.clone()), &parse_error).await;
                return Err(parse_error);
            }
        };

        debug!(
            "Slice {}: Next response JSON: {}",
            state.slice_id,
            sonic_rs::to_string(&next_json).unwrap_or_default()
        );

        state.current_id = latest_pit_id(&next_json);
        state.update_search_after_from_hits(
            next_json["hits"]["hits"].as_array().map(|items| &items[..]),
        );
        shared_pit
            .observe_returned_id(state.current_id.as_deref())
            .await;

        let hits_are_empty =
            match dispatch_response_batch(ctx, state, Arc::new(next_json), batch_bytes).await {
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
    use super::build_pit_search_body;
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
            Some(&json!(["_shard_doc", 77])),
        );

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
}
