use anyhow::{Result, anyhow};
use bytes::Bytes;
use bytesize::ByteSize;
use elasticsearch::{ClearScrollParts, Elasticsearch, http::response::Response};
use http::StatusCode;
use log::warn;
use sonic_rs::{JsonContainerTrait, JsonValueMutTrait, JsonValueTrait, Value, json};
use std::sync::atomic::Ordering;

use super::{
    context::RetrievalContext, messages::RetrievalMessage, pit::SharedPitCoordinator, pit_search,
    scroll, slice_state::SliceState,
};
use crate::cli::SearchType;

pub(crate) fn latest_scroll_id(response: &Value) -> Option<String> {
    response.get("_scroll_id").as_str().map(|id| id.to_string())
}

pub(crate) fn latest_pit_id(response: &Value) -> Option<String> {
    response.get("pit_id").as_str().map(|id| id.to_string())
}

pub(crate) fn refresh_search_id(search_type: &SearchType, response: &Value, id: &mut String) {
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
) -> Result<Bytes> {
    let status = response.status_code();
    let response_bytes = response.bytes().await.map_err(|e| {
        anyhow!(
            "Slice {}: Failed to read {} response bytes: {}",
            slice_id,
            operation,
            e
        )
    })?;

    validate_response_bytes(status, response_bytes, slice_id, operation)
}

pub(crate) fn validate_response_bytes(
    status: StatusCode,
    response_bytes: Bytes,
    slice_id: usize,
    operation: &str,
) -> Result<Bytes> {
    if !status.is_success() {
        let response_body = String::from_utf8_lossy(response_bytes.as_ref());
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

pub(crate) fn ensure_pit_sort(search_body: &mut Value) {
    let body = search_body
        .as_object_mut()
        .expect("PIT search body must be an object");
    if !body.contains_key(&"sort") {
        body.insert(&"sort", json!(["_shard_doc"]));
    }
}

pub(crate) async fn abort_shared_pit(
    shared_pit: &Option<SharedPitCoordinator>,
    error: &anyhow::Error,
) {
    if let Some(shared_pit) = shared_pit {
        shared_pit.abort(error.to_string()).await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TotalHitsEstimate {
    pub(crate) value: u64,
    pub(crate) is_exact: bool,
}

pub(crate) fn extract_total_hits_estimate(response: &Value) -> TotalHitsEstimate {
    let value = response["hits"]["total"]["value"]
        .as_u64()
        .unwrap_or_else(|| {
            response["hits"]["hits"]
                .as_array()
                .map_or(0, |hits| hits.len() as u64)
        });
    let relation = response["hits"]["total"]["relation"]
        .as_str()
        .unwrap_or("eq");

    TotalHitsEstimate {
        value,
        is_exact: relation == "eq",
    }
}

pub(crate) fn record_total_hits(
    ctx: &RetrievalContext,
    estimate: TotalHitsEstimate,
) -> TotalHitsEstimate {
    let previous_total = ctx
        .total_hits_count
        .fetch_add(estimate.value, Ordering::Relaxed);
    let new_total = previous_total + estimate.value;

    if let (Some(input_bar), Some(output_bar)) = (&ctx.input_bar, &ctx.output_bar) {
        input_bar.set_length(new_total);
        output_bar.set_length(new_total);
    }

    estimate
}

pub(crate) async fn dispatch_response_batch(
    ctx: &RetrievalContext,
    state: &mut SliceState,
    response_bytes: Bytes,
    doc_count: u64,
    hits_are_empty: bool,
    batch_bytes: u64,
) -> Result<bool> {
    state.retrieved_hits += doc_count;
    let current_retrieved = ctx.retrieved_count.fetch_add(doc_count, Ordering::Relaxed) + doc_count;
    let current_bytes = ctx
        .retrieved_bytes
        .fetch_add(batch_bytes, Ordering::Relaxed)
        + batch_bytes;

    if let Some(input_bar) = &ctx.input_bar {
        input_bar.set_position(current_retrieved);
        let elapsed_secs = ctx.start_time.elapsed().as_secs_f64().max(1e-6);
        input_bar.set_message(format!(
            "{} @ {} /s",
            ByteSize(current_bytes),
            ByteSize((current_bytes as f64 / elapsed_secs) as u64)
        ));
    }

    if doc_count == 0 {
        return Ok(hits_are_empty);
    }

    let worker = state.dispatch_worker(ctx.worker_txs.len());
    ctx.worker_txs[worker]
        .send(RetrievalMessage::Batch(response_bytes))
        .await
        .map_err(|error| {
            anyhow!(
                "Failed to send batch to worker {} for slice {}: {}",
                worker,
                state.slice_id,
                error
            )
        })?;

    Ok(hits_are_empty)
}

pub(crate) async fn cleanup_search_context(
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
    ctx: RetrievalContext,
    mut state: SliceState,
    search_type: SearchType,
    scroll_ttl: String,
    pit_keep_alive: String,
    shared_pit: Option<SharedPitCoordinator>,
) -> tokio::task::JoinHandle<Result<TotalHitsEstimate>> {
    tokio::spawn(async move {
        match search_type {
            SearchType::Scroll => scroll::run_scroll_slice(&ctx, &mut state, &scroll_ttl).await,
            SearchType::PointInTime => {
                pit_search::run_pit_slice(&ctx, &mut state, &pit_keep_alive, shared_pit).await
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use http::StatusCode;
    use sonic_rs::json;
    use std::{
        sync::{Arc, atomic::AtomicU64},
        time::Instant,
    };
    use tokio::sync::mpsc;
    use url::Url;

    use super::super::{
        context::RetrievalContext, messages::RetrievalMessage, slice_state::SliceState,
    };

    fn test_context(worker_txs: Vec<mpsc::Sender<RetrievalMessage>>) -> RetrievalContext {
        RetrievalContext {
            client: crate::elasticsearch::create_client(
                Url::parse("http://localhost:9200/").unwrap(),
                None,
                None,
                false,
            )
            .unwrap(),
            index: Arc::<str>::from("test-index"),
            worker_txs,
            total_hits_count: Arc::new(AtomicU64::new(0)),
            input_bar: None,
            output_bar: None,
            retrieved_count: Arc::new(AtomicU64::new(0)),
            retrieved_bytes: Arc::new(AtomicU64::new(0)),
            start_time: Instant::now(),
        }
    }

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

    #[test]
    fn validate_response_bytes_preserves_success_buffer() {
        let body = Bytes::from_static(br#"{"hits":{"hits":[]}}"#);
        let original_ptr = body.as_ptr();

        let returned =
            super::validate_response_bytes(StatusCode::OK, body, 0, "initial search").unwrap();

        assert_eq!(returned.as_ptr(), original_ptr);
        assert_eq!(returned, Bytes::from_static(br#"{"hits":{"hits":[]}}"#));
    }

    #[test]
    fn validate_response_bytes_reports_http_error_body() {
        let error = super::validate_response_bytes(
            StatusCode::NOT_FOUND,
            Bytes::from_static(br#"{"error":"missing"}"#),
            3,
            "continuation search",
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("Slice 3"));
        assert!(error.contains("HTTP 404"));
        assert!(error.contains(r#"{"error":"missing"}"#));
    }

    #[tokio::test]
    async fn dispatch_response_batch_skips_empty_batches() {
        let (worker_tx, mut worker_rx) = mpsc::channel(1);
        let ctx = test_context(vec![worker_tx]);
        let mut state = SliceState::new(0, 0, json!({"size": 10}));
        let response_bytes = Bytes::from_static(br#"{"hits":{"hits":[]}}"#);

        let hits_are_empty =
            super::dispatch_response_batch(&ctx, &mut state, response_bytes, 0, true, 19)
                .await
                .unwrap();

        assert!(hits_are_empty);
        assert_eq!(state.retrieved_hits, 0);
        assert_eq!(
            ctx.retrieved_count
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_eq!(
            ctx.retrieved_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            19
        );
        assert!(worker_rx.try_recv().is_err());
    }
}
