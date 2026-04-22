use anyhow::{Result, anyhow};
use bytes::Bytes;
use sonic_rs::{JsonValueTrait, LazyValue, PointerTree, get_many, pointer};
use std::sync::OnceLock;

use crate::cli::SearchType;

use super::retrieval_task::TotalHitsEstimate;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BatchMetadata {
    pub(crate) total_hits: TotalHitsEstimate,
    pub(crate) next_scroll_id: Option<String>,
    pub(crate) next_pit_id: Option<String>,
    pub(crate) last_sort_raw: Option<Vec<u8>>,
    pub(crate) doc_count: u64,
    pub(crate) hits_are_empty: bool,
}

pub(crate) struct ExtractedOutputBatch {
    pub(crate) buffer: Vec<u8>,
    pub(crate) doc_count: u64,
}

fn metadata_tree() -> &'static PointerTree {
    static TREE: OnceLock<PointerTree> = OnceLock::new();

    TREE.get_or_init(|| {
        let mut tree = PointerTree::new();
        tree.add_path(&["_scroll_id"]);
        tree.add_path(&["pit_id"]);
        tree.add_path(&["hits", "total", "value"]);
        tree.add_path(&["hits", "total", "relation"]);
        tree.add_path(&pointer!["hits", "hits"]);
        tree
    })
}

fn require_hits_array<'a>(hits: Option<LazyValue<'a>>) -> Result<LazyValue<'a>> {
    let hits = hits.ok_or_else(|| anyhow!("Elasticsearch response is missing hits.hits"))?;
    if !hits.is_array() {
        return Err(anyhow!(
            "Elasticsearch response field hits.hits is not an array"
        ));
    }

    Ok(hits)
}

fn count_hits_and_last_sort_raw<'a>(
    hits: LazyValue<'a>,
    search_type: &SearchType,
) -> Result<(u64, Option<Vec<u8>>)> {
    let mut doc_count = 0u64;
    let mut last_sort_raw = None;
    let iter = hits
        .into_array_iter()
        .ok_or_else(|| anyhow!("Elasticsearch response field hits.hits is not iterable"))?;

    for hit in iter {
        let hit = hit?;
        doc_count += 1;

        if matches!(search_type, SearchType::PointInTime) {
            match hit.get("sort") {
                Some(value) if value.is_array() => {
                    let buffer = last_sort_raw.get_or_insert_with(Vec::new);
                    buffer.clear();
                    buffer.extend_from_slice(value.as_raw_str().as_bytes());
                }
                _ => {
                    last_sort_raw = None;
                }
            }
        }
    }

    Ok((doc_count, last_sort_raw))
}

pub(crate) fn extract_batch_metadata(
    response_bytes: &Bytes,
    search_type: &SearchType,
) -> Result<BatchMetadata> {
    let mut values = get_many(response_bytes, metadata_tree())?;
    let hits = require_hits_array(values.pop().flatten())?;
    let relation = values
        .pop()
        .flatten()
        .and_then(|value| value.as_str().map(str::to_owned));
    let total_value = values.pop().flatten().and_then(|value| value.as_u64());
    let next_pit_id = values
        .pop()
        .flatten()
        .and_then(|value| value.as_str().map(str::to_owned));
    let next_scroll_id = values
        .pop()
        .flatten()
        .and_then(|value| value.as_str().map(str::to_owned));

    let (doc_count, last_sort_raw) = count_hits_and_last_sort_raw(hits, search_type)?;

    if matches!(search_type, SearchType::PointInTime) && doc_count > 0 && last_sort_raw.is_none() {
        return Err(anyhow!(
            "PIT response contains hits but the final hit is missing a usable sort value"
        ));
    }

    let total_hits = match (total_value, relation.as_deref()) {
        (Some(value), Some("eq")) => TotalHitsEstimate {
            value,
            is_exact: true,
        },
        (Some(value), Some(_)) => TotalHitsEstimate {
            value,
            is_exact: false,
        },
        (Some(value), None) => TotalHitsEstimate {
            value,
            is_exact: true,
        },
        (None, _) => TotalHitsEstimate {
            value: doc_count,
            is_exact: true,
        },
    };

    Ok(BatchMetadata {
        total_hits,
        next_scroll_id,
        next_pit_id,
        last_sort_raw,
        doc_count,
        hits_are_empty: doc_count == 0,
    })
}

pub(crate) fn build_output_batch(response_bytes: &Bytes) -> Result<ExtractedOutputBatch> {
    let hits = sonic_rs::get(response_bytes, &["hits", "hits"]).map_err(|error| {
        anyhow!(
            "Failed to locate hits.hits for output extraction: {}",
            error
        )
    })?;
    let iter = hits
        .into_array_iter()
        .ok_or_else(|| anyhow!("Elasticsearch response field hits.hits is not iterable"))?;

    let mut buffer = Vec::with_capacity(response_bytes.len());
    let mut doc_count = 0u64;

    for hit in iter {
        let hit = hit?;
        buffer.extend_from_slice(hit.as_raw_str().as_bytes());
        buffer.push(b'\n');
        doc_count += 1;
    }

    Ok(ExtractedOutputBatch { buffer, doc_count })
}

#[cfg(test)]
mod tests {
    use super::{
        build_output_batch, count_hits_and_last_sort_raw, extract_batch_metadata,
        require_hits_array,
    };
    use crate::cli::SearchType;
    use bytes::Bytes;

    #[test]
    fn extract_batch_metadata_reads_scroll_total_and_scroll_id() {
        let response = Bytes::from_static(
            br#"{
                "_scroll_id":"scroll-123",
                "hits":{
                    "total":{"value":2,"relation":"eq"},
                    "hits":[
                        {"_id":"1","_source":{"name":"a"}},
                        {"_id":"2","_source":{"name":"b"}}
                    ]
                }
            }"#,
        );

        let metadata = extract_batch_metadata(&response, &SearchType::Scroll).unwrap();

        assert_eq!(metadata.next_scroll_id.as_deref(), Some("scroll-123"));
        assert_eq!(metadata.total_hits.value, 2);
        assert!(metadata.total_hits.is_exact);
        assert_eq!(metadata.doc_count, 2);
        assert!(!metadata.hits_are_empty);
        assert!(metadata.last_sort_raw.is_none());
    }

    #[test]
    fn extract_batch_metadata_reads_pit_id_and_last_sort() {
        let response = Bytes::from_static(
            br#"{
                "pit_id":"pit-next",
                "hits":{
                    "total":{"value":10000,"relation":"gte"},
                    "hits":[
                        {"_id":"1","sort":[1,"a"],"_source":{"name":"a"}},
                        {"_id":"2","sort":[2,"b"],"_source":{"name":"b"}}
                    ]
                }
            }"#,
        );

        let metadata = extract_batch_metadata(&response, &SearchType::PointInTime).unwrap();

        assert_eq!(metadata.next_pit_id.as_deref(), Some("pit-next"));
        assert_eq!(metadata.total_hits.value, 10000);
        assert!(!metadata.total_hits.is_exact);
        assert_eq!(
            metadata.last_sort_raw.as_deref(),
            Some(br#"[2,"b"]"#.as_slice())
        );
        assert_eq!(metadata.doc_count, 2);
    }

    #[test]
    fn count_hits_and_last_pit_sort_reads_raw_json_without_vec_per_hit() {
        let response = Bytes::from_static(
            br#"{
                "pit_id":"pit-next",
                "hits":{
                    "hits":[
                        {"_id":"1","sort":[1,"a"],"_source":{"name":"a"}},
                        {"_id":"2","sort":[2,{"nested":true}],"_source":{"name":"b"}}
                    ]
                }
            }"#,
        );

        let hits =
            require_hits_array(Some(sonic_rs::get(&response, &["hits", "hits"]).unwrap())).unwrap();
        let (doc_count, last_sort_raw) =
            count_hits_and_last_sort_raw(hits, &SearchType::PointInTime).unwrap();

        assert_eq!(doc_count, 2);
        assert_eq!(
            last_sort_raw.as_deref(),
            Some(br#"[2,{"nested":true}]"#.as_slice())
        );
    }

    #[test]
    fn extract_batch_metadata_rejects_non_usable_final_pit_sort() {
        let response = Bytes::from_static(
            br#"{
                "pit_id":"pit-next",
                "hits":{
                    "total":{"value":2,"relation":"eq"},
                    "hits":[
                        {"_id":"1","sort":[1,"a"],"_source":{"name":"a"}},
                        {"_id":"2","sort":null,"_source":{"name":"b"}}
                    ]
                }
            }"#,
        );

        let error = extract_batch_metadata(&response, &SearchType::PointInTime)
            .unwrap_err()
            .to_string();

        assert!(error.contains("usable sort value"));
    }

    #[test]
    fn extract_batch_metadata_falls_back_to_hits_len_when_total_missing() {
        let response = Bytes::from_static(
            br#"{
                "_scroll_id":"scroll-123",
                "hits":{
                    "hits":[
                        {"_id":"1","_source":{"name":"a"}},
                        {"_id":"2","_source":{"name":"b"}},
                        {"_id":"3","_source":{"name":"c"}}
                    ]
                }
            }"#,
        );

        let metadata = extract_batch_metadata(&response, &SearchType::Scroll).unwrap();

        assert_eq!(metadata.total_hits.value, 3);
        assert!(metadata.total_hits.is_exact);
        assert_eq!(metadata.doc_count, 3);
    }

    #[test]
    fn build_output_batch_preserves_original_hit_json() {
        let response = Bytes::from_static(
            br#"{
                "_scroll_id":"scroll-123",
                "hits":{
                    "total":{"value":2,"relation":"eq"},
                    "hits":[
                        {"_id":"1","_source":{"name":"a","nested":{"x":1}}},
                        {"_id":"2","_source":{"name":"b","tags":["t1","t2"]}}
                    ]
                }
            }"#,
        );

        let batch = build_output_batch(&response).unwrap();
        let output = String::from_utf8(batch.buffer).unwrap();

        assert_eq!(batch.doc_count, 2);
        assert!(output.contains(r#"{"_id":"1","_source":{"name":"a","nested":{"x":1}}}"#));
        assert!(output.contains(r#"{"_id":"2","_source":{"name":"b","tags":["t1","t2"]}}"#));
        assert_eq!(output.lines().count(), 2);
    }

    #[test]
    fn extract_batch_metadata_rejects_missing_hits_array() {
        let response = Bytes::from_static(br#"{"hits":{"total":{"value":0,"relation":"eq"}}}"#);

        let error = extract_batch_metadata(&response, &SearchType::Scroll)
            .unwrap_err()
            .to_string();

        assert!(error.contains("hits.hits"));
    }
}
