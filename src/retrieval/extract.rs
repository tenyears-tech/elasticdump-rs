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

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ExtractedBatch {
    pub(crate) metadata: BatchMetadata,
    pub(crate) output: ExtractedOutputBatch,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ExtractedOutputBatch {
    pub(crate) buffer: Vec<u8>,
    pub(crate) doc_count: u64,
}

struct MetadataValues {
    relation: Option<String>,
    total_value: Option<u64>,
    next_pit_id: Option<String>,
    next_scroll_id: Option<String>,
    shards_failed: Option<u64>,
    timed_out: Option<bool>,
}

struct MetadataInputs<'a> {
    values: MetadataValues,
    hits: LazyValue<'a>,
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
        // Added AFTER the five above so the existing reverse-add pops stay valid;
        // these two are popped FIRST in extract_metadata_inputs (see the pop order there).
        tree.add_path(&["_shards", "failed"]);
        tree.add_path(&["timed_out"]);
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

fn extract_metadata_inputs(response_bytes: &Bytes) -> Result<MetadataInputs<'_>> {
    let mut values = get_many(response_bytes, metadata_tree())?;
    // INVARIANT: get_many yields values in metadata_tree()'s add order, and pop()
    // returns them in reverse. metadata_tree() adds, in order: _scroll_id, pit_id,
    // hits.total.value, hits.total.relation, hits.hits, _shards.failed, timed_out.
    // So these pops MUST stay in this exact (reverse) order and adjacent.
    let timed_out = values.pop().flatten().and_then(|value| value.as_bool());
    let shards_failed = values.pop().flatten().and_then(|value| value.as_u64());
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

    Ok(MetadataInputs {
        values: MetadataValues {
            relation,
            total_value,
            next_pit_id,
            next_scroll_id,
            shards_failed,
            timed_out,
        },
        hits,
    })
}

fn finalize_metadata(
    values: MetadataValues,
    search_type: &SearchType,
    doc_count: u64,
    last_sort_raw: Option<Vec<u8>>,
) -> Result<BatchMetadata> {
    if matches!(search_type, SearchType::PointInTime) && doc_count > 0 && last_sort_raw.is_none() {
        return Err(anyhow!(
            "PIT response contains hits but the final hit is missing a usable sort value"
        ));
    }

    // Partial responses must NEVER be treated as a complete batch: a timed-out or
    // failed-shard response silently drops documents. Absent fields (None) are fine
    // (not every response carries them) — only positive signals reject the batch.
    if values.timed_out == Some(true) {
        return Err(anyhow!(
            "Elasticsearch response reports timed_out=true; refusing partial results (raise --requestTimeout / cluster timeouts and retry)"
        ));
    }
    if let Some(failed) = values.shards_failed.filter(|f| *f > 0) {
        return Err(anyhow!(
            "Elasticsearch response reports {failed} failed shard(s); refusing a partial dump (check cluster health and retry)"
        ));
    }

    let total_hits = match (values.total_value, values.relation.as_deref()) {
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
            is_exact: false,
        },
    };

    Ok(BatchMetadata {
        total_hits,
        next_scroll_id: values.next_scroll_id,
        next_pit_id: values.next_pit_id,
        last_sort_raw,
        doc_count,
        hits_are_empty: doc_count == 0,
    })
}

pub(crate) fn extract_batch_metadata(
    response_bytes: &Bytes,
    search_type: &SearchType,
) -> Result<BatchMetadata> {
    let metadata_inputs = extract_metadata_inputs(response_bytes)?;
    let (doc_count, last_sort_raw) =
        count_hits_and_last_sort_raw(metadata_inputs.hits, search_type)?;

    finalize_metadata(metadata_inputs.values, search_type, doc_count, last_sort_raw)
}

pub(crate) fn extract_batch(
    response_bytes: &Bytes,
    search_type: &SearchType,
) -> Result<ExtractedBatch> {
    let metadata_inputs = extract_metadata_inputs(response_bytes)?;
    let iter = metadata_inputs
        .hits
        .into_array_iter()
        .ok_or_else(|| anyhow!("Elasticsearch response field hits.hits is not iterable"))?;

    let mut doc_count = 0u64;
    let mut last_sort_raw = None;
    let mut buffer = Vec::with_capacity(response_bytes.len());

    for hit in iter {
        let hit = hit?;
        doc_count += 1;

        buffer.extend_from_slice(hit.as_raw_str().as_bytes());
        buffer.push(b'\n');

        if matches!(search_type, SearchType::PointInTime) {
            match hit.get("sort") {
                Some(value) if value.is_array() => {
                    let sort_buffer = last_sort_raw.get_or_insert_with(Vec::new);
                    sort_buffer.clear();
                    sort_buffer.extend_from_slice(value.as_raw_str().as_bytes());
                }
                _ => {
                    last_sort_raw = None;
                }
            }
        }
    }

    let metadata =
        finalize_metadata(metadata_inputs.values, search_type, doc_count, last_sort_raw)?;

    Ok(ExtractedBatch {
        metadata,
        output: ExtractedOutputBatch { buffer, doc_count },
    })
}

#[cfg(test)]
mod tests {
    use super::{
        count_hits_and_last_sort_raw, extract_batch, extract_batch_metadata, require_hits_array,
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
        assert!(!metadata.total_hits.is_exact);
        assert_eq!(metadata.doc_count, 3);
    }

    #[test]
    fn extract_batch_fuses_pit_metadata_and_output() {
        let response = Bytes::from_static(
            br#"{
                "pit_id":"pit-next",
                "hits":{
                    "total":{"value":2,"relation":"gte"},
                    "hits":[
                        {"_id":"1","sort":[1,"a"],"_source":{"name":"a"}},
                        {"_id":"2","sort":[2,{"nested":true}],"_source":{"name":"b"}}
                    ]
                }
            }"#,
        );

        let extracted = extract_batch(&response, &SearchType::PointInTime).unwrap();

        assert_eq!(extracted.metadata.next_pit_id.as_deref(), Some("pit-next"));
        assert_eq!(extracted.metadata.total_hits.value, 2);
        assert!(!extracted.metadata.total_hits.is_exact);
        assert_eq!(
            extracted.metadata.last_sort_raw.as_deref(),
            Some(br#"[2,{"nested":true}]"#.as_slice())
        );
        assert_eq!(extracted.output.doc_count, 2);
        assert_eq!(
            String::from_utf8(extracted.output.buffer).unwrap(),
            concat!(
                "{\"_id\":\"1\",\"sort\":[1,\"a\"],\"_source\":{\"name\":\"a\"}}\n",
                "{\"_id\":\"2\",\"sort\":[2,{\"nested\":true}],\"_source\":{\"name\":\"b\"}}\n"
            )
        );
    }

    #[test]
    fn extract_batch_fuses_scroll_metadata_and_output() {
        let response = Bytes::from_static(
            br#"{
                "_scroll_id":"scroll-next",
                "hits":{
                    "total":{"value":2,"relation":"eq"},
                    "hits":[
                        {"_id":"1","_source":{"name":"a"}},
                        {"_id":"2","_source":{"name":"b"}}
                    ]
                }
            }"#,
        );

        let extracted = extract_batch(&response, &SearchType::Scroll).unwrap();

        assert_eq!(
            extracted.metadata.next_scroll_id.as_deref(),
            Some("scroll-next")
        );
        assert_eq!(extracted.metadata.total_hits.value, 2);
        assert!(extracted.metadata.total_hits.is_exact);
        assert!(extracted.metadata.last_sort_raw.is_none());
        assert_eq!(extracted.output.doc_count, 2);
        assert_eq!(
            String::from_utf8(extracted.output.buffer).unwrap(),
            concat!(
                "{\"_id\":\"1\",\"_source\":{\"name\":\"a\"}}\n",
                "{\"_id\":\"2\",\"_source\":{\"name\":\"b\"}}\n"
            )
        );
    }

    #[test]
    fn extract_batch_metadata_rejects_missing_hits_array() {
        let response = Bytes::from_static(br#"{"hits":{"total":{"value":0,"relation":"eq"}}}"#);

        let error = extract_batch_metadata(&response, &SearchType::Scroll)
            .unwrap_err()
            .to_string();

        assert!(error.contains("hits.hits"));
    }

    #[test]
    fn extract_batch_metadata_rejects_failed_shards() {
        let response = Bytes::from_static(
            br#"{"_scroll_id":"s","_shards":{"total":5,"successful":4,"skipped":0,"failed":1},"hits":{"total":{"value":10,"relation":"eq"},"hits":[{"_id":"1","_source":{}}]}}"#,
        );
        let error = extract_batch_metadata(&response, &SearchType::Scroll)
            .unwrap_err()
            .to_string();
        assert!(error.contains("1 failed shard"));
    }

    #[test]
    fn extract_batch_metadata_rejects_timed_out_responses() {
        let response = Bytes::from_static(
            br#"{"_scroll_id":"s","timed_out":true,"_shards":{"total":5,"successful":5,"skipped":0,"failed":0},"hits":{"total":{"value":10,"relation":"eq"},"hits":[{"_id":"1","_source":{}}]}}"#,
        );
        let error = extract_batch_metadata(&response, &SearchType::Scroll)
            .unwrap_err()
            .to_string();
        assert!(error.contains("timed_out"));
    }

    #[test]
    fn extract_batch_metadata_accepts_zero_failed_shards() {
        let response = Bytes::from_static(
            br#"{"_scroll_id":"s","timed_out":false,"_shards":{"total":5,"successful":5,"skipped":0,"failed":0},"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"1","_source":{}}]}}"#,
        );

        let metadata = extract_batch_metadata(&response, &SearchType::Scroll).unwrap();

        assert_eq!(metadata.next_scroll_id.as_deref(), Some("s"));
        assert_eq!(metadata.doc_count, 1);
        assert!(!metadata.hits_are_empty);
    }

    #[test]
    fn extract_batch_preserves_bytes_with_escapes_and_unicode() {
        // Adversarial hits: JSON escapes (quote/backslash/slash), non-ASCII + emoji,
        // raw UTF-8, and _source keys colliding with metadata pointers
        // ("hits", "sort", "_scroll_id", "pit_id") carrying object/array values.
        let hit1 = r#"{"_id":"1","_source":{"text":"quote\"backslash\\slash\/","unicode":"é中😀","hits":{"nested":true},"sort":[1,"a"]}}"#;
        let hit2 = r#"{"_id":"2","_source":{"raw":"中文😀","_scroll_id":"decoy-scroll","pit_id":["decoy","array"]}}"#;
        let response_str = format!(
            r#"{{"_scroll_id":"real-scroll","hits":{{"total":{{"value":2,"relation":"eq"}},"hits":[{hit1},{hit2}]}}}}"#
        );
        let response = Bytes::from(response_str.into_bytes());

        let extracted = extract_batch(&response, &SearchType::Scroll).unwrap();

        // The top-level pointers must win over the colliding _source keys.
        assert_eq!(
            extracted.metadata.next_scroll_id.as_deref(),
            Some("real-scroll")
        );
        assert!(extracted.metadata.next_pit_id.is_none());

        // Byte-identical output: exact raw hit bytes joined by '\n', not a `contains` check.
        let mut expected = Vec::new();
        expected.extend_from_slice(hit1.as_bytes());
        expected.push(b'\n');
        expected.extend_from_slice(hit2.as_bytes());
        expected.push(b'\n');
        assert_eq!(extracted.output.buffer, expected);
        assert_eq!(extracted.output.doc_count, 2);
    }

    #[test]
    fn extract_batch_handles_empty_hits_array() {
        let response = Bytes::from_static(
            br#"{"_scroll_id":"scroll-empty","hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}"#,
        );

        let extracted = extract_batch(&response, &SearchType::Scroll).unwrap();

        assert_eq!(extracted.metadata.doc_count, 0);
        assert!(extracted.metadata.hits_are_empty);
        assert!(extracted.output.buffer.is_empty());
        assert_eq!(extracted.output.doc_count, 0);
    }
}
