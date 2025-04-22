use anyhow::Result;
use serde_json::Value;

/// Represents a processed batch ready for writing
pub struct ProcessedBatch {
    pub buffer: Vec<u8>,
    pub doc_count: u64,
}

/// Process a batch of Elasticsearch response data into a format ready for output
pub fn process_batch(response: &Value) -> Result<ProcessedBatch> {
    let hits = match response["hits"]["hits"].as_array() {
        Some(hits) => hits,
        None => {
            // No hits array, return empty batch
            return Ok(ProcessedBatch {
                buffer: Vec::new(),
                doc_count: 0,
            });
        }
    };

    if hits.is_empty() {
        // Hits array is empty, return empty batch
        return Ok(ProcessedBatch {
            buffer: Vec::new(),
            doc_count: 0,
        });
    }

    // Estimate buffer size: (average doc size + newline) * num docs
    // This is a rough guess, Vec will reallocate if needed.
    // Assuming average doc size ~ 1KB for initial allocation.
    let estimated_size = hits.len() * (1024 + 1);
    let mut buffer = Vec::with_capacity(estimated_size);

    for hit in hits {
        // Serialize the entire hit document
        serde_json::to_writer(&mut buffer, hit)?;
        // Append a newline character
        buffer.push(b'\n');
    }

    let doc_count = hits.len() as u64; // Use hits.len() for doc count

    Ok(ProcessedBatch {
        buffer, // The buffer now contains all serialized docs + newlines
        doc_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_process_batch() {
        // Create test data
        let test_response = json!({
            "hits": {
                "hits": [
                    {
                        "_id": "doc1",
                        "_index": "test_index",
                        "_type": "_doc",
                        "_score": 1.0,
                        "_source": {
                            "id": 1,
                            "name": "Document 1"
                        }
                    },
                    {
                        "_id": "doc2",
                        "_index": "test_index",
                        "_type": "_doc",
                        "_score": 1.0,
                        "_source": {
                            "id": 2,
                            "name": "Document 2"
                        }
                    }
                ]
            }
        });

        // Process the batch
        let processed = process_batch(&test_response).unwrap();

        // Verify the count
        assert_eq!(processed.doc_count, 2, "Should have processed 2 documents");

        // Verify the buffer content
        assert!(!processed.buffer.is_empty(), "Buffer should not be empty");

        // Split the buffer by newline and validate each line as JSON
        let lines: Vec<_> = processed
            .buffer
            .split(|&c| c == b'\n')
            .filter(|line| !line.is_empty())
            .collect();
        assert_eq!(lines.len(), 2, "Should have 2 lines in the buffer");

        // Convert lines (Vec<u8>) to strings for comparison
        let line1_str = std::str::from_utf8(lines[0]).expect("Line 1 should be valid UTF-8");
        let line2_str = std::str::from_utf8(lines[1]).expect("Line 2 should be valid UTF-8");

        // Parse back to Value to compare
        let parsed1: Value = serde_json::from_str(line1_str).expect("Line 1 should be valid JSON");
        let parsed2: Value = serde_json::from_str(line2_str).expect("Line 2 should be valid JSON");

        // Verify the full document structure
        assert_eq!(parsed1["_id"], "doc1", "First document ID mismatch");
        assert_eq!(
            parsed1["_index"], "test_index",
            "First document index mismatch"
        );
        assert_eq!(
            parsed1["_source"]["id"], 1,
            "First document source id mismatch"
        );
        assert_eq!(
            parsed1["_source"]["name"], "Document 1",
            "First document source name mismatch"
        );

        assert_eq!(parsed2["_id"], "doc2", "Second document ID mismatch");
        assert_eq!(
            parsed2["_index"], "test_index",
            "Second document index mismatch"
        );
        assert_eq!(
            parsed2["_source"]["id"], 2,
            "Second document source id mismatch"
        );
        assert_eq!(
            parsed2["_source"]["name"], "Document 2",
            "Second document source name mismatch"
        );
    }
} 
