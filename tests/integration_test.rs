use anyhow::Result;
use elasticsearch::{
    Elasticsearch, SearchParts,
    http::request::JsonBody,
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    indices::{IndicesCreateParts, IndicesDeleteParts, IndicesRefreshParts},
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{BufRead, BufReader},
    process::{Command, Stdio},
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};
use url::Url;

// Nested under the single integration-test target: only `.rs` files directly in
// `tests/` become their own test crates, so `support/` stays a plain module.
mod support;
use support::mock_es::{CannedResponse, MockEs};

const ES_URL: &str = "http://localhost:9200";
const TEST_INDEX_PREFIX: &str = "elasticdump_rs_test";

// Static counter to ensure unique index names for parallel test execution
static INDEX_COUNTER: AtomicU32 = AtomicU32::new(0);

// Helper function to get a unique test index name. The process id keeps two
// concurrent `cargo test` processes sharing one ES node from colliding, and the
// per-process counter keeps parallel tests within a process distinct.
fn get_unique_test_index() -> String {
    let counter = INDEX_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}_{}_{}", TEST_INDEX_PREFIX, std::process::id(), counter)
}

// A process-and-counter-unique output path under the system temp dir, so file
// outputs never collide across concurrent test processes and never litter the
// working tree.
fn unique_output_path(label: &str) -> std::path::PathBuf {
    let counter = INDEX_COUNTER.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!(
        "elasticdump_rs_{}_{}_{}.jsonl",
        label,
        std::process::id(),
        counter
    ))
}

// Helper function to wait for Elasticsearch to be available
async fn wait_for_elasticsearch() -> Result<()> {
    let url = Url::parse(ES_URL)?;
    let conn_pool = SingleNodeConnectionPool::new(url.clone());
    let transport = TransportBuilder::new(conn_pool).build()?;
    let client = Elasticsearch::new(transport);

    // Try to connect up to 5 times with a 2-second delay between attempts
    for attempt in 1..=5 {
        println!(
            "Attempting to connect to Elasticsearch (attempt {})",
            attempt
        );
        match client.ping().send().await {
            Ok(_) => {
                println!("Successfully connected to Elasticsearch");
                return Ok(());
            }
            Err(err) => {
                if attempt == 5 {
                    return Err(anyhow::anyhow!(
                        "Failed to connect to Elasticsearch: {}",
                        err
                    ));
                }
                println!("Connection failed: {}. Retrying in 2 seconds...", err);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }

    unreachable!()
}

// Helper function to insert large dataset using bulk indexing
async fn insert_large_dataset_bulk(
    client: &Elasticsearch,
    test_index: &str,
    start_id: u32,
    num_docs: u32,
    paragraph_words: usize, // Number of words for the random paragraph
    bulk_size: usize,
) -> Result<()> {
    println!(
        "Adding {} documents (IDs {} to {}) using bulk indexing...",
        num_docs,
        start_id,
        start_id + num_docs - 1
    );

    for batch_start in (0..num_docs).step_by(bulk_size) {
        let current_batch_size = std::cmp::min(bulk_size, (num_docs - batch_start) as usize);
        let batch_end_id = start_id + batch_start + current_batch_size as u32 - 1;

        println!(
            "  Adding batch: IDs {} to {}",
            start_id + batch_start,
            batch_end_id
        );

        let mut body: Vec<JsonBody<Value>> = Vec::with_capacity(current_batch_size * 2);

        for i in 0..current_batch_size {
            let doc_id = start_id + batch_start + i as u32;

            // Add index action metadata
            body.push(json!({ "index": {} }).into());

            // Generate random paragraph using simpler lipsum::lipsum
            let extra_content = lipsum::lipsum(paragraph_words);

            // Add document source
            let document = json!({
                "id": doc_id,
                "name": format!("Bulk Test Document {}", doc_id),
                "description": format!("This is a bulk test document with ID {}", doc_id),
                "timestamp": "2023-06-15T12:00:00Z",
                "tags": ["test", "bulk", if doc_id % 2 == 0 { "even" } else { "odd" }],
                "extra": extra_content, // Use generated random text
                "nested_field": {
                    "value1": format!("nested value {}", doc_id),
                    "value2": doc_id * 10
                },
                "array_field": [doc_id, doc_id*2, doc_id*3]
            });
            body.push(document.into());
        }

        let response = client
            .bulk(elasticsearch::BulkParts::Index(test_index))
            .body(body)
            .send()
            .await?;

        if !response.status_code().is_success() {
            return Err(anyhow::anyhow!(
                "Failed to bulk insert batch starting at {}: {:?}",
                start_id + batch_start,
                response.text().await?
            ));
        }

        // Check for item-level errors in the bulk response
        let response_body: Value = response.json().await?;
        if response_body["errors"].as_bool().unwrap_or(false) {
            let mut errors = Vec::new();
            if let Some(items) = response_body["items"].as_array() {
                for item in items {
                    if let Some(op_type) = item.as_object().and_then(|obj| obj.keys().next()) {
                        if let Some(error) = item[op_type]["error"].as_object() {
                            errors.push(error.clone());
                        }
                    }
                }
            }
            if !errors.is_empty() {
                return Err(anyhow::anyhow!(
                    "Errors encountered during bulk insert: {:?}",
                    errors
                ));
            }
        }
    }

    Ok(())
}

// Setup test data in Elasticsearch
async fn setup_test_data(test_index: &str) -> Result<()> {
    // Wait for Elasticsearch to be available
    wait_for_elasticsearch().await?;

    let url = Url::parse(ES_URL)?;
    let conn_pool = SingleNodeConnectionPool::new(url.clone());
    let transport = TransportBuilder::new(conn_pool).build()?;
    let client = Elasticsearch::new(transport);

    // Delete the test index if it exists
    let _ = client
        .indices()
        .delete(IndicesDeleteParts::Index(&[test_index]))
        .send()
        .await;

    // Create the test index
    let create_response = client
        .indices()
        .create(IndicesCreateParts::Index(test_index))
        .body(json!({
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": {
                "properties": {
                    "id": { "type": "integer" },
                    "name": { "type": "keyword" },
                    "description": { "type": "text" },
                    "timestamp": { "type": "date" },
                    "tags": { "type": "keyword" }
                }
            }
        }))
        .send()
        .await?;

    assert!(
        create_response.status_code().is_success(),
        "Failed to create index: {:?}",
        create_response.text().await?
    );

    // Insert documents individually
    for i in 1..=100 {
        let document = json!({
            "id": i,
            "name": format!("Test Document {}", i),
            "description": format!("This is a test document with ID {}", i),
            "timestamp": "2023-06-15T12:00:00Z",
            "tags": ["test", if i % 2 == 0 { "even" } else { "odd" }]
        });

        let response = client
            .index(elasticsearch::IndexParts::Index(test_index))
            .body(document)
            .send()
            .await?;

        assert!(
            response.status_code().is_success(),
            "Failed to insert document {}: {:?}",
            i,
            response.text().await?
        );
    }

    // Refresh the index to make sure all documents are available for search
    let refresh_response = client
        .indices()
        .refresh(IndicesRefreshParts::Index(&[test_index]))
        .send()
        .await?;

    assert!(
        refresh_response.status_code().is_success(),
        "Failed to refresh index: {:?}",
        refresh_response.text().await?
    );

    // Verify the documents were inserted
    let search_response = client
        .search(SearchParts::Index(&[test_index]))
        .body(json!({
            "query": { "match_all": {} },
            "size": 0
        }))
        .send()
        .await?;

    let search_response: Value = search_response.json().await?;
    let total_hits = search_response["hits"]["total"]["value"].as_u64().unwrap();

    assert_eq!(
        total_hits, 100,
        "Expected 100 documents, found {}",
        total_hits
    );

    // Refresh the index
    client
        .indices()
        .refresh(IndicesRefreshParts::Index(&[test_index]))
        .send()
        .await?;

    Ok(())
}

// Run the elasticdump-rs command
fn elasticdump_binary() -> &'static str {
    env!("CARGO_BIN_EXE_elasticdump-rs")
}

/// A `Command` for the binary with any ambient HTTP proxy disabled, so requests
/// to the loopback mock server (and to localhost ES) connect directly instead
/// of being intercepted by a developer machine's system proxy.
fn binary_command() -> Command {
    let mut cmd = Command::new(elasticdump_binary());
    cmd.env_remove("HTTP_PROXY")
        .env_remove("http_proxy")
        .env_remove("HTTPS_PROXY")
        .env_remove("https_proxy")
        .env_remove("ALL_PROXY")
        .env_remove("all_proxy")
        .env("NO_PROXY", "127.0.0.1,localhost,::1")
        .env("no_proxy", "127.0.0.1,localhost,::1");
    cmd
}

fn run_elasticdump_command(args: &[&str]) -> Result<()> {
    let status = binary_command()
        .args(args)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()?;

    assert!(status.success(), "elasticdump-rs command failed");

    Ok(())
}

fn run_elasticdump_command_capture(args: &[&str]) -> Result<std::process::Output> {
    binary_command().args(args).output().map_err(Into::into)
}

// Cleanup test data
async fn cleanup(test_index: &str, output_file: &str) -> Result<()> {
    let url = Url::parse(ES_URL)?;
    let conn_pool = SingleNodeConnectionPool::new(url.clone());
    let transport = TransportBuilder::new(conn_pool).build()?;
    let client = Elasticsearch::new(transport);

    // Delete the test index
    let _ = client
        .indices()
        .delete(IndicesDeleteParts::Index(&[test_index]))
        .send()
        .await;

    // Remove output file if it exists
    let _ = std::fs::remove_file(output_file);

    Ok(())
}

#[tokio::test]
async fn test_basic_dump() -> Result<()> {
    // Get a unique test index and output file
    let test_index = get_unique_test_index();
    let output_file = format!("test_output_{}.jsonl", test_index);

    // Setup test data
    setup_test_data(&test_index).await?;

    // Run the elasticdump-rs command
    run_elasticdump_command(&[
        "--input",
        &format!("{}/{}", ES_URL, test_index),
        "--output",
        &output_file,
        "--quiet", // Suppress progress bar in tests
    ])?;

    // Verify the output file exists and contains the correct data
    let file = File::open(&output_file)?;
    let reader = BufReader::new(file);
    let mut line_count = 0;

    for line in reader.lines() {
        let line = line?;
        let document: Value = serde_json::from_str(&line)?;

        assert!(
            document.is_object(),
            "Each line should be a valid JSON object"
        );
        assert!(
            document.get("_source").is_some(),
            "Each document should have a _source field"
        );
        let source = document["_source"].as_object().unwrap();
        assert!(
            source.contains_key("id"),
            "Each document should have an id in _source"
        );
        assert!(
            source.contains_key("name"),
            "Each document should have a name in _source"
        );
        assert!(
            source.contains_key("description"),
            "Each document should have a description in _source"
        );

        line_count += 1;
    }

    assert_eq!(line_count, 100, "Expected 100 documents in the output file");

    // Cleanup
    cleanup(&test_index, &output_file).await?;

    Ok(())
}

#[tokio::test]
async fn test_filtered_dump() -> Result<()> {
    // Get a unique test index and output file
    let test_index = get_unique_test_index();
    let output_file = format!("test_output_{}.jsonl", test_index);

    // Setup test data
    setup_test_data(&test_index).await?;

    // Run the elasticdump-rs command with a filter query using --searchBody
    run_elasticdump_command(&[
        "--input",
        &format!("{}/{}", ES_URL, test_index),
        "--output",
        &output_file,
        "--searchBody", // Use the new flag name
        r#"{"query":{"term":{"tags":"even"}}}"#,
        "--quiet", // Suppress progress bar in tests
    ])?;

    // Verify the output file exists and contains only even-tagged documents
    let file = File::open(&output_file)?;
    let reader = BufReader::new(file);
    let mut line_count = 0;

    for line in reader.lines() {
        let line = line?;
        let document: Value = serde_json::from_str(&line)?;

        // Verify this is an even-tagged document
        let tags = document["_source"]["tags"].as_array().unwrap();
        assert!(
            tags.contains(&Value::String("even".to_string())),
            "Document should have 'even' tag: {:?}",
            document
        );

        line_count += 1;
    }

    assert_eq!(
        line_count, 50,
        "Expected 50 documents (even tags) in the output file"
    );

    // Cleanup
    cleanup(&test_index, &output_file).await?;

    Ok(())
}

#[tokio::test]
async fn test_stdout_output() -> Result<()> {
    // Get a unique test index
    let test_index = get_unique_test_index();

    // Setup test data
    setup_test_data(&test_index).await?;

    // Run elasticdump-rs with output to stdout, captured to a file
    let output = binary_command()
        .args([
            "--input",
            &format!("{}/{}", ES_URL, test_index),
            "--output",
            "$",
            "--quiet", // Suppress progress bar in tests
        ])
        .stdout(Stdio::piped())
        .spawn()?
        .wait_with_output()?;

    assert!(output.status.success(), "elasticdump-rs command failed");

    // Parse the output to count and validate documents
    let output_str = String::from_utf8(output.stdout)?;
    let mut line_count = 0;

    for line in output_str.lines() {
        if line.is_empty() {
            continue;
        }

        let document: Value = serde_json::from_str(line)?;

        assert!(
            document.is_object(),
            "Each line should be a valid JSON object"
        );
        assert!(
            document["_source"].get("id").is_some(),
            "Each document should have an ID"
        );

        line_count += 1;
    }

    assert_eq!(
        line_count, 100,
        "Expected 100 documents in the stdout output"
    );

    // Cleanup
    cleanup(&test_index, "").await?;

    Ok(())
}

#[tokio::test]
async fn test_pagination_and_complex_query() -> Result<()> {
    // Get a unique test index and output file
    let test_index = get_unique_test_index();
    let output_file = format!("test_output_{}.jsonl", test_index);

    // Setup test data
    setup_test_data(&test_index).await?;

    // Run the elasticdump-rs command with a smaller batch size and complex query
    run_elasticdump_command(&[
        "--input",
        &format!("{}/{}", ES_URL, test_index),
        "--output",
        &output_file,
        "--limit",
        "10",           // Only 10 documents per batch to test pagination
        "--searchBody", // Use the new flag name
        r#"{"query":{"bool":{"must":[{"range":{"id":{"gte":20,"lte":70}}}]}}}"#,
        "--quiet", // Suppress progress bar in tests
    ])?;

    // Verify the output file exists and contains the expected data
    let file = File::open(&output_file)?;
    let reader = BufReader::new(file);
    let mut line_count = 0;
    let mut ids = Vec::new();

    for line in reader.lines() {
        let line = line?;
        let document: Value = serde_json::from_str(&line)?;

        assert!(
            document.is_object(),
            "Each line should be a valid JSON object"
        );

        // Extract and store the ID
        let id = document["_source"]["id"].as_i64().unwrap();
        assert!(
            (20..=70).contains(&id),
            "ID should be between 20 and 70, got {}",
            id
        );
        ids.push(id);

        line_count += 1;
    }

    // We should have exactly 51 documents (20 to 70 inclusive)
    assert_eq!(line_count, 51, "Expected 51 documents in the output file");

    // Verify we got all IDs in the range
    ids.sort();
    for i in 20..=70 {
        assert!(
            ids.contains(&i64::from(i)),
            "Missing ID {} in the results",
            i
        );
    }

    // Cleanup
    cleanup(&test_index, &output_file).await?;

    Ok(())
}

#[tokio::test]
async fn test_scroll_dump_preserves_full_hit_documents() -> Result<()> {
    let test_index = get_unique_test_index();
    let output_file = format!("test_scroll_streaming_output_{}.jsonl", test_index);

    setup_test_data(&test_index).await?;

    run_elasticdump_command(&[
        "--input",
        &format!("{}/{}", ES_URL, test_index),
        "--output",
        &output_file,
        "--type",
        "data",
        "--searchType",
        "scroll",
        "--limit",
        "10",
        "--quiet",
    ])?;

    let file = File::open(&output_file)?;
    let reader = BufReader::new(file);
    let mut saw_source = false;

    for line in reader.lines() {
        let line = line?;
        let parsed: Value = serde_json::from_str(&line)?;
        if parsed["_source"]["name"].as_str().is_some() {
            saw_source = true;
        }
    }

    assert!(saw_source);
    cleanup(&test_index, &output_file).await?;
    Ok(())
}

#[tokio::test]
async fn test_search_body_from_file() -> Result<()> {
    // Get a unique test index and output file
    let test_index = get_unique_test_index();
    let output_file = format!("test_output_{}.jsonl", test_index);
    let query_file_path = format!("test_query_{}.json", test_index);

    // Create a temporary query file
    let query_content = r#"{"query":{"term":{"tags":"odd"}}}"#;
    std::fs::write(&query_file_path, query_content)?;

    // Setup test data
    setup_test_data(&test_index).await?;

    // Run the elasticdump-rs command using --searchBody=@file
    run_elasticdump_command(&[
        "--input",
        &format!("{}/{}", ES_URL, test_index),
        "--output",
        &output_file,
        "--searchBody",
        &format!("@{}", query_file_path), // Load query from file
        "--quiet",                        // Suppress progress bar in tests
    ])?;

    // Verify the output file exists and contains only odd-tagged documents
    let file = File::open(&output_file)?;
    let reader = BufReader::new(file);
    let mut line_count = 0;

    for line in reader.lines() {
        let line = line?;
        let document: Value = serde_json::from_str(&line)?;

        // Verify this is an odd-tagged document
        let tags = document["_source"]["tags"].as_array().unwrap();
        assert!(
            tags.contains(&Value::String("odd".to_string())),
            "Document should have 'odd' tag: {:?}",
            document
        );

        line_count += 1;
    }

    assert_eq!(
        line_count, 50,
        "Expected 50 documents (odd tags) in the output file"
    );

    // Cleanup
    cleanup(&test_index, &output_file).await?;
    let _ = std::fs::remove_file(&query_file_path); // Remove the temp query file

    Ok(())
}

#[tokio::test]
async fn test_overwrite_flag() -> Result<()> {
    // Get a unique test index and output file
    let test_index = get_unique_test_index();
    let output_file = format!("test_output_{}.jsonl", test_index);

    // Setup test data
    setup_test_data(&test_index).await?;

    // Create a dummy output file first
    std::fs::write(&output_file, "dummy content")?;

    // Run dump without --overwrite (should fail or do nothing depending on implementation)
    // We expect our implementation with OpenOptions::create_new to fail here
    let status = binary_command()
        .args([
            "--input",
            &format!("{}/{}", ES_URL, test_index),
            "--output",
            &output_file,
            "--quiet",
        ])
        .status()?;
    assert!(
        !status.success(),
        "Command should fail without --overwrite when file exists"
    );

    // Run dump with --overwrite
    run_elasticdump_command(&[
        "--input",
        &format!("{}/{}", ES_URL, test_index),
        "--output",
        &output_file,
        "--overwrite", // Add the overwrite flag
        "--quiet",
    ])?;

    // Verify the content is now the actual dump (check line count)
    let file = File::open(&output_file)?;
    let reader = BufReader::new(file);
    let line_count = reader.lines().count();
    assert_eq!(line_count, 100, "Expected 100 documents after overwriting");

    // Cleanup
    cleanup(&test_index, &output_file).await?;

    Ok(())
}

#[tokio::test]
async fn test_overwrite_preserves_existing_file_on_invalid_search_body() -> Result<()> {
    let output_path = unique_output_path("invalid_search_body_preserve");
    let output_file = output_path.to_str().unwrap();
    let original_content = "keep me intact\n";
    std::fs::write(output_file, original_content)?;

    let output = Command::new(elasticdump_binary())
        .args([
            "--input",
            "http://localhost:9200/nonexistent_index",
            "--output",
            output_file,
            "--overwrite",
            "--searchBody",
            "{not valid json}",
            "--quiet",
        ])
        .output()?;

    assert!(
        !output.status.success(),
        "Command should fail when searchBody is invalid"
    );

    let final_content = std::fs::read_to_string(output_file)?;
    assert_eq!(
        final_content, original_content,
        "Existing output should remain untouched when validation fails"
    );

    std::fs::remove_file(output_file)?;

    Ok(())
}

#[tokio::test]
async fn test_reports_index_not_found_error_for_scroll() -> Result<()> {
    let output = run_elasticdump_command_capture(&[
        "--input",
        &format!("{}/{}", ES_URL, "definitely_missing_index_review_probe"),
        "--output",
        "$",
        "--quiet",
    ])?;

    assert!(
        !output.status.success(),
        "Command should fail for a missing index"
    );

    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("index_not_found_exception"),
        "stderr should include the Elasticsearch error type, got: {}",
        stderr
    );
    assert!(
        stderr.contains("definitely_missing_index_review_probe"),
        "stderr should include the missing index name, got: {}",
        stderr
    );

    Ok(())
}

#[tokio::test]
async fn test_reports_index_not_found_error_for_pit() -> Result<()> {
    let output = run_elasticdump_command_capture(&[
        "--input",
        &format!("{}/{}", ES_URL, "definitely_missing_index_review_probe"),
        "--output",
        "$",
        "--searchType",
        "pit",
        "--pitKeepAlive",
        "1m",
        "--quiet",
    ])?;

    assert!(
        !output.status.success(),
        "Command should fail for a missing index"
    );

    let stderr = String::from_utf8(output.stderr)?;
    assert!(
        stderr.contains("index_not_found_exception"),
        "stderr should include the Elasticsearch error type, got: {}",
        stderr
    );
    assert!(
        stderr.contains("definitely_missing_index_review_probe"),
        "stderr should include the missing index name, got: {}",
        stderr
    );

    Ok(())
}

#[ignore = "benchmark-style integration test"]
#[tokio::test]
async fn test_performance_benchmark() -> Result<()> {
    // Get a unique test index and output file
    let test_index = get_unique_test_index();
    let output_file = format!("test_output_{}.jsonl", test_index);

    // Setup test data
    setup_test_data(&test_index).await?;

    // Add more documents to test performance with a larger dataset
    let url = Url::parse(ES_URL)?;
    let conn_pool = SingleNodeConnectionPool::new(url.clone());
    let transport = TransportBuilder::new(conn_pool).build()?;
    let client = Elasticsearch::new(transport);

    println!("Adding more documents for performance testing...");
    // Add 10000 more documents (in addition to the 100 we already have)
    insert_large_dataset_bulk(&client, &test_index, 101, 10000, 50, 500).await?; // 50 words, 500 bulk size

    // Refresh the index
    client
        .indices()
        .refresh(IndicesRefreshParts::Index(&[&test_index]))
        .send()
        .await?;

    println!("Running performance test (dumping 10100 documents)...");
    let start_time = std::time::Instant::now();

    // Run with a larger batch size for better performance
    run_elasticdump_command(&[
        "--input",
        &format!("{}/{}", ES_URL, test_index),
        "--output",
        &output_file,
        "--limit",
        "500", // Use a larger batch size for performance
        "--overwrite",
        "--quiet", // Suppress progress bar in tests
    ])?;

    let elapsed = start_time.elapsed();
    println!("Performance test completed in {:.2?}", elapsed);

    // Verify the document count
    let file = File::open(&output_file)?;
    let reader = BufReader::new(file);
    let line_count = reader.lines().count();

    assert_eq!(
        line_count, 10100,
        "Expected 10100 documents in the output file"
    );

    // Get file size for byte throughput calculation
    let file_metadata = std::fs::metadata(&output_file)?;
    let file_size = file_metadata.len();

    // Calculate and report throughput
    let doc_throughput = line_count as f64 / elapsed.as_secs_f64();
    let bytes_throughput = file_size as f64 / elapsed.as_secs_f64(); // Bytes per second
    println!("Throughput: {:.2} documents/second", doc_throughput);
    println!(
        "Throughput: {:.2} MB/second",
        bytes_throughput / (1024.0 * 1024.0)
    ); // Report in MB/s

    // Cleanup
    cleanup(&test_index, &output_file).await?;

    Ok(())
}

#[cfg(feature = "large_scale_test")]
#[tokio::test]
async fn test_large_scale_performance() -> Result<()> {
    // Get a unique test index and output file
    let test_index = get_unique_test_index();
    let output_file = format!("test_output_{}.jsonl", test_index);

    // Setup test data
    setup_test_data(&test_index).await?;

    // Add more documents to test performance with a larger dataset
    let url = Url::parse(ES_URL)?;
    let conn_pool = SingleNodeConnectionPool::new(url.clone());
    let transport = TransportBuilder::new(conn_pool).build()?;
    let client = Elasticsearch::new(transport);

    println!("Adding more documents for performance testing...");
    // Add 1,000,000 more documents (in addition to the 100 we already have)
    insert_large_dataset_bulk(&client, &test_index, 101, 1000000, 50, 5000).await?; // 50 words, 5000 bulk size

    // Refresh the index
    client
        .indices()
        .refresh(IndicesRefreshParts::Index(&[&test_index]))
        .send()
        .await?;

    println!("Running performance test (dumping 1000100 documents)...");
    let start_time = std::time::Instant::now();

    // Run with a larger batch size for better performance
    run_elasticdump_command(&[
        "--input",
        &format!("{}/{}", ES_URL, test_index),
        "--output",
        &output_file,
        "--limit",
        "5000", // Use a larger batch size for performance
        "--overwrite",
        "--quiet", // Suppress progress bar in tests
    ])?;

    let elapsed = start_time.elapsed();
    println!("Performance test completed in {:.2?}", elapsed);

    // Verify the document count
    let file = File::open(&output_file)?;
    let reader = BufReader::new(file);
    let line_count = reader.lines().count();

    assert_eq!(
        line_count, 1000100,
        "Expected 1000100 documents in the output file"
    );

    // Get file size for byte throughput calculation
    let file_metadata = std::fs::metadata(&output_file)?;
    let file_size = file_metadata.len();

    // Calculate and report throughput
    let doc_throughput = line_count as f64 / elapsed.as_secs_f64();
    let bytes_throughput = file_size as f64 / elapsed.as_secs_f64(); // Bytes per second
    println!("Throughput: {:.2} documents/second", doc_throughput);
    println!(
        "Throughput: {:.2} MB/second",
        bytes_throughput / (1024.0 * 1024.0)
    ); // Report in MB/s

    // Cleanup
    cleanup(&test_index, &output_file).await?;

    Ok(())
}

#[tokio::test]
async fn test_sliced_scroll() -> Result<()> {
    let test_index = get_unique_test_index();
    let output_file = format!("test_sliced_scroll_output_{}.jsonl", test_index);

    // Set up test data
    setup_test_data(&test_index).await?;

    // Run elasticdump-rs with slices=4
    run_elasticdump_command(&[
        "--input",
        &format!("{}/{}", ES_URL, test_index),
        "--output",
        &output_file,
        "--slices",
        "4",       // Use 4 slices for parallel processing
        "--quiet", // Suppress progress output
    ])?;

    // Validate the output
    let file = File::open(&output_file)?;
    let reader = BufReader::new(file);
    let lines: Vec<String> = reader.lines().collect::<Result<_, _>>()?;

    // Verify we have all 100 documents
    assert_eq!(lines.len(), 100, "Should have exported all 100 documents");

    // Verify each line is valid JSON
    for line in &lines {
        let doc: Value = serde_json::from_str(line)?;
        assert!(doc.is_object(), "Each line should be a valid JSON object");
        assert!(
            doc.get("_source").is_some() && doc["_source"].get("id").is_some(),
            "Each document should have a _source with id field"
        );
    }

    // Verify we have all docs with ids 1-100
    let mut found_ids = std::collections::HashSet::new();
    for line in &lines {
        let doc: Value = serde_json::from_str(line)?;
        if let Some(id) = doc["_source"].get("id").and_then(|id| id.as_u64()) {
            found_ids.insert(id);
        }
    }

    assert_eq!(
        found_ids.len(),
        100,
        "Should have found all 100 document IDs (1-100)"
    );

    for i in 1..=100 {
        assert!(
            found_ids.contains(&(i as u64)),
            "Document with ID {} should be present",
            i
        );
    }

    // Clean up
    cleanup(&test_index, &output_file).await?;

    Ok(())
}

#[tokio::test]
async fn test_sliced_scroll_with_query() -> Result<()> {
    let test_index = get_unique_test_index();
    let output_file = format!("test_sliced_scroll_with_query_output_{}.jsonl", test_index);

    // Set up test data
    setup_test_data(&test_index).await?;

    // Create a temporary file to hold the query JSON
    let query_file = format!("test_slice_query_{}.json", test_index);
    std::fs::write(&query_file, r#"{"query": {"term": {"tags": "even"}}}"#)?;

    // Run elasticdump-rs with slices=3 and a query to get only even-tagged documents
    run_elasticdump_command(&[
        "--input",
        &format!("{}/{}", ES_URL, test_index),
        "--output",
        &output_file,
        "--slices",
        "3", // Use 3 slices for parallel processing
        "--searchBody",
        &format!("@{}", &query_file),
        "--quiet", // Suppress progress output
    ])?;

    // Validate the output
    let file = File::open(&output_file)?;
    let reader = BufReader::new(file);
    let lines: Vec<String> = reader.lines().collect::<Result<_, _>>()?;

    // We should have 50 documents (IDs divisible by 2)
    assert_eq!(
        lines.len(),
        50,
        "Should have exported 50 documents with even tags"
    );

    // Verify all documents have the "even" tag
    for line in &lines {
        let doc: Value = serde_json::from_str(line)?;

        // Get the ID and verify it's even
        let id = doc["_source"]["id"].as_u64().unwrap();
        assert_eq!(id % 2, 0, "Document ID should be even");

        // Verify it has the "even" tag
        let tags = doc["_source"]["tags"].as_array().unwrap();
        assert!(
            tags.iter().any(|tag| tag.as_str().unwrap() == "even"),
            "Document should have the 'even' tag"
        );
    }

    // Cleanup
    cleanup(&test_index, &output_file).await?;
    std::fs::remove_file(&query_file)?;

    Ok(())
}

#[ignore = "benchmark-style integration test"]
#[tokio::test]
async fn test_slices_performance_comparison() -> Result<()> {
    let test_index = get_unique_test_index();
    let output_file_base = format!("test_slices_performance_output_{}", test_index);

    // Create an index with more documents for meaningful benchmarking
    // First set up the basic test data with 100 documents
    setup_test_data(&test_index).await?;

    // Then add more documents for a total of 10000
    let url = Url::parse(ES_URL)?;
    let conn_pool = SingleNodeConnectionPool::new(url.clone());
    let transport = TransportBuilder::new(conn_pool).build()?;
    let client = Elasticsearch::new(transport);

    println!("Adding 10000 more documents for performance testing...");
    // Replace manual loop with bulk insert helper
    insert_large_dataset_bulk(&client, &test_index, 101, 9900, 30, 1000).await?; // Add 9900 more docs, 30 words, 1000 bulk size

    // Refresh the index
    client
        .indices()
        .refresh(IndicesRefreshParts::Index(&[&test_index]))
        .send()
        .await?;

    // Run multiple tests with different slice configurations
    let slice_configs = [0, 2, 4, 8]; // 0 means no slices
    let mut results = Vec::new();

    for &slices in &slice_configs {
        let output_file = format!("{}_{}_slices.jsonl", output_file_base, slices);
        let formatted_url = format!("{}/{}", ES_URL, test_index);
        println!(
            "Running performance test with {} slices...",
            if slices == 0 {
                "no".to_string()
            } else {
                slices.to_string()
            }
        );

        let start_time = std::time::Instant::now();

        // Run with specified number of slices
        let slice_str = slices.to_string();
        let mut args = vec![
            "--input",
            &formatted_url,
            "--output",
            &output_file,
            "--limit",
            "50",      // Smaller batch size for more noticeable slicing effect
            "--quiet", // Suppress progress bar in tests
        ];

        if slices > 0 {
            args.push("--slices");
            args.push(&slice_str);
        }

        run_elasticdump_command(&args)?;

        let elapsed = start_time.elapsed();
        println!(
            "Test with {} slices completed in {:.2?}",
            if slices == 0 {
                "no".to_string()
            } else {
                slices.to_string()
            },
            elapsed
        );

        // Verify the document count
        let file = File::open(&output_file)?;
        let reader = BufReader::new(file);
        let line_count = reader.lines().count();

        assert_eq!(
            line_count, 10000,
            "Expected 10000 documents in the output file"
        );

        results.push((slices, elapsed));

        // Clean up the output file
        std::fs::remove_file(&output_file)?;
    }

    // Print comparative results
    println!("\nSliced scroll performance comparison:");
    println!("-----------------------------------");
    println!("Slices | Time      | Speedup");
    println!("-----------------------------------");

    let base_time = results[0].1; // Time with no slices

    for (slices, elapsed) in results {
        let speedup = base_time.as_secs_f64() / elapsed.as_secs_f64();
        println!(
            "{:6} | {:9.2?} | {:.2}x",
            if slices == 0 {
                "None".to_string()
            } else {
                slices.to_string()
            },
            elapsed,
            speedup
        );
    }
    println!("-----------------------------------");

    // Cleanup
    cleanup(&test_index, "").await?;

    Ok(())
}

#[tokio::test]
async fn test_basic_pit() -> Result<()> {
    // Get a unique test index and output file
    let test_index = get_unique_test_index();
    let output_file = format!("test_output_{}.jsonl", test_index);

    // Setup test data
    setup_test_data(&test_index).await?;

    // Run the elasticdump-rs command with PIT
    run_elasticdump_command(&[
        "--input",
        &format!("{}/{}", ES_URL, test_index),
        "--output",
        &output_file,
        "--searchType",
        "pit",
        "--pitKeepAlive",
        "1m",
        "--quiet", // Suppress progress bar in tests
    ])?;

    // Verify the output file exists and contains the correct data
    let file = File::open(&output_file)?;
    let reader = BufReader::new(file);
    let mut line_count = 0;

    for line in reader.lines() {
        let line = line?;
        let document: Value = serde_json::from_str(&line)?;

        assert!(
            document.is_object(),
            "Each line should be a valid JSON object"
        );
        assert!(
            document.get("_source").is_some(),
            "Each document should have a _source field"
        );
        let source = document["_source"].as_object().unwrap();
        assert!(
            source.contains_key("id"),
            "Each document should have an id in _source"
        );
        assert!(
            source.contains_key("name"),
            "Each document should have a name in _source"
        );
        assert!(
            source.contains_key("description"),
            "Each document should have a description in _source"
        );

        line_count += 1;
    }

    assert_eq!(line_count, 100, "Expected 100 documents in the output file");

    // Cleanup
    cleanup(&test_index, &output_file).await?;

    Ok(())
}

#[tokio::test]
async fn test_basic_pit_still_dumps_all_documents_with_streaming_extractor() -> Result<()> {
    let test_index = get_unique_test_index();
    let output_file = format!("test_pit_streaming_output_{}.jsonl", test_index);

    setup_test_data(&test_index).await?;

    run_elasticdump_command(&[
        "--input",
        &format!("{}/{}", ES_URL, test_index),
        "--output",
        &output_file,
        "--searchType",
        "pit",
        "--pitKeepAlive",
        "1m",
        "--limit",
        "10",
        "--quiet",
    ])?;

    let file = File::open(&output_file)?;
    let reader = BufReader::new(file);
    let mut line_count = 0;
    let mut saw_source = false;

    for line in reader.lines() {
        let line = line?;
        let parsed: Value = serde_json::from_str(&line)?;
        if parsed["_source"]["name"].as_str().is_some() {
            saw_source = true;
        }
        line_count += 1;
    }

    assert_eq!(line_count, 100, "Expected 100 documents in the output file");
    assert!(saw_source);

    cleanup(&test_index, &output_file).await?;
    Ok(())
}

#[tokio::test]
async fn test_pit_with_query() -> Result<()> {
    // Get a unique test index and output file
    let test_index = get_unique_test_index();
    let output_file = format!("test_output_{}.jsonl", test_index);

    // Setup test data
    setup_test_data(&test_index).await?;

    // Run the elasticdump-rs command with PIT and a query
    run_elasticdump_command(&[
        "--input",
        &format!("{}/{}", ES_URL, test_index),
        "--output",
        &output_file,
        "--searchType",
        "pit",
        "--pitKeepAlive",
        "1m",
        "--searchBody",
        r#"{"query":{"term":{"tags":"even"}}}"#,
        "--quiet", // Suppress progress bar in tests
    ])?;

    // Verify the output file exists and contains only even-tagged documents
    let file = File::open(&output_file)?;
    let reader = BufReader::new(file);
    let mut line_count = 0;

    for line in reader.lines() {
        let line = line?;
        let document: Value = serde_json::from_str(&line)?;

        // Verify this is an even-tagged document
        let tags = document["_source"]["tags"].as_array().unwrap();
        assert!(
            tags.contains(&Value::String("even".to_string())),
            "Document should have 'even' tag: {:?}",
            document
        );

        line_count += 1;
    }

    assert_eq!(
        line_count, 50,
        "Expected 50 documents (even tags) in the output file"
    );

    // Cleanup
    cleanup(&test_index, &output_file).await?;

    Ok(())
}

#[tokio::test]
async fn test_sliced_pit() -> Result<()> {
    // Get a unique test index and output file
    let test_index = get_unique_test_index();
    let output_file = format!("test_output_{}.jsonl", test_index);

    // Setup test data
    setup_test_data(&test_index).await?;

    // Run the elasticdump-rs command with PIT and slices
    run_elasticdump_command(&[
        "--input",
        &format!("{}/{}", ES_URL, test_index),
        "--output",
        &output_file,
        "--searchType",
        "pit",
        "--pitKeepAlive",
        "1m",
        "--slices",
        "4",       // Use 4 slices for parallel processing
        "--quiet", // Suppress progress bar in tests
    ])?;

    // Verify the output file exists and contains the correct data
    let file = File::open(&output_file)?;
    let reader = BufReader::new(file);
    let mut line_count = 0;
    let mut ids = Vec::new();

    for line in reader.lines() {
        let line = line?;
        let document: Value = serde_json::from_str(&line)?;

        assert!(
            document.is_object(),
            "Each line should be a valid JSON object"
        );

        // Extract and store ID to ensure we have all documents
        let id = document["_source"]["id"].as_i64().unwrap();
        ids.push(id);

        line_count += 1;
    }

    // We should have exactly 100 documents
    assert_eq!(line_count, 100, "Expected 100 documents in the output file");

    // Verify we got all IDs (1-100)
    ids.sort();
    for i in 1..=100 {
        assert!(
            ids.contains(&i64::from(i)),
            "Missing ID {} in the results",
            i
        );
    }

    // Cleanup
    cleanup(&test_index, &output_file).await?;

    Ok(())
}

#[ignore = "benchmark-style integration test"]
#[tokio::test]
async fn test_compare_scroll_vs_pit() -> Result<()> {
    // Get a unique test index and output files
    let test_index = get_unique_test_index();
    let scroll_output = format!("test_output_scroll_{}.jsonl", test_index);
    let pit_output = format!("test_output_pit_{}.jsonl", test_index);

    // Setup more test data for a meaningful performance test
    setup_test_data(&test_index).await?;

    // Add more documents
    let url = Url::parse(ES_URL)?;
    let conn_pool = SingleNodeConnectionPool::new(url.clone());
    let transport = TransportBuilder::new(conn_pool).build()?;
    let client = Elasticsearch::new(transport);

    println!("Adding more documents for comparison testing...");
    // Add 900 more documents (total 1000)
    insert_large_dataset_bulk(&client, &test_index, 101, 900, 40, 500).await?; // Add 900 more docs, 40 words, 500 bulk size

    // Refresh the index
    client
        .indices()
        .refresh(IndicesRefreshParts::Index(&[&test_index]))
        .send()
        .await?;

    // Test with Scroll API
    println!("Running dump with Scroll API...");
    let scroll_start = std::time::Instant::now();

    run_elasticdump_command(&[
        "--input",
        &format!("{}/{}", ES_URL, test_index),
        "--output",
        &scroll_output,
        "--searchType",
        "scroll",
        "--scrollTime",
        "1m",
        "--limit",
        "100",
        "--quiet",
    ])?;

    let scroll_elapsed = scroll_start.elapsed();
    println!("Scroll API completed in {:.2?}", scroll_elapsed);

    // Test with PIT API
    println!("Running dump with PIT API...");
    let pit_start = std::time::Instant::now();

    run_elasticdump_command(&[
        "--input",
        &format!("{}/{}", ES_URL, test_index),
        "--output",
        &pit_output,
        "--searchType",
        "pit",
        "--pitKeepAlive",
        "1m",
        "--limit",
        "100",
        "--quiet",
    ])?;

    let pit_elapsed = pit_start.elapsed();
    println!("PIT API completed in {:.2?}", pit_elapsed);

    // Verify both outputs have the same number of documents
    let scroll_count = std::fs::read_to_string(&scroll_output)?.lines().count();

    let pit_count = std::fs::read_to_string(&pit_output)?.lines().count();

    assert_eq!(
        scroll_count, 1000,
        "Scroll output should contain 1000 documents"
    );
    assert_eq!(pit_count, 1000, "PIT output should contain 1000 documents");

    // Report performance comparison
    println!("Performance comparison:");
    println!(
        "  Scroll API: {:.2?} ({:.0} docs/sec)",
        scroll_elapsed,
        1000.0 / scroll_elapsed.as_secs_f64()
    );
    println!(
        "  PIT API:    {:.2?} ({:.0} docs/sec)",
        pit_elapsed,
        1000.0 / pit_elapsed.as_secs_f64()
    );
    println!(
        "  Difference: {:.1}%",
        (1.0 - (pit_elapsed.as_secs_f64() / scroll_elapsed.as_secs_f64())) * 100.0
    );

    // Cleanup
    cleanup(&test_index, &scroll_output).await?;
    let _ = std::fs::remove_file(&pit_output); // Remove the second output file

    Ok(())
}

#[tokio::test]
async fn test_sliced_pit_large_dataset() -> Result<()> {
    // Get a unique test index and output file
    let test_index = get_unique_test_index();
    let output_file = format!("test_output_{}.jsonl", test_index);

    // Setup test data
    setup_test_data(&test_index).await?;

    // Add more documents
    let url = Url::parse(ES_URL)?;
    let conn_pool = SingleNodeConnectionPool::new(url.clone());
    let transport = TransportBuilder::new(conn_pool).build()?;
    let client = Elasticsearch::new(transport);

    println!("Adding more documents for large sliced PIT test...");
    // Add 400 more documents (total 500)
    insert_large_dataset_bulk(&client, &test_index, 101, 400, 25, 200).await?; // Add 400 more docs, 25 words, 200 bulk size

    // Refresh the index
    client
        .indices()
        .refresh(IndicesRefreshParts::Index(&[&test_index]))
        .send()
        .await?;

    // Run the elasticdump-rs command with PIT and slices
    run_elasticdump_command(&[
        "--input",
        &format!("{}/{}", ES_URL, test_index),
        "--output",
        &output_file,
        "--searchType",
        "pit",
        "--pitKeepAlive",
        "1m",
        "--slices",
        "4", // Use 4 slices for parallel processing
        "--limit",
        "50",      // Smaller batch size to test multiple batches per slice
        "--quiet", // Suppress progress bar in tests
    ])?;

    // Verify the output file exists and contains the correct data
    let file = File::open(&output_file)?;
    let reader = BufReader::new(file);
    let mut line_count = 0;
    let mut ids = Vec::new();

    for line in reader.lines() {
        let line = line?;
        let document: Value = serde_json::from_str(&line)?;

        assert!(
            document.is_object(),
            "Each line should be a valid JSON object"
        );

        // Extract and store ID to ensure we have all documents
        let id = document["_source"]["id"].as_i64().unwrap();
        ids.push(id);

        line_count += 1;
    }

    // We should have exactly 500 documents
    assert_eq!(line_count, 500, "Expected 500 documents in the output file");

    // Verify we got all IDs (1-500)
    ids.sort();
    for i in 1..=500 {
        assert!(
            ids.contains(&i64::from(i)),
            "Missing ID {} in the results",
            i
        );
    }

    // Cleanup
    cleanup(&test_index, &output_file).await?;

    Ok(())
}

#[cfg(feature = "manual_test")]
#[tokio::test]
async fn test_create_data_for_manual_testing() -> Result<()> {
    // Get a unique test index
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let test_index = format!("{}_manual_{}", TEST_INDEX_PREFIX, timestamp);

    // Setup test data
    setup_test_data(&test_index).await?;

    // Add more documents to have a more realistic test dataset
    let url = Url::parse(ES_URL)?;
    let conn_pool = SingleNodeConnectionPool::new(url.clone());
    let transport = TransportBuilder::new(conn_pool).build()?;
    let client = Elasticsearch::new(transport);

    println!("Adding more documents for manual testing...");
    // Add 1,000,000 more documents (in addition to the 100 we already have)
    insert_large_dataset_bulk(&client, &test_index, 101, 1000000, 50, 5000).await?; // 50 words, 5000 bulk size

    // Refresh the index
    client
        .indices()
        .refresh(IndicesRefreshParts::Index(&[&test_index]))
        .send()
        .await?;

    // Tell the user the index name so they can use it for manual testing
    println!("\n=============================================");
    println!("Manual test data created in index: {}", test_index);
    println!("Run elasticdump-rs with:");
    println!(
        "cargo run -- --input {}/{} --output <your_output_file>",
        ES_URL, test_index
    );
    println!("=============================================\n");

    // Don't clean up - leave the data for manual testing
    Ok(())
}

#[tokio::test]
async fn test_dump_correctness() -> Result<()> {
    // Get a unique test index and output file
    let test_index = get_unique_test_index();
    let output_file = format!("test_output_correctness_{}.jsonl", test_index);

    // Setup test data (100 documents)
    setup_test_data(&test_index).await?;

    // Run the elasticdump-rs command for a basic dump
    run_elasticdump_command(&[
        "--input",
        &format!("{}/{}", ES_URL, test_index),
        "--output",
        &output_file,
        "--quiet", // Suppress progress bar in tests
        "--limit",
        "10",
        "--overwrite", // Add overwrite flag
    ])?;

    // --- Verification Phase ---
    let file = File::open(&output_file)?;
    let reader = BufReader::new(file);
    let mut line_count = 0;
    let mut dumped_ids = HashSet::new();
    let mut dumped_docs = HashMap::new();

    println!("Verifying output file: {}", output_file);

    // 1. Read output file, check for duplicates, store docs
    for line_result in reader.lines() {
        let line = line_result?;
        if line.trim().is_empty() {
            continue; // Skip empty lines if any
        }
        let document: Value = serde_json::from_str(&line)?;

        assert!(document.is_object(), "Line is not a JSON object: {}", line);
        let doc_id = document["_id"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Document missing _id: {}", line))?
            .to_string();
        let source = document["_source"].clone();
        assert!(!source.is_null(), "Document missing _source: {}", line);

        // Check for duplicate IDs
        if !dumped_ids.insert(doc_id.clone()) {
            return Err(anyhow::anyhow!("Duplicate document ID found: {}", doc_id));
        }

        dumped_docs.insert(doc_id, source);
        line_count += 1;
    }

    // 2. Verify total document count
    assert_eq!(
        line_count, 100,
        "Expected 100 documents in the output file, found {}",
        line_count
    );
    assert_eq!(
        dumped_ids.len(),
        100,
        "Expected 100 unique document IDs, found {}",
        dumped_ids.len()
    );

    println!("Verified {} unique documents in output file.", line_count);

    // 3. Connect to Elasticsearch and verify each document's content
    let url = Url::parse(ES_URL)?;
    let conn_pool = SingleNodeConnectionPool::new(url.clone());
    let transport = TransportBuilder::new(conn_pool).build()?;
    let client = Elasticsearch::new(transport);

    println!(
        "Verifying document content against Elasticsearch index: {}",
        test_index
    );

    // No need to create a new runtime, just await directly in the async test fn
    for (doc_id, dumped_source) in &dumped_docs {
        let get_response = client
            .get(elasticsearch::GetParts::IndexId(&test_index, doc_id))
            .send()
            .await?;

        if !get_response.status_code().is_success() {
            return Err(anyhow::anyhow!(
                "Failed to retrieve document {} from Elasticsearch: {:?}",
                doc_id,
                get_response.text().await?
            ));
        }

        let es_doc: Value = get_response.json().await?;
        let es_source = es_doc
            .get("_source")
            .ok_or_else(|| anyhow::anyhow!("Elasticsearch document {} missing _source", doc_id))?;

        // Compare the source from the file with the source from ES
        // Note: serde_json::Value comparison handles field order differences
        if es_source != dumped_source {
            return Err(anyhow::anyhow!(
                "Mismatch for document ID {}:\nDumped: {}\nActual: {}",
                doc_id,
                serde_json::to_string_pretty(dumped_source)?,
                serde_json::to_string_pretty(es_source)?
            ));
        }
    }

    println!(
        "Successfully verified content of all {} documents.",
        line_count
    );

    // Cleanup
    cleanup(&test_index, &output_file).await?;

    Ok(())
}

// ===========================================================================
// Task 6: failure-path, empty/edge, adversarial, SIGINT, and mock-ES coverage
//
// Each test below guards a specific Task 1-5 behavior; the comment on each says
// what a regression would look like (i.e. how the test fails if the guard is
// removed). The two mock partial-response tests additionally assert
// `request_count()` to prove the mocked code path was actually driven.
// ===========================================================================

/// A bare Elasticsearch client against the live test node.
async fn es_client() -> Result<Elasticsearch> {
    let url = Url::parse(ES_URL)?;
    let conn_pool = SingleNodeConnectionPool::new(url);
    let transport = TransportBuilder::new(conn_pool).build()?;
    Ok(Elasticsearch::new(transport))
}

/// Create a fresh, empty index (1 shard, 0 replicas), replacing any prior one.
async fn create_empty_index(test_index: &str) -> Result<()> {
    wait_for_elasticsearch().await?;
    let client = es_client().await?;
    let _ = client
        .indices()
        .delete(IndicesDeleteParts::Index(&[test_index]))
        .send()
        .await;
    let response = client
        .indices()
        .create(IndicesCreateParts::Index(test_index))
        .body(json!({
            "settings": { "number_of_shards": 1, "number_of_replicas": 0 }
        }))
        .send()
        .await?;
    assert!(
        response.status_code().is_success(),
        "Failed to create index {}: {:?}",
        test_index,
        response.text().await?
    );
    client
        .indices()
        .refresh(IndicesRefreshParts::Index(&[test_index]))
        .send()
        .await?;
    Ok(())
}

/// Seed `num_docs` minimal documents (auto ids) into a fresh index and verify
/// the count landed.
async fn seed_bulk_docs(test_index: &str, num_docs: u32) -> Result<()> {
    create_empty_index(test_index).await?;
    let client = es_client().await?;
    // 3 words of lipsum keeps documents tiny so large seeds stay fast.
    insert_large_dataset_bulk(&client, test_index, 1, num_docs, 3, 1000).await?;
    client
        .indices()
        .refresh(IndicesRefreshParts::Index(&[test_index]))
        .send()
        .await?;

    let search = client
        .search(SearchParts::Index(&[test_index]))
        .body(json!({ "size": 0, "track_total_hits": true }))
        .send()
        .await?;
    let body: Value = search.json().await?;
    let total = body["hits"]["total"]["value"].as_u64().unwrap();
    assert_eq!(
        total, num_docs as u64,
        "Expected {} seeded docs, found {}",
        num_docs, total
    );
    Ok(())
}

/// Count non-EOF newline-delimited lines in an output file.
fn count_lines(path: &str) -> Result<usize> {
    let file = File::open(path)?;
    Ok(BufReader::new(file).lines().count())
}

/// Collect any `.{file_name}.part-*` staging siblings left in `dir`.
fn staging_siblings(dir: &std::path::Path, file_name: &str) -> Vec<std::path::PathBuf> {
    let prefix = format!(".{file_name}.part-");
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.starts_with(&prefix))
                .unwrap_or(false)
        })
        .collect()
}

/// Run the binary to completion off the async runtime, capturing its output.
async fn run_binary(args: Vec<String>) -> std::process::Output {
    tokio::task::spawn_blocking(move || {
        binary_command()
            .args(args)
            .output()
            .expect("failed to spawn elasticdump-rs")
    })
    .await
    .expect("subprocess task panicked")
}

/// `--limit 0` must be rejected at clap parse time; no ES interaction. Guards
/// finding 30: a regression that dropped the `value_parser` would exit 0 here.
#[test]
fn test_limit_zero_is_rejected() {
    let output = Command::new(elasticdump_binary())
        .args([
            "--input",
            "http://localhost:9200/whatever",
            "--output",
            "$",
            "--limit",
            "0",
        ])
        .output()
        .expect("failed to spawn elasticdump-rs");

    assert!(!output.status.success(), "--limit 0 must be rejected");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("greater than 0"),
        "stderr should explain the limit must be > 0, got: {stderr}"
    );
}

/// `--slices 1` should warn and behave exactly like an unsliced dump (ES rejects
/// slice.max=1). A regression that passed slices=1 straight to ES would make ES
/// return an error and the dump would exit non-zero.
#[tokio::test]
async fn test_slices_one_runs_unsliced() -> Result<()> {
    for search_type in ["scroll", "pit"] {
        let test_index = get_unique_test_index();
        let output_path = unique_output_path(&format!("slices_one_{search_type}"));
        let output_file = output_path.to_str().unwrap();

        setup_test_data(&test_index).await?;

        let output = run_elasticdump_command_capture(&[
            "--input",
            &format!("{}/{}", ES_URL, test_index),
            "--output",
            output_file,
            "--searchType",
            search_type,
            "--slices",
            "1",
            "--quiet",
        ])?;
        assert!(
            output.status.success(),
            "--slices 1 ({search_type}) should exit 0; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            count_lines(output_file)?,
            100,
            "expected 100 docs with --slices 1 ({search_type})"
        );

        cleanup(&test_index, output_file).await?;
    }
    Ok(())
}

/// A failed dump must leave an existing destination byte-for-byte intact and
/// leave no `.part-` staging sibling. Guards finding 32/33 (staged output): a
/// regression that wrote in place would clobber the sentinel.
#[tokio::test]
async fn test_failed_dump_preserves_existing_output() -> Result<()> {
    wait_for_elasticsearch().await?;

    let dir = tempfile::tempdir()?;
    let dest = dir.path().join("existing_output.jsonl");
    let sentinel = "sentinel content that must survive\n";
    std::fs::write(&dest, sentinel)?;

    let missing_index = format!("elasticdump_rs_missing_{}", std::process::id());
    let output = run_elasticdump_command_capture(&[
        "--input",
        &format!("{}/{}", ES_URL, missing_index),
        "--output",
        dest.to_str().unwrap(),
        "--overwrite",
        "--quiet",
    ])?;

    assert!(
        !output.status.success(),
        "dump against a missing index must fail; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::read_to_string(&dest)?,
        sentinel,
        "existing destination must be untouched after a failed dump"
    );
    let leftovers = staging_siblings(dir.path(), "existing_output.jsonl");
    assert!(
        leftovers.is_empty(),
        "no staging sibling should remain: {leftovers:?}"
    );
    Ok(())
}

/// A dump of an empty index succeeds and produces an empty output file (both
/// scroll and PIT). A regression that errored on zero hits would exit non-zero.
#[tokio::test]
async fn test_empty_index_dump() -> Result<()> {
    for search_type in ["scroll", "pit"] {
        let test_index = get_unique_test_index();
        let output_path = unique_output_path(&format!("empty_{search_type}"));
        let output_file = output_path.to_str().unwrap();

        create_empty_index(&test_index).await?;

        let output = run_elasticdump_command_capture(&[
            "--input",
            &format!("{}/{}", ES_URL, test_index),
            "--output",
            output_file,
            "--searchType",
            search_type,
            "--quiet",
        ])?;
        assert!(
            output.status.success(),
            "empty index dump ({search_type}) should exit 0; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            std::path::Path::new(output_file).exists(),
            "output file should exist for an empty dump ({search_type})"
        );
        assert_eq!(
            count_lines(output_file)?,
            0,
            "empty index should yield 0 lines ({search_type})"
        );

        cleanup(&test_index, output_file).await?;
    }
    Ok(())
}

/// A high-contention pipeline (more slices than workers, buffer size 1, small
/// batch) must still emit every document exactly once. Guards the pipeline core
/// (finding-set for Task 4): a lost-wakeup/backpressure regression drops docs.
#[tokio::test]
async fn test_backpressure_matrix() -> Result<()> {
    for search_type in ["scroll", "pit"] {
        let test_index = get_unique_test_index();
        let output_path = unique_output_path(&format!("backpressure_{search_type}"));
        let output_file = output_path.to_str().unwrap();

        seed_bulk_docs(&test_index, 5_000).await?;

        let output = run_elasticdump_command_capture(&[
            "--input",
            &format!("{}/{}", ES_URL, test_index),
            "--output",
            output_file,
            "--searchType",
            search_type,
            "--slices",
            "3",
            "--workers",
            "2",
            "--bufferSize",
            "1",
            "--limit",
            "100",
            "--quiet",
        ])?;
        assert!(
            output.status.success(),
            "backpressure matrix ({search_type}) should exit 0; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            count_lines(output_file)?,
            5_000,
            "expected exactly 5000 docs ({search_type})"
        );

        cleanup(&test_index, output_file).await?;
    }
    Ok(())
}

/// With progress bars ENABLED (no --quiet) and `--output $`, stdout must be pure
/// JSONL and all progress noise must stay on stderr. Guards finding 31: a
/// regression that drew progress to stdout would break the JSON parse below.
#[tokio::test]
async fn test_stdout_is_pure_jsonl_with_progress_enabled() -> Result<()> {
    let test_index = get_unique_test_index();
    seed_bulk_docs(&test_index, 50).await?;

    // Deliberately omit --quiet so progress bars are active.
    let output = run_elasticdump_command_capture(&[
        "--input",
        &format!("{}/{}", ES_URL, test_index),
        "--output",
        "$",
    ])?;
    assert!(
        output.status.success(),
        "dump should exit 0; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let stdout = String::from_utf8(output.stdout)?;
    let mut line_count = 0;
    for line in stdout.lines() {
        if line.is_empty() {
            continue;
        }
        let doc: Value = serde_json::from_str(line)
            .map_err(|e| anyhow::anyhow!("stdout line is not valid JSON: {line:?}: {e}"))?;
        assert!(
            doc["_source"].get("id").is_some(),
            "each stdout doc should carry _source.id, got: {line}"
        );
        line_count += 1;
    }
    assert_eq!(line_count, 50, "expected exactly 50 JSON lines on stdout");

    cleanup(&test_index, "").await?;
    Ok(())
}

/// Documents whose `_source` carries unicode, JSON escapes, and keys that
/// collide with the extractor's metadata pointers must round-trip byte-exactly
/// (both scroll and PIT). Guards the streaming extractor (Task 2/perf fuse): a
/// regression that mis-parsed raw hit bytes would corrupt these sources.
#[tokio::test]
async fn test_adversarial_documents_roundtrip() -> Result<()> {
    // Values are nested under a mapping-disabled `payload` object so ES stores
    // _source verbatim without dynamic-mapping conflicts (mixed-type arrays,
    // underscore-prefixed keys). The extractor still sees the tricky raw bytes.
    let payloads: Vec<(String, Value)> = vec![
        (
            "adv-1".to_string(),
            json!({
                "text": "quote\" backslash\\ newline\n tab\t end",
                "unicode": "é中文😀🚀 Ω≈ç√",
                "hits": { "nested": true, "list": [1, 2, 3] },
                "sort": ["a", "b", "c"],
                "pit_id": ["decoy", "array"],
                "_scroll_id": "decoy-scroll"
            }),
        ),
        (
            "adv-2".to_string(),
            json!({
                "raw": "中文😀",
                "message": "line1\nline2 with \"quotes\" and \\slash",
                "hits": { "deep": { "deeper": "值" } },
                "empty_obj": {},
                "empty_arr": []
            }),
        ),
    ];

    for search_type in ["scroll", "pit"] {
        let test_index = get_unique_test_index();
        let output_path = unique_output_path(&format!("adversarial_{search_type}"));
        let output_file = output_path.to_str().unwrap();

        wait_for_elasticsearch().await?;
        let client = es_client().await?;
        let _ = client
            .indices()
            .delete(IndicesDeleteParts::Index(&[test_index.as_str()]))
            .send()
            .await;
        let create = client
            .indices()
            .create(IndicesCreateParts::Index(&test_index))
            .body(json!({
                "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
                "mappings": { "properties": { "payload": { "type": "object", "enabled": false } } }
            }))
            .send()
            .await?;
        assert!(
            create.status_code().is_success(),
            "create adversarial index failed: {:?}",
            create.text().await?
        );

        let mut expected: HashMap<String, Value> = HashMap::new();
        for (id, payload) in &payloads {
            let source = json!({ "payload": payload.clone() });
            let resp = client
                .index(elasticsearch::IndexParts::IndexId(&test_index, id))
                .body(source.clone())
                .send()
                .await?;
            assert!(
                resp.status_code().is_success(),
                "index {id} failed: {:?}",
                resp.text().await?
            );
            expected.insert(id.clone(), source);
        }
        client
            .indices()
            .refresh(IndicesRefreshParts::Index(&[test_index.as_str()]))
            .send()
            .await?;

        let output = run_elasticdump_command_capture(&[
            "--input",
            &format!("{}/{}", ES_URL, test_index),
            "--output",
            output_file,
            "--searchType",
            search_type,
            "--quiet",
        ])?;
        assert!(
            output.status.success(),
            "adversarial dump ({search_type}) should exit 0; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let file = File::open(output_file)?;
        let mut dumped: HashMap<String, Value> = HashMap::new();
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let doc: Value = serde_json::from_str(&line)?;
            let id = doc["_id"].as_str().unwrap().to_string();
            dumped.insert(id, doc["_source"].clone());
        }

        assert_eq!(
            dumped.len(),
            expected.len(),
            "doc count mismatch ({search_type})"
        );
        for (id, source) in &expected {
            let got = dumped
                .get(id)
                .unwrap_or_else(|| panic!("missing dumped doc {id} ({search_type})"));
            assert_eq!(got, source, "_source mismatch for {id} ({search_type})");
        }

        cleanup(&test_index, output_file).await?;
    }
    Ok(())
}

/// SIGINT during a running dump must cancel it, remove the staging file, exit
/// non-zero, and never create the destination. Guards Task 5 signal handling.
#[cfg(unix)]
#[tokio::test]
async fn test_sigint_cleans_up_staging() -> Result<()> {
    let test_index = get_unique_test_index();
    // 20k docs at --limit 200 means ~100 sequential scroll round-trips: long
    // enough that the process is reliably mid-dump when we signal it.
    seed_bulk_docs(&test_index, 20_000).await?;

    let dir = tempfile::tempdir()?;
    let dest = dir.path().join("sigint_output.jsonl");
    let dest_str = dest.to_str().unwrap().to_string();

    let mut child = binary_command()
        .args([
            "--input",
            &format!("{}/{}", ES_URL, test_index),
            "--output",
            &dest_str,
            "--limit",
            "200",
            "--quiet",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    // The staging file is created before the first ES request; poll for it with
    // a bound so a never-starting dump fails instead of hanging.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut staging_seen = false;
    loop {
        if !staging_siblings(dir.path(), "sigint_output.jsonl").is_empty() {
            staging_seen = true;
            break;
        }
        if let Some(status) = child.try_wait()? {
            panic!("dump exited early (status {status:?}) before staging appeared");
        }
        if std::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(staging_seen, "staging .part- file never appeared");

    let pid = child.id();
    let kill_status = Command::new("kill")
        .arg("-INT")
        .arg(pid.to_string())
        .status()?;
    assert!(kill_status.success(), "kill -INT failed");

    let exit_deadline = std::time::Instant::now() + Duration::from_secs(30);
    let exit_status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if std::time::Instant::now() >= exit_deadline {
            let _ = child.kill();
            panic!("dump did not exit within 30s after SIGINT");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };

    assert!(
        !exit_status.success(),
        "an interrupted dump must exit non-zero"
    );
    assert!(
        staging_siblings(dir.path(), "sigint_output.jsonl").is_empty(),
        "staging file must be cleaned up after interrupt"
    );
    assert!(
        !dest.exists(),
        "destination must not exist after an interrupted dump"
    );

    let _ = es_client()
        .await?
        .indices()
        .delete(IndicesDeleteParts::Index(&[test_index.as_str()]))
        .send()
        .await;
    Ok(())
}

/// Mock ES: a 200 response reporting a failed shard must fail the dump. Before
/// Task 2 any 200 was treated as a complete batch, so this would have exited 0
/// while silently dropping the failed shard's documents.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_shard_failure_fails_dump() -> Result<()> {
    let mock = MockEs::serve(vec![CannedResponse::ok(
        r#"{"_scroll_id":"s1","_shards":{"total":2,"successful":1,"skipped":0,"failed":1},"hits":{"total":{"value":10,"relation":"eq"},"hits":[{"_id":"1","_source":{"a":1}}]}}"#,
    )])
    .await;

    let output = run_binary(vec![
        "--input".into(),
        mock.input_url("idx"),
        "--output".into(),
        "$".into(),
        "--quiet".into(),
    ])
    .await;

    assert!(
        !output.status.success(),
        "a failed-shard response must fail the dump"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("failed shard"),
        "stderr should mention failed shard(s), got: {stderr}"
    );
    // Prove the initial search actually reached the mock (drives the code path).
    assert!(
        mock.request_count() >= 1,
        "mock should have served the initial search, got {}",
        mock.request_count()
    );
    Ok(())
}

/// Mock ES: a 200 response with timed_out=true must fail the dump. Before Task 2
/// this partial result would have been accepted as complete (exit 0).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_timed_out_fails_dump() -> Result<()> {
    let mock = MockEs::serve(vec![CannedResponse::ok(
        r#"{"_scroll_id":"s1","timed_out":true,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":10,"relation":"eq"},"hits":[{"_id":"1","_source":{"a":1}}]}}"#,
    )])
    .await;

    let output = run_binary(vec![
        "--input".into(),
        mock.input_url("idx"),
        "--output".into(),
        "$".into(),
        "--quiet".into(),
    ])
    .await;

    assert!(
        !output.status.success(),
        "a timed_out response must fail the dump"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("timed_out"),
        "stderr should mention timed_out, got: {stderr}"
    );
    assert!(
        mock.request_count() >= 1,
        "mock should have served the initial search, got {}",
        mock.request_count()
    );
    Ok(())
}

/// Mock ES: the idempotent initial search retries a 429 and recovers. Guards the
/// retry policy (Task 5). A regression that stopped retrying idempotent 429s
/// would exit non-zero on the first response.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_retry_recovers_from_429() -> Result<()> {
    let mock = MockEs::serve(vec![
        CannedResponse::new(429, r#"{"error":"rate limited"}"#),
        CannedResponse::ok(
            r#"{"_scroll_id":"s1","timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}"#,
        ),
        // clear_scroll issued at slice end for the returned scroll id.
        CannedResponse::ok(r#"{"succeeded":true,"num_freed":1}"#),
    ])
    .await;

    let output = run_binary(vec![
        "--input".into(),
        mock.input_url("idx"),
        "--output".into(),
        "$".into(),
        "--retryDelay".into(),
        "10".into(),
        "--quiet".into(),
    ])
    .await;

    assert!(
        output.status.success(),
        "429 then a good empty response should exit 0; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        mock.request_count() >= 2,
        "retry should have issued at least a second request, got {}",
        mock.request_count()
    );
    Ok(())
}

/// Mock ES: with --retryAttempts 0 the first 429 is fatal and no retry is made.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_retry_attempts_zero_disables_retry() -> Result<()> {
    let mock = MockEs::serve(vec![CannedResponse::new(
        429,
        r#"{"error":"rate limited"}"#,
    )])
    .await;

    let output = run_binary(vec![
        "--input".into(),
        mock.input_url("idx"),
        "--output".into(),
        "$".into(),
        "--retryAttempts".into(),
        "0".into(),
        "--quiet".into(),
    ])
    .await;

    assert!(
        !output.status.success(),
        "retryAttempts=0 must not retry a 429"
    );
    assert_eq!(
        mock.request_count(),
        1,
        "exactly one request should have been made, got {}",
        mock.request_count()
    );
    Ok(())
}
