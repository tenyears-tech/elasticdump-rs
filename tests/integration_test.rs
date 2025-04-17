use anyhow::Result;
use elasticsearch::{
    Elasticsearch, SearchParts,
    http::request::JsonBody,
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
    indices::{IndicesCreateParts, IndicesDeleteParts, IndicesRefreshParts},
};
use serde_json::{Value, json};
use std::{
    fs::File,
    io::{BufRead, BufReader},
    process::{Command, Stdio},
    sync::atomic::{AtomicU32, Ordering},
    thread,
    time::Duration,
};
use url::Url;

const ES_URL: &str = "http://localhost:9200";
const TEST_INDEX_PREFIX: &str = "elasticdump_rs_test";

// Static counter to ensure unique index names for parallel test execution
static INDEX_COUNTER: AtomicU32 = AtomicU32::new(0);

// Helper function to get a unique test index name
fn get_unique_test_index() -> String {
    let counter = INDEX_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}_{}", TEST_INDEX_PREFIX, counter)
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
                thread::sleep(Duration::from_secs(2));
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
        .refresh(IndicesRefreshParts::Index(&[&test_index]))
        .send()
        .await?;

    Ok(())
}

// Run the elasticdump-rs command
fn run_elasticdump_command(args: &[&str]) -> Result<()> {
    let status = Command::new("cargo")
        .args(["run", "--"])
        .args(args)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()?;

    assert!(status.success(), "elasticdump-rs command failed");

    Ok(())
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
    let output = Command::new("cargo")
        .args(["run", "--"])
        .args(&[
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
            id >= 20 && id <= 70,
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
    let status = Command::new("cargo")
        .args(["run", "--"])
        .args(&[
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
