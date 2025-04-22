use anyhow::Result;
use log::debug;
use serde_json::{json, Value};
use tokio::fs;

use crate::cli::Cli;

/// Prepare search body JSON from CLI arguments
pub async fn prepare_search_body(args: &Cli) -> Result<Value> {
    let search_body_json = if let Some(search_body_str) = &args.search_body {
        if search_body_str.starts_with('@') {
            let file_path = &search_body_str[1..];
            debug!("Loading search body from file: {}", file_path);
            let content = fs::read_to_string(file_path)
                .await
                .with_context(|| format!("Failed to read search body from file: {}", file_path))?;
            serde_json::from_str(&content)?
        } else {
            debug!("Parsing search body from command line parameter");
            serde_json::from_str(search_body_str)
                .context("Failed to parse search body JSON string")?
        }
    } else {
        debug!("No search body provided, using empty query");
        json!({})
    };

    // Ensure we have a JSON object, wrap if necessary
    let mut final_search_body = if !search_body_json.is_object() {
        debug!("Search body is not an object, wrapping in a query object");
        json!({ "query": search_body_json })
    } else {
        search_body_json
    };

    // Add size parameter to search body
    let scroll_size = args.limit;
    let search_body_obj = final_search_body.as_object_mut().unwrap(); // Safe because we ensured it's an object
    search_body_obj.insert("size".to_string(), json!(scroll_size));
    
    debug!(
        "Final search body: {}",
        serde_json::to_string(&search_body_obj).unwrap_or_default()
    );

    Ok(final_search_body)
}

use anyhow::Context; 
