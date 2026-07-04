use anyhow::Context;
use anyhow::Result;
use log::debug;
use sonic_rs::JsonValueMutTrait;
use sonic_rs::JsonValueTrait;
use sonic_rs::{Value, json};
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
            sonic_rs::from_str(&content).with_context(|| {
                format!("Failed to parse search body JSON from file: {}", file_path)
            })?
        } else {
            debug!("Parsing search body from command line parameter");
            sonic_rs::from_str(search_body_str)
                .context("Failed to parse search body JSON string")?
        }
    } else {
        debug!("No search body provided, using empty query");
        json!({})
    };

    // Ensure we have a JSON object, wrap if necessary
    let mut final_search_body = if search_body_json.get_type() != sonic_rs::JsonType::Object {
        debug!("Search body is not an object, wrapping in a query object");
        log::warn!(
            "searchBody is not a JSON object; using it as {{\"query\": {}}} — this is almost certainly not the intended query",
            sonic_rs::to_string(&search_body_json).unwrap_or_default()
        );
        json!({ "query": search_body_json })
    } else {
        search_body_json
    };

    // Add size parameter to search body
    let scroll_size = args.limit;
    let search_body_obj = final_search_body.as_object_mut().unwrap(); // Safe because we ensured it's an object
    if let Some(user_size) = search_body_obj.get(&"size") {
        if user_size.as_u64() != Some(scroll_size as u64) {
            log::warn!(
                "Overriding searchBody size {} with --limit {}",
                sonic_rs::to_string(user_size).unwrap_or_default(),
                scroll_size
            );
        }
    }
    search_body_obj.insert(&"size", json!(scroll_size));

    debug!(
        "Final search body: {}",
        sonic_rs::to_string(&search_body_obj).unwrap_or_default()
    );

    Ok(final_search_body)
}

#[cfg(test)]
mod tests {
    use super::prepare_search_body;
    use crate::cli::Cli;
    use clap::Parser;
    use sonic_rs::json;

    fn cli_with_search_body(search_body: &str, limit: &str) -> Cli {
        Cli::parse_from([
            "elasticdump-rs",
            "--input",
            "http://localhost:9200/test_index",
            "--output",
            "$",
            "--searchBody",
            search_body,
            "--limit",
            limit,
        ])
    }

    #[tokio::test]
    async fn warns_but_overrides_user_size() {
        let cli = cli_with_search_body(r#"{"query":{"match_all":{}},"size":5}"#, "100");

        let body = prepare_search_body(&cli).await.unwrap();

        assert_eq!(body["size"], json!(100));
        assert_eq!(body["query"], json!({"match_all": {}}));
    }

    #[tokio::test]
    async fn file_parse_errors_include_file_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("body.json");
        std::fs::write(&path, "{not json").unwrap();

        let cli = cli_with_search_body(&format!("@{}", path.display()), "100");

        let error = prepare_search_body(&cli).await.unwrap_err();
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains(&path.display().to_string()),
            "parse error must name the file: {rendered}"
        );
    }

    #[tokio::test]
    async fn wraps_non_object_search_body_in_query() {
        let cli = cli_with_search_body(r#""bare-string""#, "10");

        let body = prepare_search_body(&cli).await.unwrap();

        assert_eq!(body["query"], json!("bare-string"));
        assert_eq!(body["size"], json!(10));
    }
}
