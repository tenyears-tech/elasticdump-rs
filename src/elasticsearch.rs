use anyhow::{Context, Result, anyhow};
use base64::prelude::*;
use elasticsearch::{
    Elasticsearch,
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
};
use http::header::{ACCEPT_ENCODING, AUTHORIZATION, HeaderMap, HeaderValue};
use url::Url;

use crate::cli::Cli;

fn path_segments_without_empty(url: &Url) -> Vec<&str> {
    url.path_segments()
        .map(|segments| segments.filter(|segment| !segment.is_empty()).collect())
        .unwrap_or_default()
}

/// Parse the input URL and extract host URL, index name, and auth credentials
pub fn parse_input_url(args: &Cli) -> Result<(Url, String, Option<String>, Option<String>)> {
    let input_url = Url::parse(&args.input).context("Failed to parse input URL")?;
    log::debug!("Parsed input URL: {}", input_url);

    let segments = path_segments_without_empty(&input_url);
    if segments.is_empty() {
        return Err(anyhow!("No index specified in the input URL path"));
    }

    let normalized_segments = match segments.last().copied() {
        Some("_search") if segments.len() >= 2 => &segments[..segments.len() - 1],
        _ => &segments[..],
    };

    let index = normalized_segments
        .last()
        .ok_or_else(|| anyhow!("No index specified in the input URL path"))?;
    if index.starts_with('_') {
        return Err(anyhow!(
            "Input URL must point to an index root, not an Elasticsearch API endpoint"
        ));
    }

    let mut host_url = input_url.clone();
    host_url
        .set_username("")
        .map_err(|_| anyhow!("Failed to clear username from input URL"))?;
    host_url
        .set_password(None)
        .map_err(|_| anyhow!("Failed to clear password from input URL"))?;
    host_url.set_query(None);
    host_url.set_fragment(None);

    let base_path = if normalized_segments.len() == 1 {
        "/".to_string()
    } else {
        format!("/{}/", normalized_segments[..normalized_segments.len() - 1].join("/"))
    };
    host_url.set_path(&base_path);

    log::debug!("Extracted host URL: {}", host_url);
    log::debug!("Using index: {}", index);

    let url_username = input_url.username();
    let url_password = input_url.password();

    let auth_username = args
        .username
        .as_deref()
        .or_else(|| {
            if !url_username.is_empty() {
                Some(url_username)
            } else {
                None
            }
        })
        .map(|s| s.to_string());

    let auth_password = args
        .password
        .as_deref()
        .or(url_password)
        .map(|s| s.to_string());

    Ok((host_url, index.to_string(), auth_username, auth_password))
}

/// Create and configure the Elasticsearch client
pub fn create_client(
    host_url: Url,
    auth_username: Option<String>,
    auth_password: Option<String>,
    enable_compression: bool,
) -> Result<Elasticsearch> {
    log::debug!(
        "Setting up Elasticsearch client connection to {}",
        host_url.as_str()
    );
    let conn_pool = SingleNodeConnectionPool::new(host_url);
    let mut transport_builder = TransportBuilder::new(conn_pool);

    let mut headers = HeaderMap::new();

    if let (Some(user), Some(pass)) = (auth_username.as_deref(), auth_password.as_deref()) {
        log::info!("Using basic authentication for user: {}", user);
        let auth_str = format!("{}:{}", user, pass);
        let auth_val = format!("Basic {}", BASE64_STANDARD.encode(auth_str));
        headers.insert(AUTHORIZATION, HeaderValue::from_str(&auth_val)?);
        log::debug!("Adding authorization header");
    } else if auth_username.is_some() || auth_password.is_some() {
        log::warn!(
            "Partial basic auth credentials provided (username or password missing), ignoring."
        );
    }

    // Add Accept-Encoding: identity by default, unless --esCompress is set
    if !enable_compression {
        log::debug!(
            "Disabling response compression by setting Accept-Encoding: identity (default)"
        );
        headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    } else {
        log::debug!("Allowing Elasticsearch response compression (--esCompress specified)");
    }

    // Set headers on the builder if any were added
    if !headers.is_empty() {
        transport_builder = transport_builder.headers(headers);
    }

    log::debug!("Building Elasticsearch transport");
    let transport = transport_builder
        .build()
        .context("Failed to build Elasticsearch transport")?;

    log::debug!("Elasticsearch client created successfully");
    Ok(Elasticsearch::new(transport))
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::parse_input_url;
    use crate::cli::Cli;

    fn minimal_cli() -> Cli {
        Cli::parse_from([
            "elasticdump-rs",
            "--input",
            "http://localhost:9200/placeholder",
            "--output",
            "$",
        ])
    }

    #[test]
    fn parse_input_url_preserves_base_path() {
        let args = Cli {
            input: "https://example.com/es-proxy/my_index".into(),
            output: "$".into(),
            ..minimal_cli()
        };

        let (host_url, index, _, _) = parse_input_url(&args).unwrap();
        assert_eq!(host_url.as_str(), "https://example.com/es-proxy/");
        assert_eq!(index, "my_index");
    }

    #[test]
    fn parse_input_url_normalizes_search_suffix() {
        let args = Cli {
            input: "https://example.com/es-proxy/my_index/_search".into(),
            output: "$".into(),
            ..minimal_cli()
        };

        let (host_url, index, _, _) = parse_input_url(&args).unwrap();
        assert_eq!(host_url.as_str(), "https://example.com/es-proxy/");
        assert_eq!(index, "my_index");
    }

    #[test]
    fn parse_input_url_rejects_other_endpoint_suffixes() {
        let args = Cli {
            input: "https://example.com/es-proxy/my_index/_count".into(),
            output: "$".into(),
            ..minimal_cli()
        };

        let error = parse_input_url(&args).unwrap_err().to_string();
        assert!(error.contains("index root"));
    }
}
