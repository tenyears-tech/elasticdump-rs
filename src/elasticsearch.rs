use anyhow::{Context, Result, anyhow};
use base64::prelude::*;
use elasticsearch::{
    Elasticsearch,
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
};
use http::header::{ACCEPT_ENCODING, AUTHORIZATION, HeaderMap, HeaderValue};
use url::Url;

use crate::cli::Cli;

/// Parse the input URL and extract host URL, index name, and auth credentials
pub fn parse_input_url(args: &Cli) -> Result<(Url, String, Option<String>, Option<String>)> {
    // Parse input URL
    let input_url = Url::parse(&args.input).context("Failed to parse input URL")?;
    log::debug!("Parsed input URL: {}", input_url);

    // --- Host Extraction ---
    let host_str = input_url.host_str().unwrap_or("localhost");
    let port_str = input_url
        .port()
        .map_or("".to_string(), |p| format!(":{}", p));
    let host_url_str = format!("{}://{}{}", input_url.scheme(), host_str, port_str);
    let host_url = Url::parse(&host_url_str)?;
    log::debug!("Extracted host URL: {}", host_url);

    // --- Index Extraction ---
    let mut path_segments = input_url
        .path_segments()
        .map(|segments| segments.collect::<Vec<_>>())
        .unwrap_or_default();
    path_segments.retain(|segment| !segment.is_empty());
    if path_segments.is_empty() {
        return Err(anyhow!("No index specified in the input URL path"));
    }
    let index = path_segments[0].to_string();
    log::debug!("Using index: {}", index);

    // --- Authentication ---
    // Priority: Flags > URL > None
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

    Ok((host_url, index, auth_username, auth_password))
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
