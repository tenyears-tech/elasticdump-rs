use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use base64::prelude::*;
use elasticsearch::{
    Elasticsearch,
    cert::{Certificate, CertificateValidation},
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
};
use http::header::{ACCEPT_ENCODING, AUTHORIZATION, HeaderMap, HeaderValue};
use percent_encoding::percent_decode_str;
use url::Url;

use crate::cli::Cli;

fn path_segments_without_empty(url: &Url) -> Vec<&str> {
    url.path_segments()
        .map(|segments| segments.filter(|segment| !segment.is_empty()).collect())
        .unwrap_or_default()
}

/// Percent-decode a URL component (credentials or index name) into its literal value.
/// With `redact` the raw value is kept out of the error message (use for passwords).
fn decode_component(raw: &str, what: &str, redact: bool) -> Result<String> {
    percent_decode_str(raw)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|err| {
            if redact {
                anyhow!("Failed to percent-decode {what}: {err}")
            } else {
                anyhow!("Failed to percent-decode {what} '{raw}': {err}")
            }
        })
}

/// Render a URL for logging with its password masked (the username is preserved).
pub fn redacted_url(url: &Url) -> String {
    let mut redacted = url.clone();
    if url.password().is_some() {
        let _ = redacted.set_password(Some("***"));
    }
    redacted.to_string()
}

/// Parse the input URL and extract host URL, index name, and auth credentials
pub fn parse_input_url(args: &Cli) -> Result<(Url, String, Option<String>, Option<String>)> {
    let input_url = Url::parse(&args.input).context("Failed to parse input URL")?;
    if !matches!(input_url.scheme(), "http" | "https") {
        return Err(anyhow!(
            "Unsupported URL scheme '{}': --input must use http:// or https://",
            input_url.scheme()
        ));
    }
    log::debug!("Parsed input URL: {}", redacted_url(&input_url));

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
    // Decode before the guard so encoded forms like %5Fsearch cannot bypass it.
    let index = decode_component(index, "index name", false)?;
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
    if let Some(query) = input_url.query() {
        log::warn!("Ignoring query string in --input URL: {}", query);
    }
    host_url.set_query(None);
    host_url.set_fragment(None);

    let base_path = if normalized_segments.len() == 1 {
        "/".to_string()
    } else {
        format!(
            "/{}/",
            normalized_segments[..normalized_segments.len() - 1].join("/")
        )
    };
    host_url.set_path(&base_path);

    log::debug!("Extracted host URL: {}", host_url);
    log::debug!("Using index: {}", index);

    // URL-embedded credentials are percent-encoded on the wire; decode them.
    let url_username = input_url.username();
    let decoded_url_username = if url_username.is_empty() {
        None
    } else {
        Some(decode_component(url_username, "URL username", false)?)
    };
    let decoded_url_password = match input_url.password() {
        Some(password) => Some(decode_component(password, "URL password", true)?),
        None => None,
    };

    // CLI-provided credentials override the URL and are used literally (already unencoded).
    let auth_username = args.username.clone().or(decoded_url_username);
    let auth_password = args.password.clone().or(decoded_url_password);

    Ok((host_url, index, auth_username, auth_password))
}

/// Transport-level options for the Elasticsearch client.
pub struct ClientOptions {
    pub compression: bool,
    pub request_timeout_secs: u64, // 0 = disabled
    pub insecure: bool,
    pub ca_file: Option<String>,
}

impl ClientOptions {
    pub fn from_cli(args: &Cli) -> Self {
        Self {
            compression: args.es_compress,
            request_timeout_secs: args.request_timeout_secs,
            insecure: args.insecure,
            ca_file: args.ca_file.clone(),
        }
    }
}

/// Create and configure the Elasticsearch client
pub fn create_client(
    host_url: Url,
    auth_username: Option<String>,
    auth_password: Option<String>,
    options: &ClientOptions,
) -> Result<Elasticsearch> {
    log::debug!(
        "Setting up Elasticsearch client connection to {}",
        host_url.as_str()
    );
    let conn_pool = SingleNodeConnectionPool::new(host_url);
    let mut transport_builder = TransportBuilder::new(conn_pool);

    let mut headers = HeaderMap::new();

    let basic_auth = match (auth_username.as_deref(), auth_password.as_deref()) {
        (Some(user), Some(pass)) => {
            log::info!("Using basic authentication for user: {}", user);
            Some(format!("{}:{}", user, pass))
        }
        (Some(user), None) => {
            log::warn!(
                "No password provided for user '{user}'; sending basic auth with an empty password"
            );
            log::info!("Using basic authentication for user: {}", user);
            Some(format!("{}:", user))
        }
        (None, Some(_)) => {
            return Err(anyhow!(
                "--password/URL password provided without a username"
            ));
        }
        (None, None) => None,
    };

    if let Some(auth_str) = basic_auth {
        let auth_val = format!("Basic {}", BASE64_STANDARD.encode(auth_str));
        headers.insert(AUTHORIZATION, HeaderValue::from_str(&auth_val)?);
        log::debug!("Adding authorization header");
    }

    // Add Accept-Encoding: identity by default, unless --esCompress is set
    if !options.compression {
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

    if options.request_timeout_secs > 0 {
        transport_builder =
            transport_builder.timeout(Duration::from_secs(options.request_timeout_secs));
    }

    if options.insecure {
        log::warn!("--insecure: TLS certificate verification is DISABLED");
        transport_builder = transport_builder.cert_validation(CertificateValidation::None);
    } else if let Some(path) = &options.ca_file {
        let pem =
            std::fs::read(path).with_context(|| format!("Failed to read --caFile '{path}'"))?;
        let cert = Certificate::from_pem(&pem)
            .with_context(|| format!("Failed to parse PEM certificate(s) from '{path}'"))?;
        transport_builder = transport_builder.cert_validation(CertificateValidation::Full(cert));
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
    use url::Url;

    use super::{parse_input_url, redacted_url};
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

    #[test]
    fn parse_input_url_percent_decodes_credentials_and_index() {
        let args = Cli {
            input: "http://user%40corp:p%40ss%23w@localhost:9200/logs%2D2026".into(),
            output: "$".into(),
            ..minimal_cli()
        };
        let (_, index, user, pass) = parse_input_url(&args).unwrap();
        assert_eq!(index, "logs-2026");
        assert_eq!(user.as_deref(), Some("user@corp"));
        assert_eq!(pass.as_deref(), Some("p@ss#w"));
    }

    #[test]
    fn parse_input_url_rejects_non_http_schemes() {
        let args = Cli {
            input: "ftp://localhost:9200/idx".into(),
            output: "$".into(),
            ..minimal_cli()
        };
        let error = parse_input_url(&args).unwrap_err().to_string();
        assert!(error.contains("http:// or https://"));
    }

    #[test]
    fn redacted_input_url_masks_password() {
        let url = Url::parse("http://u:secret@localhost:9200/idx").unwrap();
        assert!(!redacted_url(&url).contains("secret"));
        assert!(redacted_url(&url).contains("u:***@"));
    }

    #[test]
    fn parse_input_url_password_decode_error_does_not_leak_password() {
        // %FF percent-decodes to a lone 0xFF byte, which is invalid UTF-8.
        let args = Cli {
            input: "http://u:secret%FF@localhost:9200/idx".into(),
            output: "$".into(),
            ..minimal_cli()
        };
        let error = parse_input_url(&args).unwrap_err().to_string();
        assert!(
            !error.contains("secret"),
            "error must not contain the raw password: {error}"
        );
    }

    #[test]
    fn parse_input_url_rejects_percent_encoded_api_endpoint() {
        // %5F decodes to '_'; the API-endpoint guard must see the decoded segment.
        let args = Cli {
            input: "http://localhost:9200/%5Fsearch".into(),
            output: "$".into(),
            ..minimal_cli()
        };
        let error = parse_input_url(&args).unwrap_err().to_string();
        assert!(error.contains("index root"));
    }
}
