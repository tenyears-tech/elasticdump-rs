//! Minimal in-process mock Elasticsearch HTTP server for driving the binary's
//! failure/retry code paths without a live cluster.
//!
//! It speaks just enough HTTP/1.1 to satisfy the `elasticsearch` client: read
//! the request headers up to the CRLFCRLF delimiter, consume any
//! `Content-Length` body, then reply with a canned status + JSON body carrying
//! `Content-Length` and `Connection: close`. Canned responses are served in
//! request order; any request beyond the script gets a 500 so an over-eager
//! client fails fast instead of hanging on a silent socket.
//!
//! The binary issues strictly sequential requests (each reply sets
//! `Connection: close`, forcing a fresh connection per request), so a single
//! accept loop handling one connection at a time preserves script order
//! deterministically and lets `request_count()` be read without races.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// One scripted HTTP reply: an HTTP status code and a JSON body.
#[derive(Clone)]
pub struct CannedResponse {
    pub status: u16,
    pub body: String,
}

impl CannedResponse {
    /// A canned reply with an explicit status and JSON body.
    pub fn new(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            body: body.into(),
        }
    }

    /// A `200 OK` reply with the given JSON body.
    pub fn ok(body: impl Into<String>) -> Self {
        Self::new(200, body)
    }
}

/// Handle to a running mock server. Dropping it aborts the accept loop.
pub struct MockEs {
    addr: String,
    counter: Arc<AtomicUsize>,
    handle: tokio::task::JoinHandle<()>,
}

impl MockEs {
    /// Bind an ephemeral `127.0.0.1` port and start serving `script` in request
    /// order. Returns once the listener is bound so [`MockEs::input_url`] is
    /// immediately usable by a spawned subprocess.
    pub async fn serve(script: Vec<CannedResponse>) -> MockEs {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock ES: failed to bind ephemeral port");
        let addr = listener
            .local_addr()
            .expect("mock ES: no local addr")
            .to_string();
        let counter = Arc::new(AtomicUsize::new(0));
        let script = Arc::new(script);

        let handle = tokio::spawn({
            let counter = Arc::clone(&counter);
            let script = Arc::clone(&script);
            async move {
                loop {
                    let mut socket = match listener.accept().await {
                        Ok((socket, _peer)) => socket,
                        Err(_) => return,
                    };
                    // A peer that connected but never sent a full request (e.g.
                    // a dropped connection) is ignored and must not consume a
                    // scripted response.
                    if read_http_request(&mut socket).await.is_none() {
                        continue;
                    }
                    let index = counter.fetch_add(1, Ordering::SeqCst);
                    let (status, body) = match script.get(index) {
                        Some(canned) => (canned.status, canned.body.as_str()),
                        None => (
                            500,
                            r#"{"error":"mock-es: request beyond scripted responses"}"#,
                        ),
                    };
                    // A write error only means the client went away mid-dump;
                    // keep serving so later assertions still observe the count.
                    let _ = write_response(&mut socket, status, body).await;
                    let _ = socket.shutdown().await;
                }
            }
        });

        MockEs {
            addr,
            counter,
            handle,
        }
    }

    /// Number of HTTP requests received so far.
    pub fn request_count(&self) -> usize {
        self.counter.load(Ordering::SeqCst)
    }

    /// `http://127.0.0.1:PORT/INDEX` input URL for the binary's `--input`.
    pub fn input_url(&self, index: &str) -> String {
        format!("http://{}/{}", self.addr, index)
    }
}

impl Drop for MockEs {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Read one HTTP/1.1 request: headers up to CRLFCRLF, then the
/// `Content-Length` body. Returns `None` if the peer closed before a full
/// header block arrived.
async fn read_http_request(socket: &mut TcpStream) -> Option<()> {
    let mut buffer = Vec::with_capacity(1024);
    let mut chunk = [0u8; 4096];

    let header_end = loop {
        if let Some(pos) = find_subslice(&buffer, b"\r\n\r\n") {
            break pos + 4;
        }
        let n = socket.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..n]);
    };

    let content_length = parse_content_length(&buffer[..header_end]).unwrap_or(0);
    let mut body_read = buffer.len() - header_end;
    while body_read < content_length {
        let n = socket.read(&mut chunk).await.ok()?;
        if n == 0 {
            break;
        }
        body_read += n;
    }
    Some(())
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn parse_content_length(headers: &[u8]) -> Option<usize> {
    let text = std::str::from_utf8(headers).ok()?;
    for line in text.split("\r\n") {
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("content-length") {
                return value.trim().parse().ok();
            }
        }
    }
    None
}

async fn write_response(socket: &mut TcpStream, status: u16, body: &str) -> std::io::Result<()> {
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {len}\r\n\
         X-Elastic-Product: Elasticsearch\r\n\
         Connection: close\r\n\r\n",
        reason = reason_phrase(status),
        len = body.len(),
    );
    socket.write_all(head.as_bytes()).await?;
    socket.write_all(body.as_bytes()).await?;
    socket.flush().await
}

fn reason_phrase(status: u16) -> &'static str {
    match status {
        200 => "OK",
        408 => "Request Timeout",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        504 => "Gateway Timeout",
        _ => "Status",
    }
}
