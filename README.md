# elasticdump-rs

> [!WARNING]
> This project was completely written using AI tools (e.g., ChatGPT, Cursor). The author does not typically write Rust and considers this project experimental. While the author has made efforts to test the code, there is no guarantee that all features will function correctly. Please use this project at your own risk.

A blazing fast Elasticsearch data dumper written in Rust. It implements a subset of features from the original [`elasticdump`](https://github.com/elasticsearch-dump/elasticsearch-dump) tool and maintains compatibility with many of its command-line options, making it a potential drop-in replacement for data dumping tasks. This tool allows you to efficiently dump data from Elasticsearch to a file in JSONL format.

## Features

- High-performance data dumping from Elasticsearch 7.x and 8.x
- JSONL output format (one JSON document per line)
- Support for basic authentication
- Progress bar with throughput display
- Customizable scroll size and timeout
- Support for both Scroll API and Point in Time API for efficient data retrieval
- Shared PIT coordination across slices so sliced PIT keeps one generation-aligned PIT ID in flight
- Support for filtering documents with custom query
- Ability to output to stdout for piping to other tools
- Multi-threaded processing for optimal performance
- Configurable TLS verification (custom CA bundle or, at your own risk, disabled)
- Automatic, data-safe retries with exponential backoff for transient failures
- Atomic, durable file output that never clobbers an existing file on failure
- Rejects partial (timed-out or failed-shard) responses instead of writing an incomplete dump

## Platform Support

`elasticdump-rs` currently supports Unix-like platforms only. Windows is not supported in this release train.

## Installation

```bash
# Clone the repository
git clone https://github.com/tenyears-tech/elasticdump-rs.git
cd elasticdump-rs

# Build the project
cargo build --release

# Optionally, build with native optimizations.
# Use RUSTFLAGS so the flag applies to every dependency (the hot path lives in
# sonic-rs and the ES client, not just this crate). `cargo rustc -C target-cpu`
# would only optimize the final crate and miss them.
RUSTFLAGS="-C target-cpu=native" cargo build --release

# The binary will be available at
./target/release/elasticdump-rs
```

## Usage

```bash
# Basic usage - dump all documents from an index to a file
elasticdump-rs --input http://localhost:9200/my_index --output /path/to/output.jsonl

# Using stdout as output
elasticdump-rs --input http://localhost:9200/my_index --output $

# Pipe output to gzip
elasticdump-rs --input http://localhost:9200/my_index --output $ | gzip > output.jsonl.gz

# With basic authentication
elasticdump-rs --input http://localhost:9200/my_index --output output.jsonl --username user --password pass

# With custom query
elasticdump-rs --input http://localhost:9200/my_index --output output.jsonl --searchBody '{"query":{"match":{"field":"value"}}}'

# With query from file
elasticdump-rs --input http://localhost:9200/my_index --output output.jsonl --searchBody @query.json

# Customize scroll size and timeout
elasticdump-rs --input http://localhost:9200/my_index --output output.jsonl --limit 5000 --scrollTime 5m

# Use point-in-time search instead of scroll
elasticdump-rs --input http://localhost:9200/my_index --output output.jsonl --searchType pit --pitKeepAlive 2m

# Bound each HTTP request with a 30s timeout (0, the default, disables it)
elasticdump-rs --input https://localhost:9200/my_index --output output.jsonl --requestTimeout 30

# Verify the server certificate against a custom CA bundle
elasticdump-rs --input https://localhost:9200/my_index --output output.jsonl --caFile /path/to/ca.pem

# Skip TLS verification (DANGEROUS: only on trusted networks)
elasticdump-rs --input https://localhost:9200/my_index --output output.jsonl --insecure

# Tune retry behavior (defaults: 3 attempts, 1000ms base delay, doubling per attempt)
elasticdump-rs --input http://localhost:9200/my_index --output output.jsonl --retryAttempts 5 --retryDelay 500
```

## Options

```plain
--input              Elasticsearch URL including index (e.g., http://user:pass@localhost:9200/my_index)
--output             Output file path or '$' for stdout
--type               Type of operation, only 'data' is supported [default: data]
--limit              Batch size: documents fetched per scroll/PIT page, must be >= 1 [default: 10000]
--searchBody         Optional JSON query string or @/path/to/file.json to filter documents
--username           Username for basic authentication (overrides URL credentials)
--password           Password for basic authentication (overrides URL credentials)
--scrollTime         Scroll timeout [default: 10m]
--searchType         Search type (scroll or point-in-time) [default: scroll]
--pitKeepAlive       Point in Time keep alive value (when using pit search type) [default: 10m]
--overwrite          Overwrite output file if it exists
--quiet              Suppress progress display
--debug              Enable verbose logging
--workers            Number of processing workers, must be >= 1 [default: 4] (clamped to the number of slices)
--slices             Number of parallel slices for Elasticsearch sliced scroll API [default: 0, disabled]
--bufferSize         Size of internal channel buffers, must be >= 1 [default: 16]
--esCompress         Enable Elasticsearch response compression (default: disabled)
--requestTimeout     Per-request HTTP timeout in seconds; 0 disables it [default: 0]
--insecure           Skip TLS certificate verification (DANGEROUS; conflicts with --caFile)
--caFile             Path to a PEM CA bundle used to verify the server certificate
--retryAttempts      Retry attempts for retry-safe requests; 0 disables retries [default: 3]
--retryDelay         Base delay between retries in milliseconds (doubles per attempt, capped at 30s) [default: 1000]
```

`--limit` must be at least 1; `--workers` and `--bufferSize` likewise reject `0`.

When `--output` points to a file, `elasticdump-rs` now stages writes to a temporary file and only replaces the destination after a successful dump. This prevents `--overwrite` from destroying an existing file when validation or retrieval fails early.

`--input` may point to either the index root URL or a trailing `/_search` URL. Proxy/base-path prefixes are preserved, but other endpoint URLs such as `/my_index/_count` are rejected.

When `--searchType pit` is used without an explicit `sort`, `elasticdump-rs` now defaults to `["_shard_doc"]`, which is the recommended fast path for full PIT dumps. If you provide your own `sort`, it is preserved unchanged.

When `--searchType pit` is combined with `--slices`, `elasticdump-rs` now coordinates one shared PIT per generation across all active slices. Each active slice uses the same PIT ID for a generation, the coordinator advances only after every active slice reports back, and finished slices drop out of later generations. The final PIT is closed once after retrieval completes.

Batches are processed and written as workers finish, so dump output is not guaranteed to preserve Elasticsearch document order once more than one worker or slice is active, even when the search request uses `sort`.

`--workers` scales processing only up to the number of slices, and the effective count is clamped to it (`min(workers, slices)`; an unsliced dump runs a single slice, so extra workers add threads without adding parallelism). Within one slice, fetching and extraction never overlap: each page must be extracted to recover the `_scroll_id`/`pit_id` and `search_after` before the next request can be built. Raise `--slices` — not just `--workers` — to actually parallelize a dump.

Passing `--slices 1` is equivalent to an unsliced dump (Elasticsearch rejects `slice.max=1`); the tool warns and runs unsliced.

## Examples

```bash
# Basic usage - dump all documents from an index to a file
elasticdump-rs --input http://localhost:9200/my_index --output /path/to/output.jsonl

# Using parallel sliced scrolling for better performance on large indices
elasticdump-rs --input http://localhost:9200/my_index --output output.jsonl --slices 4

# Using Point in Time API instead of Scroll API
elasticdump-rs --input http://localhost:9200/my_index --output output.jsonl --searchType pit

# Enable debug logging for verbose output
elasticdump-rs --input http://localhost:9200/my_index --output output.jsonl --debug

# Using stdout as output
elasticdump-rs --input http://localhost:9200/my_index --output $
```

## Reliability & failure semantics

- **Partial results are rejected.** A batch whose response reports `timed_out=true` or any failed shard (`_shards.failed > 0`) is treated as an error, not written. This prevents silently producing an incomplete dump. Raise `--requestTimeout` (and/or cluster-side timeouts) and retry if you hit this.
- **Automatic retries with backoff.** Retry-safe requests are retried up to `--retryAttempts` times (default 3) with exponential backoff starting at `--retryDelay` ms and capped at 30s. Retryable conditions are transport errors and HTTP 408/429/502/503/504. **Scroll continuations are a deliberate exception: they are retried only on HTTP 429.** A scroll continuation advances the server-side cursor, so retrying it after a transport/timeout failure (where the request may have reached the server) could silently skip a page of documents; PIT `search_after` pages and initial searches carry no such risk and retry fully.
- **Ctrl+C is graceful.** The first interrupt cancels the pipeline, cleans up staged output, clears the scroll/PIT context, and closes the PIT, then exits with a failure status. A second Ctrl+C force-quits immediately with exit status 130.
- **Atomic, durable file output.** File dumps are written to a temporary staging file that is `fsync`'d and only then committed to the destination. Without `--overwrite`, the commit uses a hard link so it fails (rather than clobbering) if the destination appeared while the dump was running; with `--overwrite` it renames into place and preserves the prior file's permissions. Any early validation or retrieval failure leaves the existing destination untouched.
- **Broken-pipe-friendly stdout.** When writing to stdout (`--output $`) and the reader goes away (e.g. piping into `head`), the dump stops early and exits `0` rather than reporting a write error.

## Differences from elasticdump

This tool is inspired by the Node.js [elasticdump](https://github.com/elasticsearch-dump/elasticsearch-dump) but focuses only on dumping data from Elasticsearch to files (`--type data`), aiming to be a much faster, more memory-efficient drop-in for that use case. Notable behavioral differences:

- **No `--size` total-document cap.** `elasticdump-rs` always dumps every matching document; there is no option to stop after N documents. Restrict the result set with `--searchBody` instead.
- **`--limit` is the batch (page) size**, matching the original's meaning — documents fetched per scroll/PIT request, not a total.
- **Retries are on by default.** The original defaults `retryAttempts` to `0`; here `--retryAttempts` defaults to `3` (see the retry-safety rules above).
- **Only the data-dumping subset is implemented.** Flags outside that scope (mappings/analyzer/alias transfer, `--size`, output splitting, etc.) are not supported and are simply not accepted — the tool does not attempt to emulate every original flag.

### Tuning notes

- **Memory ceiling.** Peak in-flight memory scales with `--bufferSize` times the typical batch size, plus roughly one in-flight batch per slice. A batch is about `--limit` × average document size. For very large documents or a large `--limit`, lower `--bufferSize` (and/or `--limit`) to cap memory; raise them to trade memory for throughput.
- **`--scrollTime` / `--pitKeepAlive` must outlast sink stalls.** The scroll/PIT context has to stay alive between consecutive requests for a slice. If the output sink stalls (slow disk, a slow consumer on a stdout pipe), the next request can be delayed by that stall, so set `--scrollTime`/`--pitKeepAlive` comfortably longer than any expected sink backpressure, not just longer than a single fetch.

## Performance

`elasticdump-rs` is optimized for high throughput and low memory usage, making it suitable for dumping large indices. Performance varies with your network, Elasticsearch cluster, and local machine. Raise `--slices` to parallelize retrieval (see the `--workers` note above), and consider a `target-cpu=native` release build.

## Testing

The project includes both unit tests and integration tests:

```bash
# Run unit tests
cargo test

# Run the default integration suite (requires a running Elasticsearch instance)
cargo test --test integration_test

# Run the benchmark-style integration tests explicitly
cargo test --test integration_test -- --ignored --nocapture

# Run the scroll-vs-PIT comparison benchmark explicitly
cargo test --test integration_test test_compare_scroll_vs_pit -- --ignored --nocapture

# Run large-scale performance test (requires significant resources)
cargo test --features large_scale_test --test integration_test test_large_scale_performance -- --nocapture
```

The integration tests require a running Elasticsearch instance. By default, they look for Elasticsearch at `http://localhost:9200`.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
