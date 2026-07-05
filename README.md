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
--workers            Number of processing workers, must be >= 1 [default: 4] (clamped to the effective slice count (1 when slicing is disabled))
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

Basic-auth credentials (from the `--input` URL or `--username`/`--password`, with the CLI flags overriding the URL) are now validated instead of silently ignored: a password without a username is a hard error, and a username without a password sends basic auth with an empty password and logs a warning. Previously both of these cases silently proceeded unauthenticated.

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

### Benchmark results

The maintainer benchmark (`scripts/benchmark/compare-with-elasticdump.sh`) exports a full index to JSONL with both `elasticdump-rs` and the original Node.js `elasticdump`, using each tool's Scroll and PIT modes. Setup: **2,000,000** synthetic log documents (an analyzed ~256-byte `message` plus typical keyword/date/integer fields), Elasticsearch 7.17, index of 2 primary shards / 0 replicas, force-merged to a single segment with the default codec (modelling a static, read-optimized dump target). Host: 32-core x86-64 Linux. Figures are the mean of 2 measured runs after 1 warmup, and throughput is wall-clock (documents ÷ elapsed time).

#### Drop-in default (unsliced), pinned to 1 vs 4 CPU cores

Both tools in their default configuration; each timed run is pinned to *N* CPU cores with `taskset` (the pinning applies to the dump process — Elasticsearch keeps all cores).

Throughput (higher is better):

| Export mode | 1 CPU core | 4 CPU cores |
|---|---|---|
| **elasticdump-rs**, scroll | 375,587 docs/s · 252.3 MiB/s | 339,271 docs/s · 227.9 MiB/s |
| **elasticdump-rs**, PIT | 390,625 docs/s · 269.5 MiB/s | 343,938 docs/s · 237.3 MiB/s |
| elasticdump (Node.js), scroll | 190,749 docs/s · 127.8 MiB/s | 212,314 docs/s · 142.2 MiB/s |
| elasticdump (Node.js), PIT | 191,663 docs/s · 128.4 MiB/s | 210,970 docs/s · 141.3 MiB/s |

Speedup of `elasticdump-rs` over `elasticdump`:

| Metric | 1 CPU core | 4 CPU cores |
|---|---|---|
| Wall-clock, scroll | **1.97× faster** | **1.60× faster** |
| Wall-clock, PIT | **2.04× faster** | **1.63× faster** |
| CPU time consumed, scroll | 5.1× less | 5.2× less |
| CPU time consumed, PIT | 4.4× less | 4.2× less |

Notes:

- **The unsliced `elasticdump-rs` run is bound by Elasticsearch round-trips, not the CPU.** It spends only ~2 s of CPU on a ~5 s dump driving a single sequential cursor, so extra cores do not raise throughput *in this mode* — the small differences between the 1- and 4-core columns are run-to-run variance. Slicing (below) is what engages more cores. The ~4–5× lower CPU cost leaves the machine free for other work either way.
- **`elasticdump` is CPU-bound.** It needs ~10 s of CPU per dump; pinned to a single core it serialises to ~191k docs/s, and only recovers to ~211k docs/s once it can spread across more cores.

#### Sliced retrieval (`--slices`)

The tables above run `elasticdump-rs` with a single retrieval cursor. Passing `--slices` splits the dump into parallel per-slice cursors and changes the picture entirely — same 2M-doc index, no CPU pinning:

| Export mode | Throughput | vs Node.js elasticdump |
|---|---|---|
| **elasticdump-rs**, PIT, `--slices 16` | 2,030,457 docs/s · 1.4 GiB/s | **9.66× faster**, 2.7× less CPU |
| **elasticdump-rs**, scroll, `--slices 2` | 788,955 docs/s · 530.0 MiB/s | **3.77× faster**, 5.4× less CPU |
| elasticdump (Node.js), PIT | 210,084 docs/s · 140.7 MiB/s | — (no slicing support) |
| elasticdump (Node.js), scroll | 209,096 docs/s · 140.1 MiB/s | — (no slicing support) |

Reproduce with e.g. `scripts/benchmark/compare-with-elasticdump.sh --rs-slices 16 --search-types pit`.

Tuning guidance from the slice sweep on this host (2-shard index):

- **PIT tolerates — and rewards — more slices than shards.** Throughput rose through 4/8/16 slices (~2.9×/4.2×/5.1× the unsliced rate) and levelled off around 16 on this host; Elasticsearch slices PIT searches with an efficient partitioning strategy, and `elasticdump-rs` already defaults PIT to the recommended `_shard_doc` sort.
- **Scroll slicing should match the primary-shard count.** `--slices 2` on the 2-shard index scaled near-perfectly (1.94×), but over-slicing scroll is a trap: 4 slices on 2 shards measured *slower than unsliced* (0.98×), because each shard then evaluates a per-document `_id`-hash slice filter.
- **Keep `--workers` ≥ `--slices`.** The tool clamps workers to `min(workers, slices)`; under-provisioning workers at 8 slices cost ~6% (4 workers) to ~26% (2 workers). The benchmark script matches workers to slices automatically.
- Sliced output interleaves across slices — global document order is not preserved (see the ordering note above).

Absolute numbers depend heavily on your Elasticsearch cluster, network, disk, and page-cache state. Treat the ratios as the portable result and re-run the script in your own environment.

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
