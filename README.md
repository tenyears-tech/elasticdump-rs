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

## Platform Support

`elasticdump-rs` currently supports Unix-like platforms only. Windows is not supported in this release train.

## Installation

```bash
# Clone the repository
git clone https://github.com/tenyears-tech/elasticdump-rs.git
cd elasticdump-rs

# Build the project
cargo build --release

# Optionally, build with native optimizations
cargo rustc --release --bin elasticdump-rs -- -C target-cpu=native

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
```

## Options

```plain
--input              Elasticsearch URL including index (e.g., http://user:pass@localhost:9200/my_index)
--output             Output file path or '$' for stdout
--type               Type of operation, only 'data' is supported [default: data]
--limit              Number of documents to fetch per scroll batch [default: 10000]
--searchBody         Optional JSON query string or @/path/to/file.json to filter documents
--username           Username for basic authentication (overrides URL credentials)
--password           Password for basic authentication (overrides URL credentials)
--scrollTime         Scroll timeout [default: 10m]
--searchType         Search type (scroll or point-in-time) [default: scroll]
--pitKeepAlive       Point in Time keep alive value (when using pit search type) [default: 10m]
--overwrite          Overwrite output file if it exists
--quiet              Suppress progress display
--debug              Enable verbose logging
--workers            Number of worker threads for processing, must be >= 1 [default: 4]
--slices             Number of parallel slices for Elasticsearch sliced scroll API [default: 0, disabled]
--bufferSize         Size of internal buffer for processing, must be >= 1 [default: 16]
--esCompress         Enable Elasticsearch response compression (default: disabled)
```

When `--output` points to a file, `elasticdump-rs` now stages writes to a temporary file and only replaces the destination after a successful dump. This prevents `--overwrite` from destroying an existing file when validation or retrieval fails early.

`--input` may point to either the index root URL or a trailing `/_search` URL. Proxy/base-path prefixes are preserved, but other endpoint URLs such as `/my_index/_count` are rejected.

When `--searchType pit` is used without an explicit `sort`, `elasticdump-rs` now defaults to `["_shard_doc"]`, which is the recommended fast path for full PIT dumps. If you provide your own `sort`, it is preserved unchanged.

When `--searchType pit` is combined with `--slices`, `elasticdump-rs` now coordinates one shared PIT per generation across all active slices. Each active slice uses the same PIT ID for a generation, the coordinator advances only after every active slice reports back, and finished slices drop out of later generations. The final PIT is closed once after retrieval completes.

When `--workers` is greater than `1`, batches are processed and written as workers finish. Dump output is therefore not guaranteed to preserve Elasticsearch document order, even when the search request uses `sort`.

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

## Compared to the original elasticdump

This tool is inspired by the Node.js [elasticdump](https://github.com/elasticsearch-dump/elasticsearch-dump) tool but focuses only on the core functionality of dumping data from Elasticsearch to files. The original goal was to create a drop-in replacement for `elasticdump` with significantly better performance. It's designed to be significantly faster and more memory efficient by leveraging Rust's performance characteristics.

## Performance

`elasticdump-rs` is optimized for high throughput and low memory usage, making it suitable for dumping large indices. The performance will vary based on your network, Elasticsearch cluster, and local machine capabilities.

The canonical maintainer-run local comparison lives in `scripts/benchmark/compare-with-elasticdump.sh`. It is intended for apples-to-apples benchmarking against the original Node.js `elasticdump`, outside the normal Rust integration test harness.

The benchmark script:

- creates and seeds its own logging-style benchmark index
- runs `elasticdump-rs` and the original `elasticdump` sequentially with the same practical settings
- keeps `elasticdump-rs` on Scroll mode for direct comparability with `elasticdump`
- treats the first pass for each tool as a warmup by default
- validates output line counts before reporting a result
- prints a human-readable per-run summary plus averages and a headline comparison

Local prerequisites:

- a reachable Elasticsearch node at `ES_URL` (default `http://localhost:9200`)
- `elasticdump`
- `python3`
- `curl`
- a built `elasticdump-rs` release binary or a buildable workspace

Typical maintainer run:

```bash
scripts/benchmark/compare-with-elasticdump.sh
```

Useful environment overrides:

```bash
BENCH_DOCS=200000 \
BENCH_BULK_SIZE=5000 \
BENCH_LIMIT=10000 \
BENCH_TEXT_BYTES=256 \
BENCH_WARMUP_RUNS=1 \
BENCH_MEASURED_RUNS=2 \
BENCH_KEEP_ARTIFACTS=1 \
scripts/benchmark/compare-with-elasticdump.sh
```

Warmup runs are excluded from the headline result. Only measured runs are recorded in the summary and used for the average wall-clock comparison.

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
cargo test --features large_scale_test test_large_scale_performance -- --test integration_test --nocapture
```

The integration tests require a running Elasticsearch instance. By default, they look for Elasticsearch at `http://localhost:9200`.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
