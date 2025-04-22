use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, ValueEnum)]
pub enum DumpType {
    #[clap(name = "data")]
    Data,
}

#[derive(Debug, Clone, ValueEnum)]
pub enum SearchType {
    #[clap(name = "scroll")]
    Scroll,
    #[clap(name = "pit")]
    PointInTime,
}

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about = "A blazing fast Elasticsearch data dumper")]
pub struct Cli {
    /// Elasticsearch input URL (e.g. http://user:pass@localhost:9200/my_index)
    #[clap(long, required = true)]
    pub input: String,

    /// Output file path or stdout if '$'
    #[clap(long, required = true)]
    pub output: String,

    /// Type of operation, only 'data' is supported
    #[clap(long, default_value = "data")]
    pub r#type: DumpType,

    /// Number of documents to scroll per batch
    #[clap(long, default_value = "10000")]
    pub limit: usize,

    /// Optional JSON query string or @/path/to/file.json to filter documents
    #[clap(long("searchBody"))]
    pub search_body: Option<String>,

    /// Username for basic auth (optional, overrides username in --input URL)
    #[clap(long)]
    pub username: Option<String>,

    /// Password for basic auth (optional, overrides password in --input URL)
    #[clap(long)]
    pub password: Option<String>,

    /// Scroll timeout
    #[clap(long("scrollTime"), default_value = "10m")]
    pub scroll: String,

    /// Search type (scroll or point-in-time)
    #[clap(long("searchType"), default_value = "scroll")]
    pub search_type: SearchType,

    /// Point in Time keep alive value (when using pit search type)
    #[clap(long("pitKeepAlive"), default_value = "10m")]
    pub pit_keep_alive: String,

    /// Overwrite output file if it exists
    #[clap(long)]
    pub overwrite: bool,

    /// Quiet mode, suppress progress output
    #[clap(long)]
    pub quiet: bool,

    /// Debug mode, enable verbose logging
    #[clap(long)]
    pub debug: bool,

    /// Number of processing workers
    #[clap(long, default_value = "4")]
    pub workers: usize,

    /// Number of parallel slices for the Elasticsearch sliced scroll API (0 disables sliced scroll)
    #[clap(long, default_value = "0")]
    pub slices: usize,

    /// Buffer size for channels
    #[clap(long("bufferSize"), default_value = "16")]
    pub buffer_size: usize,

    /// Enable Elasticsearch response compression (default: disabled)
    #[clap(long("esCompress"))]
    pub es_compress: bool,
}
