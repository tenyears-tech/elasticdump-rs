use elasticsearch::Elasticsearch;
use indicatif::ProgressBar;
use std::{
    sync::{Arc, atomic::AtomicU64},
    time::Instant,
};
use tokio::sync::mpsc::Sender;
use tokio_util::sync::CancellationToken;

use super::messages::RetrievalMessage;
use super::retrieval_task::RetryPolicy;

#[derive(Clone)]
pub(crate) struct RetrievalContext {
    pub(crate) client: Elasticsearch,
    pub(crate) index: Arc<str>,
    pub(crate) worker_txs: Vec<Sender<RetrievalMessage>>,
    pub(crate) total_hits_count: Arc<AtomicU64>,
    pub(crate) input_bar: Option<ProgressBar>,
    pub(crate) output_bar: Option<ProgressBar>,
    pub(crate) retrieved_count: Arc<AtomicU64>,
    pub(crate) retrieved_bytes: Arc<AtomicU64>,
    pub(crate) start_time: Instant,
    /// Cancels the whole pipeline: signal handler, output failures, and
    /// failing sibling slices all fire the same token.
    pub(crate) cancel: CancellationToken,
    pub(crate) retry: RetryPolicy,
}
