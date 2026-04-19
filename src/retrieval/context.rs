use elasticsearch::Elasticsearch;
use indicatif::ProgressBar;
use std::{
    sync::{Arc, atomic::AtomicU64},
    time::Instant,
};
use tokio::sync::mpsc::Sender;

use super::messages::RetrievalMessage;

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
}
