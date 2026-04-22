use bytes::Bytes;
use std::fmt;
use tokio::sync::oneshot;

use crate::cli::SearchType;

use super::extract::BatchMetadata;

#[derive(Debug)]
pub(crate) struct BatchProcessingFailure {
    metadata: Option<BatchMetadata>,
    error: anyhow::Error,
}

impl BatchProcessingFailure {
    pub(crate) fn with_metadata(error: anyhow::Error, metadata: BatchMetadata) -> Self {
        Self {
            metadata: Some(metadata),
            error,
        }
    }

    pub(crate) fn metadata(&self) -> Option<&BatchMetadata> {
        self.metadata.as_ref()
    }

    pub(crate) fn into_error(self) -> anyhow::Error {
        self.error
    }

    pub(crate) fn with_fallback_metadata(mut self, metadata: Option<BatchMetadata>) -> Self {
        if self.metadata.is_none() {
            self.metadata = metadata;
        }
        self
    }
}

impl From<anyhow::Error> for BatchProcessingFailure {
    fn from(error: anyhow::Error) -> Self {
        Self {
            metadata: None,
            error,
        }
    }
}

impl fmt::Display for BatchProcessingFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for BatchProcessingFailure {}

pub(crate) struct BatchJob {
    pub(crate) response_bytes: Bytes,
    pub(crate) search_type: SearchType,
    pub(crate) reply_tx: oneshot::Sender<std::result::Result<BatchMetadata, BatchProcessingFailure>>,
}

pub(crate) enum RetrievalMessage {
    Batch(BatchJob),
    Done,
}

#[cfg(test)]
mod tests {
    use super::{BatchJob, RetrievalMessage};
    use crate::cli::SearchType;
    use tokio::sync::oneshot;

    #[test]
    fn retrieval_message_batch_is_a_direct_variant() {
        let (reply_tx, _reply_rx) = oneshot::channel();
        let message = RetrievalMessage::Batch(BatchJob {
            response_bytes: bytes::Bytes::from_static(br#"{"hits":{"hits":[]}}"#),
            search_type: SearchType::Scroll,
            reply_tx,
        });

        match message {
            RetrievalMessage::Batch(job) => {
                assert!(matches!(job.search_type, SearchType::Scroll));
            }
            RetrievalMessage::Done => panic!("expected batch message"),
        }
    }
}
