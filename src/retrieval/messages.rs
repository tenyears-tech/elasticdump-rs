use bytes::Bytes;

/// Define message types for our channels
#[derive(Clone)]
pub enum RetrievalMessage {
    Batch(Bytes),
    Done,
}
