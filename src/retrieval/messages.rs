use sonic_rs::Value;
use std::sync::Arc;

/// Define message types for our channels
#[derive(Clone)]
pub enum RetrievalMessage {
    Batch(Arc<Value>),
    Done,
}
